"""Binance websocket/REST connector for 1m klines.

This module provides an asynchronous entry point :func:`start_binance_stream`
which connects to Binance's websocket API, subscribes to 1m klines for the
requested symbols and writes the received candles into the ``bars_1m`` table
via :func:`storage.sqlite_manager.upsert_bar`.

The implementation is intentionally resilient – the websocket connection is
re-established with exponential back-off on errors, and gaps between klines
are filled by querying the REST ``/api/v3/klines`` endpoint.  The module is
designed so that individual pieces can be mocked in tests without requiring
network access.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

import httpx
import websockets

from storage.sqlite_manager import upsert_bar

LOGGER = logging.getLogger(__name__)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
BINANCE_REST_BASE = "https://api.binance.com/api/v3/klines"


def _utc_now() -> int:
    """Return the current UTC timestamp in seconds."""

    return int(time.time())


def _normalise_symbol(symbol: str) -> str:
    """Return the Binance websocket symbol (lower-case)."""

    return symbol.lower()


def _combine_stream_url(symbols: Iterable[str]) -> str:
    streams = "/".join(f"{_normalise_symbol(sym)}@kline_1m" for sym in symbols)
    return f"{BINANCE_WS_BASE}?streams={streams}"


def _kline_to_bar(symbol: str, kline: Dict[str, Any]) -> Dict[str, Any]:
    close_ts = int(kline["T"]) // 1000
    open_ts = int(kline["t"]) // 1000
    base, quote = symbol[:-4], symbol[-4:] if len(symbol) > 4 else (symbol, "")
    return {
        "source": "cex",
        "exchange": "binance",
        "chain": "",
        "symbol": symbol,
        "base": base,
        "quote": quote,
        "open_ts": open_ts,
        "close_ts": close_ts,
        "open": float(kline["o"]),
        "high": float(kline["h"]),
        "low": float(kline["l"]),
        "close": float(kline["c"]),
        "volume_base": float(kline["v"]),
        "volume_quote": float(kline["q"]),
        "notional_usd": float(kline["q"]),
        "trades": int(kline["n"]),
    }


async def _upsert_bar_async(bar: Dict[str, Any]) -> None:
    await asyncio.to_thread(upsert_bar, "bars_1m", bar)


@dataclass
class BinanceClients:
    """Container for injectable dependencies used by :class:`BinanceStream`."""

    http_factory: Callable[[], httpx.AsyncClient] = lambda: httpx.AsyncClient(
        base_url=BINANCE_REST_BASE, timeout=10.0
    )
    ws_connect: Callable[[str], Awaitable[Any]] = websockets.connect


class BinanceStream:
    """Manage the websocket stream and rest backfill for Binance klines."""

    def __init__(
        self,
        symbols: Iterable[str],
        clients: Optional[BinanceClients] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if not symbols:
            raise ValueError("at least one symbol must be provided")
        self._symbols = [symbol.upper() for symbol in symbols]
        self._clients = clients or BinanceClients()
        self._loop = loop or asyncio.get_event_loop()
        self._last_close_ts: Dict[str, int] = {}
        self._http: Optional[httpx.AsyncClient] = None
        self._reconnect_attempt = 0

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = self._clients.http_factory()
        return self._http

    async def _fetch_gap(self, symbol: str, start_ts: int, end_ts: int) -> None:
        """Fetch missing klines via REST and upsert them."""

        if end_ts <= start_ts:
            return
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_ts * 1000,
            "endTime": end_ts * 1000,
            "limit": 1000,
        }
        client = await self._get_http()
        LOGGER.info(
            "Fetching %s gap from REST: start=%s end=%s",
            symbol,
            start_ts,
            end_ts,
        )
        response = await client.get("", params=params)
        response.raise_for_status()
        data = response.json()
        for entry in data:
            bar = {
                "source": "cex",
                "exchange": "binance",
                "chain": "",
                "symbol": symbol,
                "base": symbol[:-4],
                "quote": symbol[-4:] if len(symbol) > 4 else "",
                "open_ts": int(entry[0]) // 1000,
                "close_ts": int(entry[6]) // 1000,
                "open": float(entry[1]),
                "high": float(entry[2]),
                "low": float(entry[3]),
                "close": float(entry[4]),
                "volume_base": float(entry[5]),
                "volume_quote": float(entry[7]),
                "notional_usd": float(entry[7]),
                "trades": int(entry[8]),
            }
            await _upsert_bar_async(bar)

    async def _handle_closed_kline(self, symbol: str, kline: Dict[str, Any]) -> None:
        close_ts = int(kline["T"]) // 1000
        last_close = self._last_close_ts.get(symbol)
        if last_close and close_ts - last_close > 60:
            await self._fetch_gap(symbol, last_close, close_ts - 60)
        bar = _kline_to_bar(symbol, kline)
        await _upsert_bar_async(bar)
        self._last_close_ts[symbol] = close_ts

    async def _listen_once(self) -> None:
        url = _combine_stream_url(self._symbols)
        LOGGER.info("Connecting to Binance websocket: %s", url)
        async with self._clients.ws_connect(url) as ws:
            self._reconnect_attempt = 0
            async for raw in ws:
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    LOGGER.warning("Failed to decode websocket message: %s", raw)
                    continue
                data = payload.get("data", payload)
                kline = data.get("k") if isinstance(data, dict) else None
                if not kline or not kline.get("x"):
                    continue
                symbol = kline["s"].upper()
                await self._handle_closed_kline(symbol, kline)

    async def run(self) -> None:
        """Run the websocket consumer with automatic reconnection."""

        backoff = 1.0
        while True:
            try:
                await self._listen_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - exercised via tests
                self._reconnect_attempt += 1
                LOGGER.exception("Binance stream error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                LOGGER.info("Reconnecting to Binance stream (attempt %s)", self._reconnect_attempt)
            else:
                LOGGER.info("Binance stream ended normally, reconnecting")
                await asyncio.sleep(1.0)

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()


async def start_binance_stream(symbols: List[str]) -> None:
    """Public entry point to start the Binance streaming task."""

    stream = BinanceStream(symbols)
    try:
        await stream.run()
    finally:
        await stream.close()


__all__ = ["start_binance_stream", "BinanceStream", "BinanceClients"]
