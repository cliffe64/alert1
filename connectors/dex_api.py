"""Extensible DEX ingestion utilities with adapter-based architecture."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Protocol

import httpx

from storage.sqlite_manager import list_tokens, upsert_bar

LOGGER = logging.getLogger(__name__)


class DexAdapter(Protocol):
    """Protocol describing a DEX data source adapter."""

    name: str
    rate_limit_seconds: float

    async def fetch_1m_bar(
        self,
        chain: str,
        token_address: str,
        pool_address: Optional[str],
        since_ts: Optional[int],
    ) -> Iterable[Dict[str, object]]:
        ...


async def _throttle(name: str, seconds: float) -> None:
    if seconds <= 0:
        return
    now = time.monotonic()
    last = _LAST_REQUEST.get(name)
    if last is not None:
        wait = seconds - (now - last)
        if wait > 0:
            LOGGER.debug("Throttling %s adapter for %.2fs", name, wait)
            await asyncio.sleep(wait)
    _LAST_REQUEST[name] = time.monotonic()


def _now_ts() -> int:
    return int(time.time())


@dataclass
class PancakeAdapter:
    """Adapter for PancakeSwap using the Dexscreener public API."""

    name: str = "pancake"
    base_url: str = "https://api.dexscreener.com/latest/dex"
    rate_limit_seconds: float = 1.0

    async def fetch_1m_bar(
        self,
        chain: str,
        token_address: str,
        pool_address: Optional[str],
        since_ts: Optional[int],
    ) -> Iterable[Dict[str, object]]:
        del token_address  # not required by the API
        if not pool_address:
            return []
        await _throttle(self.name, self.rate_limit_seconds)
        endpoint = f"/pairs/{chain}/{pool_address}"
        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0) as client:
            response = await client.get(endpoint)
            response.raise_for_status()
            data = response.json()
        pair = (data.get("pair") or {}) if isinstance(data, dict) else {}
        price = float(pair.get("priceUsd") or 0.0)
        if price <= 0:
            return []
        close_ts = _now_ts()
        open_ts = close_ts - 60
        volume_usd = float(pair.get("volume", {}).get("h24", 0.0))
        return [
            {
                "open_ts": open_ts,
                "close_ts": close_ts,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume_base": 0.0,
                "volume_quote": volume_usd,
                "notional_usd": volume_usd,
                "trades": int(pair.get("txns", {}).get("h24", 0)),
            }
        ]


@dataclass
class UniswapV3Adapter:
    """Adapter for Uniswap V3 using the Dexscreener API for simplicity."""

    name: str = "uniswapv3"
    base_url: str = "https://api.dexscreener.com/latest/dex"
    rate_limit_seconds: float = 1.0

    async def fetch_1m_bar(
        self,
        chain: str,
        token_address: str,
        pool_address: Optional[str],
        since_ts: Optional[int],
    ) -> Iterable[Dict[str, object]]:
        ref_chain = chain or "ethereum"
        pool = pool_address or token_address
        await _throttle(self.name, self.rate_limit_seconds)
        endpoint = f"/pairs/{ref_chain}/{pool}"
        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0) as client:
            response = await client.get(endpoint)
            response.raise_for_status()
            data = response.json()
        pair = (data.get("pair") or {}) if isinstance(data, dict) else {}
        price = float(pair.get("priceUsd") or 0.0)
        if price <= 0:
            return []
        close_ts = _now_ts()
        open_ts = close_ts - 60
        volume_usd = float(pair.get("volume", {}).get("h24", 0.0))
        return [
            {
                "open_ts": open_ts,
                "close_ts": close_ts,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume_base": 0.0,
                "volume_quote": volume_usd,
                "notional_usd": volume_usd,
                "trades": int(pair.get("txns", {}).get("h24", 0)),
            }
        ]


_ADAPTERS: Dict[str, DexAdapter] = {
    "pancake": PancakeAdapter(),
    "uniswapv3": UniswapV3Adapter(),
}

_LAST_REQUEST: Dict[str, float] = {}
_FAIL_STATES: Dict[str, Dict[str, float]] = {}


def register_adapter(name: str, adapter: DexAdapter) -> None:
    """Register or override a DEX adapter."""

    _ADAPTERS[name] = adapter


def _is_in_cooldown(name: str) -> bool:
    state = _FAIL_STATES.get(name)
    if not state:
        return False
    remaining = state.get("snooze_until", 0.0) - time.monotonic()
    if remaining > 0:
        LOGGER.warning("Adapter %s cooling down for %.1fs", name, remaining)
        return True
    _FAIL_STATES.pop(name, None)
    return False


def _record_failure(name: str) -> None:
    state = _FAIL_STATES.get(name, {"fail_count": 0, "snooze_until": 0.0})
    count = state.get("fail_count", 0) + 1
    cooldown = min(60.0 * count, 600.0)
    _FAIL_STATES[name] = {
        "fail_count": count,
        "snooze_until": time.monotonic() + cooldown,
    }
    LOGGER.error("Adapter %s failure #%s, entering cooldown %.1fs", name, count, cooldown)


async def fetch_1m_bar(
    chain: str,
    token_address: str,
    pool_address: Optional[str],
    since_ts: Optional[int],
    exchange: str = "pancake",
) -> List[Dict[str, object]]:
    """Fetch 1m bars via the specified adapter."""

    adapter = _ADAPTERS.get(exchange)
    if not adapter:
        raise ValueError(f"No adapter registered for {exchange}")
    if _is_in_cooldown(adapter.name):
        return []
    try:
        bars = await adapter.fetch_1m_bar(chain, token_address, pool_address, since_ts)
        _FAIL_STATES.pop(adapter.name, None)
        return list(bars)
    except Exception as exc:  # pragma: no cover - network failures
        LOGGER.exception("Failed to fetch DEX bars via %s: %s", adapter.name, exc)
        _record_failure(adapter.name)
        return []


async def sync_registered_tokens(
    since_ts: Optional[int] = None,
    limit: Optional[int] = None,
) -> int:
    """Synchronise enabled tokens from the registry into ``bars_1m``."""

    tokens = list_tokens(enabled=True)
    if limit is not None:
        tokens = tokens[:limit]
    inserted = 0
    for token in tokens:
        exchange = token.get("exchange") or "pancake"
        adapter = _ADAPTERS.get(exchange)
        if not adapter:
            LOGGER.debug("No adapter registered for %s", exchange)
            continue
        if _is_in_cooldown(adapter.name):
            continue
        try:
            bars = await fetch_1m_bar(
                token.get("chain", ""),
                token.get("token_address", ""),
                token.get("pool_address"),
                since_ts,
                exchange=exchange,
            )
        except Exception:  # pragma: no cover - handled in fetch_1m_bar
            continue
        for bar in bars:
            payload = {
                **bar,
                "source": "dex",
                "exchange": exchange,
                "chain": token.get("chain", ""),
                "symbol": token["symbol"],
                "base": token.get("base", ""),
                "quote": token.get("quote", ""),
            }
            upsert_bar("bars_1m", payload)
            inserted += 1
    return inserted


__all__ = [
    "fetch_1m_bar",
    "register_adapter",
    "sync_registered_tokens",
    "PancakeAdapter",
    "UniswapV3Adapter",
]
