"""Utilities for rolling up lower timeframe bars into higher resolutions."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from storage.sqlite_manager import _connect, _connection_lock, upsert_bar

LOGGER = logging.getLogger(__name__)


@dataclass
class RollupStats:
    aggregated: int = 0
    skipped: int = 0


def _validate_window(window: int) -> None:
    if window <= 0:
        raise ValueError("window must be positive")


def _target_table(window: int) -> str:
    if window == 5:
        return "bars_5m"
    if window == 15:
        return "bars_15m"
    raise ValueError(f"Unsupported rollup window: {window}")


def _bucket_close_ts(close_ts: int, window: int) -> int:
    # Align to multiples of the window
    return ((close_ts - 1) // (window * 60) + 1) * window * 60


def _aggregate_bucket(symbol: str, bars: List[Dict[str, float]]) -> Dict[str, float]:
    bars_sorted = sorted(bars, key=lambda item: item["close_ts"])
    first = bars_sorted[0]
    last = bars_sorted[-1]
    return {
        "source": first["source"],
        "exchange": first["exchange"],
        "chain": first.get("chain", ""),
        "symbol": symbol,
        "base": first.get("base", ""),
        "quote": first.get("quote", ""),
        "open_ts": first["open_ts"],
        "close_ts": last["close_ts"],
        "open": first["open"],
        "high": max(bar["high"] for bar in bars_sorted),
        "low": min(bar["low"] for bar in bars_sorted),
        "close": last["close"],
        "volume_base": sum(bar.get("volume_base", 0.0) for bar in bars_sorted),
        "volume_quote": sum(bar.get("volume_quote", 0.0) for bar in bars_sorted),
        "notional_usd": sum(bar.get("notional_usd", 0.0) for bar in bars_sorted),
        "trades": int(sum(bar.get("trades", 0) for bar in bars_sorted)),
    }


def _fetch_symbols(table: str, since_ts: Optional[int] = None) -> List[str]:
    query = f"SELECT DISTINCT symbol FROM {table}"
    params: List[int] = []
    if since_ts is not None:
        query += " WHERE close_ts >= ?"
        params.append(since_ts)
    with _connection_lock:
        with _connect() as conn:
            cur = conn.execute(query, params)
            rows = [row[0] for row in cur.fetchall()]
    return rows


def _load_bars(table: str, symbol: str, since_ts: Optional[int]) -> List[Dict[str, float]]:
    query = f"SELECT * FROM {table} WHERE symbol = ?"
    params: List[int | str] = [symbol]
    if since_ts is not None:
        query += " AND close_ts >= ?"
        params.append(since_ts)
    query += " ORDER BY close_ts ASC"
    with _connection_lock:
        with _connect() as conn:
            conn.row_factory = None
            cur = conn.execute(query, params)
            columns = [col[0] for col in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    return rows


def rollup_bars(
    src_table: str = "bars_1m",
    dst_table: Optional[str] = None,
    window: int = 5,
    since_ts: Optional[int] = None,
) -> RollupStats:
    """Aggregate lower timeframe bars into ``dst_table``.

    Parameters
    ----------
    src_table:
        Source table name (defaults to ``bars_1m``).
    dst_table:
        Destination table (inferred from window when omitted).
    window:
        Number of minutes to aggregate.
    since_ts:
        Optional lower bound for source ``close_ts``.
    """

    _validate_window(window)
    target = dst_table or _target_table(window)
    stats = RollupStats()

    symbols = _fetch_symbols(src_table, since_ts)
    for symbol in symbols:
        rows = _load_bars(src_table, symbol, since_ts)
        if not rows:
            continue
        buckets: Dict[int, List[Dict[str, float]]] = {}
        previous_close: Optional[int] = None
        for row in rows:
            close_ts = int(row["close_ts"])
            if previous_close and close_ts - previous_close > 60:
                LOGGER.warning(
                    "Gap detected for %s: prev=%s current=%s", symbol, previous_close, close_ts
                )
            previous_close = close_ts
            bucket_key = _bucket_close_ts(close_ts, window)
            buckets.setdefault(bucket_key, []).append(row)

        for close_ts, bars in buckets.items():
            if not any(int(bar["close_ts"]) == close_ts for bar in bars):
                LOGGER.debug(
                    "Incomplete bucket for %s ending at %s; aggregating available bars",
                    symbol,
                    close_ts,
                )
                stats.skipped += 1
            aggregated = _aggregate_bucket(symbol, bars)
            aggregated["close_ts"] = close_ts
            upsert_bar(target, aggregated)
            stats.aggregated += 1

    return stats


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rollup utility")
    parser.add_argument("--timeframe", choices=["5m", "15m"], required=True)
    parser.add_argument("--since", type=int, default=None, help="unix timestamp lower bound")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)
    window = 5 if args.timeframe == "5m" else 15
    stats = rollup_bars(window=window, since_ts=args.since)
    LOGGER.info("Rollup complete: %s", json.dumps(stats.__dict__))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
