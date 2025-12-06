"""Historical replay utility producing forward return statistics."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional

from backtest import stats
from storage import sqlite_manager

LOGGER = logging.getLogger(__name__)

DEFAULT_HORIZONS = [30, 60, 120]


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest replay controller")
    parser.add_argument("--symbols", required=True, help="Comma separated list of symbols")
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back")
    parser.add_argument("--timeframe", default="5m", choices=("5m", "15m"))
    parser.add_argument(
        "--rules",
        help="Optional comma separated rule filter (matches events.rule)",
        default=None,
    )
    parser.add_argument(
        "--output-dir",
        default="backtest/out",
        help="Directory where CSV and charts will be written",
    )
    return parser.parse_args(argv)


def _fetch_events(timeframe: str, since_ts: int, symbols: List[str], rules: Optional[List[str]]) -> List[dict]:
    return sqlite_manager.list_events(
        timeframe=timeframe,
        since_ts=since_ts,
        symbols=symbols,
        rules=rules,
    )


def _bars_for_event(symbol: str, event_ts: int, horizon_minutes: int) -> List[dict]:
    since = max(event_ts - 60, 0)
    limit = (horizon_minutes + 5) * 2
    return sqlite_manager.fetch_bars("bars_1m", symbol, since_ts=since, limit=limit)


def run_replay(args: argparse.Namespace) -> None:
    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    rules = [rule.strip() for rule in args.rules.split(",")] if args.rules else None
    since_ts = int(time.time()) - args.days * 86400
    LOGGER.info(
        "Starting replay timeframe=%s symbols=%s since_ts=%s rules=%s",
        args.timeframe,
        symbols,
        since_ts,
        rules,
    )
    events = _fetch_events(args.timeframe, since_ts, symbols, rules)
    if not events:
        LOGGER.warning("No events found for given criteria")
        return

    horizons = DEFAULT_HORIZONS
    event_rows: List[dict] = []
    metrics: List[dict] = []
    max_horizon = max(horizons)
    for event in events:
        bars = _bars_for_event(event["symbol"], int(event["ts"]), max_horizon)
        metric = stats.compute_forward_metrics(bars, int(event["ts"]), horizons)
        if metric is None:
            continue
        row = {
            "event_id": event["id"],
            "symbol": event["symbol"],
            "rule": event["rule"],
            "severity": event.get("severity", ""),
            "ts": event["ts"],
        }
        row.update(metric)
        event_rows.append(row)
        metrics.append(metric)

    if not metrics:
        LOGGER.warning("No metrics computed - insufficient bar data")
        return

    output_dir = Path(args.output_dir)
    events_path = output_dir / f"{args.timeframe}_events.csv"
    fieldnames = [
        "event_id",
        "symbol",
        "rule",
        "severity",
        "ts",
        "base_price",
        "base_ts",
        "max_drawdown",
        *[f"ret_{h}" for h in horizons],
    ]
    stats.write_csv(events_path, event_rows, fieldnames)

    summary = stats.aggregate_metrics(metrics, horizons)
    summary_path = output_dir / f"{args.timeframe}_summary.csv"
    stats.write_csv(summary_path, [summary], summary.keys())

    plot_path = output_dir / f"{args.timeframe}_returns.png"
    stats.plot_distribution([m.get("ret_30") for m in metrics], plot_path)
    LOGGER.info("Backtest artifacts written to %s", output_dir)


def main(argv: Optional[Iterable[str]] = None) -> None:
    _configure_logging()
    args = _parse_args(argv)
    run_replay(args)


if __name__ == "__main__":
    main(sys.argv[1:])
