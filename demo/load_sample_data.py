"""Load demo bars and trigger rule evaluations for a quick showcase."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Iterable, Optional

from aggregator.rollup import rollup_bars
from alerts.router import dispatch_new_events
from rules.price_alerts import scan_price_alerts
from rules.trend_channel import scan_trend_channel
from rules.volume_spike import run_volume_spike
from storage import sqlite_manager
from storage.migrate import initialize_database

LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load sample market data")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(__file__).with_name("sample_bars_1m.json"),
        help="Path to sample bars JSON",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Recreate the SQLite schema before loading",
    )
    return parser.parse_args(argv)


def _load_json(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _upsert_bars(bars: Iterable[dict]) -> int:
    inserted = 0
    for bar in bars:
        sqlite_manager.upsert_bar("bars_1m", bar)
        inserted += 1
    return inserted


async def _run_rules() -> None:
    await asyncio.to_thread(run_volume_spike, "5m")
    await asyncio.to_thread(run_volume_spike, "15m")
    await asyncio.to_thread(scan_trend_channel, "5m")
    await asyncio.to_thread(scan_trend_channel, "15m")
    await asyncio.to_thread(scan_price_alerts)
    await dispatch_new_events()


def main(argv: Optional[Iterable[str]] = None) -> None:
    _configure_logging()
    args = _parse_args(argv)
    if args.reset:
        initialize_database()
        LOGGER.info("Database schema initialized")

    bars = _load_json(args.data)
    inserted = _upsert_bars(bars)
    LOGGER.info("Inserted %s demo bars", inserted)

    rollup_bars("bars_1m", "bars_5m", 5)
    rollup_bars("bars_1m", "bars_15m", 15)

    asyncio.run(_run_rules())
    LOGGER.info("Demo pipeline completed. Open the UI dashboard to inspect events.")


if __name__ == "__main__":
    main()
