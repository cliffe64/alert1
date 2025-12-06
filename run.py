"""Main entry point orchestrating the alert service."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Awaitable, Callable

from aggregator.rollup import rollup_bars
from alerts.router import dispatch_new_events
from connectors import start_binance_stream, sync_registered_tokens
from rules.config_loader import load_config
from rules.price_alerts import scan_price_alerts
from rules.trend_channel import scan_trend_channel
from rules.volume_spike import run_volume_spike

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crypto alerting controller")
    parser.add_argument("--once", action="store_true", help="Run a single iteration")
    parser.add_argument("--loop", action="store_true", help="Start long running loop")
    return parser.parse_args()


async def run_once() -> None:
    await sync_registered_tokens()
    await asyncio.to_thread(rollup_bars, "bars_1m", "bars_5m", 5)
    await asyncio.to_thread(rollup_bars, "bars_1m", "bars_15m", 15)
    await asyncio.to_thread(run_volume_spike, "5m")
    await asyncio.to_thread(run_volume_spike, "15m")
    await asyncio.to_thread(scan_trend_channel, "5m")
    await asyncio.to_thread(scan_trend_channel, "15m")
    await asyncio.to_thread(scan_price_alerts)
    await dispatch_new_events()


async def _periodic(name: str, interval: int, coro: Callable[[], Awaitable[None]]) -> None:
    while True:
        try:
            await coro()
        except Exception as exc:  # pragma: no cover
            LOGGER.exception("Task %s failed: %s", name, exc)
        await asyncio.sleep(interval)


async def _rollup_task() -> None:
    async def _run() -> None:
        await asyncio.to_thread(rollup_bars, "bars_1m", "bars_5m", 5)
        await asyncio.to_thread(rollup_bars, "bars_1m", "bars_15m", 15)

    await _periodic("rollup", 60, _run)


async def _rules_task() -> None:
    async def _run() -> None:
        await asyncio.to_thread(run_volume_spike, "5m")
        await asyncio.to_thread(run_volume_spike, "15m")
        await asyncio.to_thread(scan_trend_channel, "5m")
        await asyncio.to_thread(scan_trend_channel, "15m")
        await asyncio.to_thread(scan_price_alerts)

    await _periodic("rules", 30, _run)


async def _notify_task() -> None:
    await _periodic("notify", 5, dispatch_new_events)


async def _dex_task() -> None:
    async def _run() -> None:
        await sync_registered_tokens()

    await _periodic("dex", 60, _run)


async def loop_forever() -> None:
    config = load_config()
    tasks = [
        asyncio.create_task(start_binance_stream(config.symbols), name="binance"),
        asyncio.create_task(_dex_task(), name="dex"),
        asyncio.create_task(_rollup_task(), name="rollup"),
        asyncio.create_task(_rules_task(), name="rules"),
        asyncio.create_task(_notify_task(), name="notify"),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def run_async(entry: Callable[[], Awaitable[None]]) -> None:
    try:
        asyncio.run(entry())
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user.")


def main() -> None:
    configure_logging()
    args = parse_args()
    if args.once and args.loop:
        raise SystemExit("--once and --loop cannot be combined")
    if args.once:
        run_async(run_once)
    elif args.loop:
        run_async(loop_forever)
    else:
        raise SystemExit("Specify --once or --loop")


if __name__ == "__main__":
    main()
