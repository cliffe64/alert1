"""Main entry point orchestrating the alert service."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Awaitable, Callable, Iterable

from aggregator.rollup import rollup_bars
from alerts.router import NotificationService
from connectors import sync_registered_tokens
from connectors.binance_provider import BinanceFuturesProvider
from connectors.onchain_provider import OnChainProvider
from core.event_bus import EventBus
from core.providers import EndpointConfig, Provider, TokenDescriptor
from rules.price_alerts import scan_price_alerts
from rules.trend_channel import scan_trend_channel
from rules.volume_spike import run_volume_spike
from core.config_models import MonitoredTarget
from storage.app_config_store import load_app_config

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


async def _dex_task() -> None:
    async def _run() -> None:
        await sync_registered_tokens()

    await _periodic("dex", 60, _run)


def _is_onchain_token(token: TokenDescriptor) -> bool:
    return bool(token.chain or token.address or ((token.extra or {}).get("type") == "onchain"))


def _select_provider(
    token: TokenDescriptor,
    futures_provider: BinanceFuturesProvider,
    onchain_provider: OnChainProvider,
) -> Provider:
    return onchain_provider if _is_onchain_token(token) else futures_provider


async def _monitor_task(
    futures_provider: BinanceFuturesProvider,
    onchain_provider: OnChainProvider,
    endpoint_urls: str,
    targets: Iterable[MonitoredTarget],
) -> None:
    async def _run() -> None:
        for target in targets:
            if not getattr(target, "enabled", True):
                continue
            provider = _select_provider(target.token, futures_provider, onchain_provider)
            LOGGER.info(
                "正在通过 %s 监控 %s (endpoint: %s)",
                provider.name,
                target.token.symbol,
                endpoint_urls,
            )
            if hasattr(provider, "current_quote_async"):
                await provider.current_quote_async(target.token)  # type: ignore[attr-defined]
            else:
                await asyncio.to_thread(provider.current_quote, target.token)

    await _periodic("monitor", 5, _run)


async def loop_forever() -> None:
    app_config = load_app_config()
    event_bus = EventBus()
    NotificationService(event_bus=event_bus, config=app_config)
    futures_provider = BinanceFuturesProvider(event_bus=event_bus)
    futures_provider.configure_endpoints(
        EndpointConfig(name=ep.name, base_url=ep.base_url, api_key=ep.api_key, priority=ep.priority)
        for ep in app_config.endpoints
    )
    onchain_provider = OnChainProvider(event_bus=event_bus)
    onchain_provider.configure_endpoints(
        EndpointConfig(name=ep.name, base_url=ep.base_url, api_key=ep.api_key, priority=ep.priority)
        for ep in app_config.endpoints
    )
    endpoint_urls = ", ".join(ep.base_url for ep in app_config.endpoints) or "(no endpoints configured)"

    tasks = [
        asyncio.create_task(
            _monitor_task(futures_provider, onchain_provider, endpoint_urls, app_config.targets),
            name="provider",
        ),
        asyncio.create_task(_dex_task(), name="dex"),
        asyncio.create_task(_rollup_task(), name="rollup"),
        asyncio.create_task(_rules_task(), name="rules"),
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
