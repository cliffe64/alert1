"""Standalone local notification client that replays undelivered events."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from dataclasses import dataclass
from typing import Callable, Optional

from alerts import local_sound
from rules.config_loader import LocalSoundNotifierConfig, load_config
from storage import sqlite_manager

LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


@dataclass
class LocalNotifierSettings:
    """Runtime configuration for :class:`LocalNotifier`."""

    client_id: str = "default"
    poll_interval: float = 5.0
    min_severity: str = "info"
    dry_run: bool = False


class LocalNotifier:
    """Poll the SQLite events table and play local audio alerts."""

    def __init__(
        self,
        sound_config: LocalSoundNotifierConfig,
        settings: LocalNotifierSettings,
        now_func: Callable[[], int] = lambda: int(time.time()),
    ) -> None:
        self._sound_config = sound_config
        self._settings = settings
        self._now = now_func
        self._last_created_at: Optional[int] = None
        self._running = True
        state = sqlite_manager.get_local_notifier_state(settings.client_id)
        if state:
            self._last_created_at = state.get("last_created_at")
            LOGGER.info(
                "Recovered notifier state client=%s last_event=%s created_at=%s",
                settings.client_id,
                state.get("last_event_id"),
                state.get("last_created_at"),
            )

    async def poll_once(self) -> int:
        """Fetch and emit events once. Returns number of processed events."""

        events = sqlite_manager.fetch_events_since(
            self._last_created_at,
            limit=200,
            min_severity=self._settings.min_severity,
        )
        processed = 0
        for event in events:
            created_at = int(event.get("created_at", 0))
            summary = f"[{event.get('severity', 'info').upper()}] {event.get('symbol')} {event.get('rule')}"
            LOGGER.info("Local alert: %s %s", summary, event.get("message", ""))
            if not self._settings.dry_run and self._sound_config.enabled:
                await asyncio.to_thread(
                    local_sound.play,
                    self._sound_config.sound_file,
                    self._sound_config.volume,
                )
            sqlite_manager.update_local_notifier_state(
                self._settings.client_id,
                event.get("id"),
                created_at,
                self._now(),
            )
            self._last_created_at = created_at
            processed += 1
        return processed

    async def run(self) -> None:
        """Run the polling loop until interrupted."""

        backoff = 1.0
        while self._running:
            try:
                processed = await self.poll_once()
                if processed:
                    backoff = 1.0
            except asyncio.CancelledError:  # pragma: no cover - runtime cancellation
                raise
            except Exception as exc:  # pragma: no cover - protective fallback
                LOGGER.exception("Local notifier error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            else:
                await asyncio.sleep(self._settings.poll_interval)

    def stop(self) -> None:
        self._running = False


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local sound notification agent")
    parser.add_argument("--client-id", default="default", help="State identifier for this client")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="Polling interval in seconds")
    parser.add_argument(
        "--min-severity",
        default="info",
        choices=("info", "warning", "error", "critical"),
        help="Ignore events below this severity",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not play sounds")
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run configuration diagnostics and exit",
    )
    return parser.parse_args()


def _run_self_test(sound_config: LocalSoundNotifierConfig) -> None:
    LOGGER.info("Local notifier self-test")
    if sound_config.enabled:
        LOGGER.info(
            "Sound enabled with file=%s volume=%.2f",
            sound_config.sound_file,
            sound_config.volume,
        )
        local_sound.test_play(sound_config.sound_file)
    else:
        LOGGER.warning("Local sound notifier disabled in configuration")


async def _async_main(args: argparse.Namespace) -> None:
    config = load_config()
    sound_config = config.notifiers.local_sound
    if args.self_test:
        _run_self_test(sound_config)
        return

    settings = LocalNotifierSettings(
        client_id=args.client_id,
        poll_interval=args.poll_interval,
        min_severity=args.min_severity,
        dry_run=args.dry_run,
    )
    notifier = LocalNotifier(sound_config, settings)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal(_: int, __: Optional[object]) -> None:
        notifier.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):  # pragma: no branch - OS dependent
        try:
            loop.add_signal_handler(sig, _handle_signal, sig, None)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            signal.signal(sig, lambda _s, _f: _handle_signal(_s, None))

    await asyncio.gather(notifier.run(), stop_event.wait())


def main() -> None:
    _configure_logging()
    args = _parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
