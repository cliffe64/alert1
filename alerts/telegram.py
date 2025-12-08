"""Telegram notifier placeholder implementing the Notifier protocol."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from alerts.notifiers.base import Notifier, NotifierTestResult, NotificationMessage

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class TelegramNotifier(Notifier):
    """预留的 Telegram 通知器，当前仅占位。"""

    enabled_flag: bool
    name: str = "telegram"

    def enabled(self) -> bool:
        return self.enabled_flag

    async def send(self, message: NotificationMessage) -> bool:  # pragma: no cover - placeholder
        LOGGER.info("Telegram notifier is a placeholder; skip send for %s", message.title)
        return False

    async def self_test(self) -> NotifierTestResult:  # pragma: no cover - placeholder
        return NotifierTestResult(ok=False, detail="Telegram notifier not implemented")


__all__ = ["TelegramNotifier"]
