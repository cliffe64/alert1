"""Notification routing via EventBus subscriptions."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Coroutine, Dict, Iterable, Optional

from alerts.dingtalk import DingTalkNotifier
from alerts.local_sound import LocalSoundNotifier
from alerts.telegram import TelegramNotifier
from alerts.notifiers.base import Notifier, NotificationMessage
from core.config_models import AppConfig, NotifierSwitch
from core.event_bus import EventBus
from core.events import EventEnvelope, EventType, PriceAlertEvent, SystemFaultEvent

LOGGER = logging.getLogger(__name__)


def _format_detail(detail: dict | None) -> str:
    if not detail:
        return ""
    try:
        return ", ".join(f"{k}: {v}" for k, v in detail.items())
    except Exception:
        return json.dumps(detail, ensure_ascii=False)


def _timestamp(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class NotificationService:
    """Subscribe to events and fan them out to enabled notifiers."""

    HIGH_PRIORITY_CHANNELS: tuple[str, ...] = ("dingtalk",)

    def __init__(self, event_bus: EventBus, config: AppConfig) -> None:
        self.event_bus = event_bus
        self.config = config
        self._notifiers: Dict[str, Notifier] = self._build_notifiers(self.config.notifiers)
        self.event_bus.subscribe(EventType.PRICE_ALERT.value, self._on_price_alert)
        self.event_bus.subscribe(EventType.SYSTEM_FAULT.value, self._on_system_fault)
        LOGGER.info("NotificationService initialized with channels: %s", list(self._notifiers))

    def _build_notifiers(self, switches: Iterable[NotifierSwitch]) -> Dict[str, Notifier]:
        registry: Dict[str, Notifier] = {}
        for switch in switches:
            notifier = self._create_notifier(switch)
            if notifier:
                registry[notifier.name] = notifier
        return registry

    def _create_notifier(self, switch: NotifierSwitch) -> Optional[Notifier]:
        if switch.name == "dingtalk":
            webhook = getattr(switch, "webhook", None) or os.environ.get("DINGTALK_WEBHOOK")
            secret = getattr(switch, "secret", None) or os.environ.get("DINGTALK_SECRET")
            return DingTalkNotifier(
                webhook=webhook,
                secret=secret,
                enabled_flag=switch.enabled,
            )
        if switch.name == "local_sound":
            return LocalSoundNotifier(enabled_flag=switch.enabled)
        if switch.name == "telegram":
            return TelegramNotifier(enabled_flag=switch.enabled)
        LOGGER.warning("Unknown notifier channel: %s", switch.name)
        return None

    def _schedule(self, coro: Coroutine[Any, Any, None]) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            asyncio.run(coro)

    def _on_price_alert(self, envelope: EventEnvelope) -> None:
        self._schedule(self._dispatch_price_alert(envelope))

    def _on_system_fault(self, envelope: EventEnvelope) -> None:
        self._schedule(self._dispatch_system_fault(envelope))

    async def _dispatch_price_alert(self, envelope: EventEnvelope) -> None:
        event = envelope.event
        if not isinstance(event, PriceAlertEvent):
            LOGGER.debug("Skip non-price alert event: %s", event)
            return
        message = NotificationMessage(
            title=f"[{event.severity.value.upper()}] {event.symbol} 价格告警",
            body=self._price_body(event, envelope.ts),
            category="price",
        )
        await self._send_to_enabled(message)

    async def _dispatch_system_fault(self, envelope: EventEnvelope) -> None:
        event = envelope.event
        if not isinstance(event, SystemFaultEvent):
            LOGGER.debug("Skip non-system fault event: %s", event)
            return
        message = NotificationMessage(
            title=f"[SYSTEM] {event.component} 故障",
            body=self._system_body(event, envelope.ts),
            category="system",
        )
        high_priority = [
            switch.name
            for switch in self.config.notifiers
            if switch.enabled and switch.name in self.HIGH_PRIORITY_CHANNELS
        ]
        await self._send_to_enabled(message, force_channels=high_priority or None)

    async def _send_to_enabled(
        self, message: NotificationMessage, force_channels: Optional[Iterable[str]] = None
    ) -> None:
        channels = force_channels or [nf.name for nf in self.config.notifiers if nf.enabled]
        for name in channels:
            notifier = self._notifiers.get(name)
            if not notifier or not notifier.enabled():
                LOGGER.debug("Notifier %s unavailable or disabled", name)
                continue
            success = await notifier.send(message)
            if success:
                LOGGER.info("Delivered %s via %s", message.category, name)
            else:
                LOGGER.warning("Failed to deliver %s via %s", message.category, name)

    def _price_body(self, event: PriceAlertEvent, ts: float) -> str:
        detail = _format_detail(event.detail) or "无"
        rule_part = f"规则: {event.rule_id}" if event.rule_id else ""
        threshold = f"阈值: {event.threshold} {event.compare}" if event.threshold else ""
        price = f"触发价: {event.triggered_price}" if event.triggered_price else ""
        return (
            f"# 价格告警\n"
            f"- 符号: {event.symbol}\n"
            f"- 时间(UTC): {_timestamp(ts)}\n"
            f"- 来源: {event.source}\n"
            f"- {event.message}\n"
            f"- {rule_part}\n"
            f"- {threshold}\n"
            f"- {price}\n"
            f"- 详情: {detail}"
        )

    def _system_body(self, event: SystemFaultEvent, ts: float) -> str:
        detail = _format_detail(event.detail) or "无"
        return (
            f"# 系统故障\n"
            f"- 时间(UTC): {_timestamp(ts)}\n"
            f"- 组件: {event.component}\n"
            f"- Endpoint: {event.endpoint or '未知'}\n"
            f"- 类别: {event.category}\n"
            f"- 信息: {event.message}\n"
            f"- 详情: {detail}"
        )


__all__ = ["NotificationService"]
