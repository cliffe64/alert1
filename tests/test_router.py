import asyncio
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alerts.router import NotificationService
from core.config_models import AppConfig, NotifierSwitch
from core.event_bus import EventBus
from core.events import EventEnvelope, EventType, PriceAlertEvent, Severity, SystemFaultEvent
from alerts.notifiers.base import NotificationMessage, Notifier, NotifierTestResult


@dataclass
class _FakeNotifier(Notifier):
    name: str
    enabled_flag: bool = True
    messages: List[NotificationMessage] = field(default_factory=list)

    def enabled(self) -> bool:
        return self.enabled_flag

    async def send(self, message: NotificationMessage) -> bool:
        self.messages.append(message)
        return True

    async def self_test(self) -> NotifierTestResult:
        return NotifierTestResult(ok=True, detail="fake")


def _config(enabled: bool = True) -> AppConfig:
    return AppConfig(endpoints=[], targets=[], notifiers=[NotifierSwitch(name="dingtalk", enabled=enabled)])


def test_price_alert_routed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        bus = EventBus()
        fake = _FakeNotifier(name="dingtalk")

        monkeypatch.setattr(NotificationService, "_build_notifiers", lambda self, switches: {"dingtalk": fake})
        NotificationService(event_bus=bus, config=_config())

        event = PriceAlertEvent(
            event_type=EventType.PRICE_ALERT,
            severity=Severity.WARNING,
            source="tester",
            message="price crossed threshold",
            symbol="BTCUSDT",
        )
        bus.publish(EventEnvelope(event=event, ts=time.time()))
        await asyncio.sleep(0)

        assert len(fake.messages) == 1
        assert "BTCUSDT" in fake.messages[0].body

    asyncio.run(_run())


def test_system_fault_prefers_high_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        bus = EventBus()
        high = _FakeNotifier(name="dingtalk")
        low = _FakeNotifier(name="local_sound")

        def _build_notifiers(self, switches):
            return {"dingtalk": high, "local_sound": low}

        config = AppConfig(
            endpoints=[],
            targets=[],
            notifiers=[NotifierSwitch(name="dingtalk", enabled=True), NotifierSwitch(name="local_sound", enabled=True)],
        )
        monkeypatch.setattr(NotificationService, "_build_notifiers", _build_notifiers)
        NotificationService(event_bus=bus, config=config)

        event = SystemFaultEvent(
            event_type=EventType.SYSTEM_FAULT,
            severity=Severity.CRITICAL,
            source="provider",
            message="Endpoint failure",
            component="provider",
            category="network",
        )
        bus.publish(EventEnvelope(event=event, ts=time.time()))
        await asyncio.sleep(0)

        assert len(high.messages) == 1
        assert len(low.messages) == 0

    asyncio.run(_run())
