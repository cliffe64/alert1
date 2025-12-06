"""Notification routing for generated events."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from alerts import dingtalk, local_sound
from rules.config_loader import AppConfig, load_config
from storage.sqlite_manager import (
    fetch_undelivered_events,
    mark_event_delivered,
    should_rate_limit,
    update_rate_limit_timestamp,
)

LOGGER = logging.getLogger(__name__)


def _format_detail(detail_json: str) -> str:
    try:
        detail = json.loads(detail_json)
    except Exception:
        return detail_json
    return ", ".join(f"{k}: {v}" for k, v in detail.items())


def _format_markdown(event: dict) -> tuple[str, str]:
    ts = datetime.fromtimestamp(int(event["ts"]), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    detail = _format_detail(event.get("detail_json") or "{}")
    title = f"[{event['severity'].upper()}] {event['symbol']} {event['rule']}"
    text = (
        f"# [{event['severity'].upper()}] {event['symbol']} {event['rule']} {event['timeframe']}\n"
        f"- 时间(UTC): {ts}\n"
        f"- 详情: {detail}\n"
        f"- 说明: {event.get('message', '')}"
    )
    return title, text


async def _deliver_dingtalk(config: AppConfig, event: dict) -> bool:
    notifier = config.notifiers.dingtalk
    webhook = notifier.webhook
    if not notifier.enabled or not webhook:
        return True
    title, text = _format_markdown(event)
    try:
        await dingtalk.send_markdown(title, text, webhook, notifier.secret)
        return True
    except Exception as exc:  # pragma: no cover - network errors
        LOGGER.exception("Failed to deliver DingTalk notification: %s", exc)
        return False


def _deliver_sound(config: AppConfig, event: dict) -> None:
    notifier = config.notifiers.local_sound
    if not notifier.enabled:
        return
    local_sound.play(notifier.sound_file, notifier.volume)


def _rate_limit_key(event: dict) -> str:
    return f"{event.get('symbol')}|{event.get('rule')}|{event.get('timeframe')}"


async def dispatch_new_events(config: Optional[AppConfig] = None) -> int:
    """Send undelivered events to configured notifiers."""

    cfg = config or load_config()
    events = fetch_undelivered_events()
    delivered = 0
    window = max(cfg.notification_rate_limit_minutes, 0) * 60
    for event in events:
        event_ts = int(event.get("ts") or datetime.now(timezone.utc).timestamp())
        key = _rate_limit_key(event)
        if should_rate_limit(key, window, event_ts):
            LOGGER.info("Skip notification due to rate limit: %s", key)
            mark_event_delivered(event["id"])
            continue
        success = await _deliver_dingtalk(cfg, event)
        if success:
            _deliver_sound(cfg, event)
            update_rate_limit_timestamp(key, event_ts)
            mark_event_delivered(event["id"])
            delivered += 1
    return delivered


__all__ = ["dispatch_new_events"]
