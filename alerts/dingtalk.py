"""DingTalk notification helper and notifier implementation."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode

import httpx

from alerts.notifiers.base import Notifier, NotifierTestResult, NotificationMessage

LOGGER = logging.getLogger(__name__)


def _sign(secret: str, timestamp: int) -> str:
    string_to_sign = f"{timestamp}\n{secret}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), string_to_sign, digestmod=hashlib.sha256).digest()
    return base64.b64encode(signature).decode("utf-8")


async def send_markdown(title: str, text: str, webhook: str, secret: Optional[str] = None) -> None:
    """Send a markdown message to DingTalk."""

    params = {}
    if secret:
        timestamp = int(time.time() * 1000)
        params["timestamp"] = timestamp
        params["sign"] = _sign(secret, timestamp)
    url = webhook
    if params:
        url = f"{webhook}&{urlencode(params)}"
    payload = {
        "msgtype": "markdown",
        "markdown": {"title": title, "text": text},
    }
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
    LOGGER.info("DingTalk message sent: %s", title)


async def send_test(webhook: str, secret: Optional[str] = None) -> None:
    await send_markdown("Alert Service Test", "Test message", webhook, secret)


@dataclass(slots=True)
class DingTalkNotifier(Notifier):
    """DingTalk 通知器，实现统一接口便于扩展。"""

    webhook: Optional[str]
    secret: Optional[str]
    enabled_flag: bool
    name: str = "dingtalk"

    def enabled(self) -> bool:
        return self.enabled_flag and bool(self.webhook)

    async def send(self, message: NotificationMessage) -> bool:
        if not self.enabled():
            LOGGER.info("DingTalk notifier disabled or missing webhook; skip send")
            return False
        try:
            await send_markdown(message.title, message.body, self.webhook or "", self.secret)
            return True
        except Exception as exc:  # pragma: no cover - network errors
            LOGGER.exception("Failed to send DingTalk message: %s", exc)
            return False

    async def self_test(self) -> NotifierTestResult:
        if not self.webhook:
            return NotifierTestResult(ok=False, detail="Missing webhook for DingTalk")
        try:
            await send_markdown(
                "[TEST] 通知自检",
                "# DingTalk 通道联通\n- 结果: 成功",
                self.webhook,
                self.secret,
            )
            return NotifierTestResult(ok=True, detail="DingTalk webhook reachable")
        except Exception as exc:  # pragma: no cover - network errors
            LOGGER.exception("DingTalk self-test failed: %s", exc)
            return NotifierTestResult(ok=False, detail=str(exc))


__all__ = ["send_markdown", "send_test", "DingTalkNotifier"]
