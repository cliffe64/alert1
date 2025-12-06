"""DingTalk notification helper."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
from typing import Optional
from urllib.parse import urlencode

import httpx

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


__all__ = ["send_markdown", "send_test"]
