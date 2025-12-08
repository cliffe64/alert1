"""通知器抽象，确保告警通道可插拔。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(slots=True)
class NotificationMessage:
    """标准化的通知载荷，方便多通道复用。"""

    title: str
    body: str
    category: str = "price"  # price | system


@dataclass(slots=True)
class NotifierTestResult:
    """自检结果：供前端“测试”按钮展示。"""

    ok: bool
    detail: str = ""


class Notifier(Protocol):
    """通知器统一接口，便于新增 DingTalk/本地声音/Telegram。"""

    name: str

    def enabled(self) -> bool:
        """返回当前通道是否开启，由前端配置驱动。"""

    async def send(self, message: NotificationMessage) -> bool:
        """发送告警消息，返回是否成功。"""

    async def self_test(self) -> NotifierTestResult:
        """触发自检，用于前端测试按钮。"""
