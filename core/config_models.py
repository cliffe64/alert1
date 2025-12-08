"""前端驱动配置的轻量模型，约束输入最小化并保留扩展性。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from core.providers import TokenDescriptor


@dataclass(slots=True)
class ThresholdRule:
    """监控规则：比较方式、阈值、频率与冷却时间。"""

    rule_id: str
    compare: str  # gt / lt / cross_up / cross_down
    threshold: float
    frequency_sec: int
    cooldown_sec: int


@dataclass(slots=True)
class MonitoredTarget:
    """监控对象，包括币安合约或链上代币。"""

    token: TokenDescriptor
    rules: List[ThresholdRule] = field(default_factory=list)
    enabled: bool = True


@dataclass(slots=True)
class NotifierSwitch:
    """通知通道的开关状态，包含测试按钮需要的元信息。"""

    name: str
    enabled: bool
    testable: bool = True


@dataclass(slots=True)
class EndpointEntry:
    """前端维护的 endpoint 条目，支持排序与可选密钥。"""

    name: str
    base_url: str
    api_key: Optional[str] = None
    priority: int = 0


@dataclass(slots=True)
class AppConfig:
    """完整配置快照，便于 UI 与后端同步。"""

    endpoints: List[EndpointEntry] = field(default_factory=list)
    targets: List[MonitoredTarget] = field(default_factory=list)
    notifiers: List[NotifierSwitch] = field(default_factory=list)
