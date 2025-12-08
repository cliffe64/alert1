"""事件定义：用于监控结果与系统健康的统一结构体。

事件层保持传输无关性，监控引擎、UI 与通知器通过共享的类型而非零散的字典通信。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping, Optional


class EventType(str, Enum):
    """事件的顶层分类。"""

    PRICE_ALERT = "price_alert"
    SYSTEM_FAULT = "system_fault"
    HEALTH_UPDATE = "health_update"


class Severity(str, Enum):
    """事件严重程度。"""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(slots=True)
class EventBase:
    """所有事件共享的公共字段。"""

    event_type: EventType
    severity: Severity
    source: str
    message: str
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PriceAlertEvent(EventBase):
    """监控引擎产生的价格告警事件。"""

    symbol: str = ""
    provider: str = ""
    rule_id: Optional[str] = None
    threshold: Optional[float] = None
    compare: Optional[str] = None
    triggered_price: Optional[float] = None


@dataclass(slots=True)
class SystemFaultEvent(EventBase):
    """系统或数据层故障事件，区分网络/API/限频等类别。"""

    component: str = ""
    endpoint: Optional[str] = None
    category: str = ""  # e.g. network, rate_limit, parsing


@dataclass(slots=True)
class HealthStatus(EventBase):
    """健康度遥测事件，面向 UI/日志展示。"""

    endpoint: str = ""
    healthy: bool = True
    latency_ms: Optional[float] = None
    retries: int = 0


@dataclass(slots=True)
class EventEnvelope:
    """事件包裹体，便于在事件总线上传递时间戳与唯一标识。"""

    event: EventBase
    ts: float
    id: Optional[str] = None
