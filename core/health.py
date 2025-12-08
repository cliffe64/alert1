"""Endpoint 池健康管理与容灾辅助。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional


@dataclass(slots=True)
class Endpoint:
    """单个 endpoint 的运行时状态。"""

    name: str
    base_url: str
    api_key: Optional[str] = None
    priority: int = 0
    last_checked: float = 0.0
    consecutive_failures: int = 0
    latency_ms: float = 0.0
    healthy: bool = True
    failure_reason: str = ""


@dataclass(slots=True)
class HealthCheckResult:
    """一次健康检查的结果，用于日志与 UI 展示。"""

    endpoint: Endpoint
    ok: bool
    reason: str = ""
    latency_ms: Optional[float] = None


class EndpointPool:
    """带失败计数与优先级的轮询池。"""

    def __init__(self, endpoints: Iterable[Endpoint]) -> None:
        self._endpoints: List[Endpoint] = sorted(list(endpoints), key=lambda e: e.priority)
        self._cursor = 0

    @property
    def endpoints(self) -> List[Endpoint]:
        return list(self._endpoints)

    def next(self) -> Endpoint:
        """返回下一个候选 endpoint，并推进游标。"""

        endpoint = self._endpoints[self._cursor % len(self._endpoints)]
        self._cursor = (self._cursor + 1) % len(self._endpoints)
        return endpoint

    def mark_success(self, endpoint: Endpoint, latency_ms: float) -> None:
        """标记成功：刷新延迟、清零失败次数并标记为健康。"""

        endpoint.latency_ms = latency_ms
        endpoint.last_checked = time.time()
        endpoint.consecutive_failures = 0
        endpoint.healthy = True
        endpoint.failure_reason = ""

    def mark_failure(self, endpoint: Endpoint, reason: str) -> None:
        """标记失败：累计失败次数并记录原因，便于 UI 呈现。"""

        endpoint.consecutive_failures += 1
        endpoint.last_checked = time.time()
        endpoint.healthy = False
        endpoint.latency_ms = 0.0
        endpoint.failure_reason = reason

    def choose_healthy(self) -> Optional[Endpoint]:
        """挑选当前健康的 endpoint；全部失败时返回 None。"""

        healthy = [ep for ep in self._endpoints if ep.healthy]
        if healthy:
            return healthy[0]
        return None

    def snapshot(self) -> List[Dict[str, object]]:
        """生成可序列化快照，用于日志与前端展示切换决策。"""

        return [
            {
                "name": ep.name,
                "base_url": ep.base_url,
                "healthy": ep.healthy,
                "latency_ms": ep.latency_ms,
                "failures": ep.consecutive_failures,
                "last_checked": ep.last_checked,
                "reason": ep.failure_reason,
            }
            for ep in self._endpoints
        ]
