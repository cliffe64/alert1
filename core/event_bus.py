"""轻量级事件总线，用于解耦模块间通信。"""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, DefaultDict, Iterable, List

from core.events import EventEnvelope

Subscriber = Callable[[EventEnvelope], None]


class EventBus:
    """发布/订阅机制，供数据源、引擎和通知模块共享。"""

    def __init__(self) -> None:
        self._subscribers: DefaultDict[str, List[Subscriber]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Subscriber) -> None:
        """注册特定事件类型的回调。"""

        self._subscribers[event_type].append(handler)

    def publish(self, envelope: EventEnvelope) -> None:
        """将事件分发给对应类型的订阅者。"""

        for handler in list(self._subscribers.get(envelope.event.event_type.value, [])):
            handler(envelope)

    def subscribers(self, event_type: str) -> Iterable[Subscriber]:
        """便于测试/调试时查看订阅者。"""

        return tuple(self._subscribers.get(event_type, ()))
