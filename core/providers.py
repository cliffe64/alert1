"""数据提供者接口，覆盖币安合约与链上代币。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Protocol


@dataclass(slots=True)
class EndpointConfig:
    """用户可配置的 endpoint 条目，用于池化与自动切换。"""

    name: str
    base_url: str
    api_key: Optional[str] = None
    priority: int = 0


@dataclass(slots=True)
class TokenDescriptor:
    """统一的代币元数据，供 UI 展示与监控引擎使用。"""

    identifier: str
    name: str
    symbol: str
    chain: Optional[str] = None
    address: Optional[str] = None
    extra: dict | None = None


@dataclass(slots=True)
class Quote:
    """价格/成交量快照。"""

    symbol: str
    price: float
    volume: Optional[float] = None
    ts: float = 0.0


class Provider(Protocol):
    """数据提供模块的统一契约。"""

    name: str

    def configure_endpoints(self, endpoints: Iterable[EndpointConfig]) -> None:
        """设置 endpoint 池，后续健康检查/自动切换会使用同一份配置。"""

    def list_futures_contracts(self) -> List[TokenDescriptor]:
        """从币安自动拉取可交易合约列表，供前端搜索/勾选。"""

    def search_tokens(self, query: str) -> List[TokenDescriptor]:
        """支持按名称/符号/地址搜索，优先名称联想，失败再用地址兜底。"""

    def resolve_token(self, address: str) -> Optional[TokenDescriptor]:
        """基于合约地址的兜底解析，当名称搜索无结果时调用。"""

    def current_quote(self, token: TokenDescriptor) -> Optional[Quote]:
        """获取指定代币的最新行情，用于监控引擎轮询。"""
