"""On-chain token provider implementing the unified Provider interface.

The provider offers search and price resolution for tokens that live on
blockchains (DexScreener-style). It supports endpoint pooling and emits
fault events to the shared EventBus when requests fail.
"""

from __future__ import annotations

import asyncio
import random
from typing import Iterable, List, Optional

import httpx

from core.event_bus import EventBus
from core.events import EventEnvelope, EventType, Severity, SystemFaultEvent
from core.health import Endpoint, EndpointPool
from core.providers import EndpointConfig, Provider, Quote, TokenDescriptor


STATIC_TOKENS: list[TokenDescriptor] = [
    TokenDescriptor(
        identifier="pepe_eth",
        name="Pepe",
        symbol="PEPE",
        chain="ethereum",
        address="0x6982508145454Ce325DdBE47a25d4ec3d2311933",
        extra={"source": "static", "type": "onchain"},
    ),
    TokenDescriptor(
        identifier="usdc_eth",
        name="USD Coin",
        symbol="USDC",
        chain="ethereum",
        address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        extra={"source": "static", "type": "onchain"},
    ),
]


class OnChainProvider(Provider):
    """Provider that resolves and quotes on-chain tokens."""

    name = "onchain"

    def __init__(self, event_bus: EventBus | None = None) -> None:
        self._pool = EndpointPool([])
        self._event_bus = event_bus

    def configure_endpoints(self, endpoints: Iterable[EndpointConfig]) -> None:
        self._pool = EndpointPool(
            Endpoint(name=ep.name, base_url=ep.base_url, api_key=ep.api_key, priority=ep.priority)
            for ep in endpoints
        )

    def list_futures_contracts(self) -> List[TokenDescriptor]:
        # Futures contracts are not applicable to the on-chain provider.
        return []

    async def _request(self, path: str, params: Optional[dict] = None) -> dict:
        errors: list[str] = []
        for endpoint in self._pool.endpoints:
            url = f"{endpoint.base_url.rstrip('/')}{path}"
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(url, params=params)
                if resp.status_code == 200:
                    self._pool.mark_success(endpoint, resp.elapsed.total_seconds() * 1000)
                    return resp.json()
                reason = f"status {resp.status_code}"
                self._pool.mark_failure(endpoint, reason)
                errors.append(f"{endpoint.name}:{reason}")
            except httpx.RequestError as exc:
                self._pool.mark_failure(endpoint, str(exc))
                errors.append(f"{endpoint.name}:{exc}")
                self._emit_fault(endpoint, "network", str(exc))
        raise RuntimeError(";".join(errors) or "no endpoints configured")

    @staticmethod
    def _looks_like_address(query: str) -> bool:
        q = query.lower().strip()
        return q.startswith("0x") or len(q) > 20

    def _emit_fault(self, endpoint: Endpoint, category: str, reason: str) -> None:
        if not self._event_bus:
            return
        event = SystemFaultEvent(
            event_type=EventType.SYSTEM_FAULT,
            severity=Severity.CRITICAL,
            source=self.name,
            message=f"Endpoint failure on {endpoint.name}: {reason}",
            component="provider",
            endpoint=endpoint.base_url,
            category=category,
        )
        self._event_bus.publish(EventEnvelope(event=event, ts=asyncio.get_event_loop().time()))

    async def search_tokens_async(self, query: str) -> List[TokenDescriptor]:
        q = query.strip()
        if not q:
            return []

        results: list[TokenDescriptor] = []
        if self._pool.endpoints:
            try:
                data = await self._request("/search", params={"q": q})
                tokens = data.get("tokens", data if isinstance(data, list) else [])
                for item in tokens:
                    address = item.get("address") or item.get("id")
                    results.append(
                        TokenDescriptor(
                            identifier=item.get("identifier", address or item.get("symbol", q)),
                            name=item.get("name", item.get("symbol", q)),
                            symbol=item.get("symbol", q[:10]),
                            chain=item.get("chain"),
                            address=address,
                            extra={"source": item.get("source", "endpoint"), "type": "onchain"},
                        )
                    )
            except Exception as exc:
                for endpoint in self._pool.endpoints:
                    self._emit_fault(endpoint, "api", str(exc))

        if not results:
            q_lower = q.lower()
            results = [t for t in STATIC_TOKENS if q_lower in t.name.lower() or q_lower in t.symbol.lower()]
            if not results and self._looks_like_address(q):
                resolved = self.resolve_token(q)
                if resolved:
                    results.append(resolved)
        return results

    def search_tokens(self, query: str) -> List[TokenDescriptor]:
        return asyncio.get_event_loop().run_until_complete(self.search_tokens_async(query))

    def resolve_token(self, address: str) -> Optional[TokenDescriptor]:
        addr = address.strip()
        if not addr:
            return None
        symbol = addr[:6] + "..." if len(addr) > 6 else addr
        return TokenDescriptor(
            identifier=addr,
            name=addr,
            symbol=symbol,
            chain=None,
            address=addr,
            extra={"source": "address", "type": "onchain"},
        )

    def _synthetic_price(self, token: TokenDescriptor) -> float:
        random.seed(token.identifier)
        return round(random.random(), 6) or 0.000001

    async def current_quote_async(self, token: TokenDescriptor) -> Optional[Quote]:
        params = {"address": token.address or token.identifier}
        try:
            if self._pool.endpoints:
                data = await self._request("/quote", params=params)
                price = data.get("price") or data.get("data", {}).get("price")
                volume = data.get("volume") or data.get("data", {}).get("volume")
                if price is not None:
                    return Quote(
                        symbol=token.symbol,
                        price=float(price),
                        volume=float(volume) if volume is not None else None,
                        ts=asyncio.get_event_loop().time(),
                    )
        except Exception as exc:
            for endpoint in self._pool.endpoints:
                self._emit_fault(endpoint, "api", str(exc))
        return Quote(symbol=token.symbol, price=self._synthetic_price(token), ts=asyncio.get_event_loop().time())

    def current_quote(self, token: TokenDescriptor) -> Optional[Quote]:
        return asyncio.get_event_loop().run_until_complete(self.current_quote_async(token))


__all__ = ["OnChainProvider"]
