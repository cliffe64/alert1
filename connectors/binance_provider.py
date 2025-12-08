"""Binance-based provider implementing the unified Provider interface."""

from __future__ import annotations

import asyncio
from typing import Iterable, List, Optional

import httpx

from core.events import EventEnvelope, EventType, HealthStatus, Severity, SystemFaultEvent
from core.health import Endpoint, EndpointPool
from core.providers import EndpointConfig, Provider, Quote, TokenDescriptor
from core.event_bus import EventBus


class BinanceFuturesProvider(Provider):
    """Data provider that pulls from Binance Futures endpoints.

    The provider accepts a pool of endpoints for failover and publishes health
    events when endpoints fail. It only depends on the common Provider
    interface so it can be swapped without touching the monitoring engine.
    """

    name = "binance_futures"

    def __init__(self, event_bus: EventBus | None = None) -> None:
        self._pool = EndpointPool([])
        self._event_bus = event_bus
        self._contracts: list[TokenDescriptor] = []

    def configure_endpoints(self, endpoints: Iterable[EndpointConfig]) -> None:
        self._pool = EndpointPool(
            Endpoint(name=ep.name, base_url=ep.base_url, api_key=ep.api_key, priority=ep.priority)
            for ep in endpoints
        )

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

    def _emit_health(self, endpoint: Endpoint) -> None:
        if not self._event_bus:
            return
        event = HealthStatus(
            event_type=EventType.HEALTH_UPDATE,
            severity=Severity.INFO if endpoint.healthy else Severity.WARNING,
            source=self.name,
            message="Endpoint health update",
            endpoint=endpoint.base_url,
            healthy=endpoint.healthy,
            latency_ms=endpoint.latency_ms,
            retries=endpoint.consecutive_failures,
        )
        self._event_bus.publish(EventEnvelope(event=event, ts=asyncio.get_event_loop().time()))

    async def list_futures_contracts_async(self) -> List[TokenDescriptor]:
        data = await self._request("/fapi/v1/exchangeInfo")
        contracts: list[TokenDescriptor] = []
        for symbol in data.get("symbols", []):
            if symbol.get("contractType"):
                contracts.append(
                    TokenDescriptor(
                        identifier=symbol["symbol"],
                        name=symbol.get("pair", symbol["symbol"]),
                        symbol=symbol["symbol"],
                        chain=None,
                        address=None,
                        extra={"status": symbol.get("status")},
                    )
                )
        self._contracts = contracts
        for ep in self._pool.endpoints:
            self._emit_health(ep)
        return contracts

    def list_futures_contracts(self) -> List[TokenDescriptor]:
        return asyncio.get_event_loop().run_until_complete(self.list_futures_contracts_async())

    def search_tokens(self, query: str) -> List[TokenDescriptor]:
        q = query.lower().strip()
        if not self._contracts:
            try:
                self._contracts = self.list_futures_contracts()
            except Exception:
                self._contracts = []
        matches = [c for c in self._contracts if q in c.symbol.lower() or q in c.name.lower()]
        return matches

    def resolve_token(self, address: str) -> Optional[TokenDescriptor]:
        address = address.strip()
        if not address:
            return None
        return TokenDescriptor(
            identifier=address,
            name=address,
            symbol=address[:6] + "...",
            chain=None,
            address=address,
            extra={"added_via": "address"},
        )

    async def current_quote_async(self, token: TokenDescriptor) -> Optional[Quote]:
        try:
            data = await self._request("/fapi/v1/ticker/price", params={"symbol": token.symbol})
            price = float(data.get("price"))
            return Quote(symbol=token.symbol, price=price, ts=asyncio.get_event_loop().time())
        except Exception as exc:
            for endpoint in self._pool.endpoints:
                self._emit_fault(endpoint, "api", str(exc))
            return None

    def current_quote(self, token: TokenDescriptor) -> Optional[Quote]:
        return asyncio.get_event_loop().run_until_complete(self.current_quote_async(token))


__all__ = ["BinanceFuturesProvider"]
