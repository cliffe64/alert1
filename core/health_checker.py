"""Endpoint health probe helpers for UI buttons and automated checks."""

from __future__ import annotations

import httpx

from core.health import Endpoint, EndpointPool, HealthCheckResult


async def probe_endpoints(pool: EndpointPool, path: str = "/fapi/v1/ping") -> list[HealthCheckResult]:
    """Ping all endpoints in the pool and return structured results.

    The function performs a lightweight GET request against the provided path
    to distinguish network errors from API failures. Results can be surfaced
    on the front-end log panel and also feed the endpoint auto-switch logic.
    """

    results: list[HealthCheckResult] = []
    for endpoint in pool.endpoints:
        url = f"{endpoint.base_url.rstrip('/')}{path}"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url)
                ok = resp.status_code == 200
                reason = "ok" if ok else f"status {resp.status_code}"
                latency_ms = resp.elapsed.total_seconds() * 1000 if resp.elapsed else None
                if ok:
                    pool.mark_success(endpoint, latency_ms or 0.0)
                else:
                    pool.mark_failure(endpoint, reason)
                results.append(
                    HealthCheckResult(endpoint=endpoint, ok=ok, reason=reason, latency_ms=latency_ms)
                )
        except httpx.RequestError as exc:
            pool.mark_failure(endpoint, str(exc))
            results.append(HealthCheckResult(endpoint=endpoint, ok=False, reason=str(exc), latency_ms=None))
    return results


__all__ = ["probe_endpoints"]
