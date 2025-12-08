import sys
from pathlib import Path

import asyncio

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.onchain_provider import OnChainProvider
from core.providers import TokenDescriptor
from run import _select_provider


def test_onchain_search_returns_static_match_when_no_endpoint():
    provider = OnChainProvider()
    results = asyncio.run(provider.search_tokens_async("pepe"))
    assert any(r.symbol == "PEPE" and r.chain == "ethereum" for r in results)


def test_onchain_resolve_address_fallback():
    provider = OnChainProvider()
    addr = "0xabc123"
    resolved = provider.resolve_token(addr)
    assert resolved is not None
    assert resolved.address == addr
    assert resolved.symbol.startswith("0x")


def test_onchain_quote_falls_back_to_synthetic_without_endpoint():
    provider = OnChainProvider()
    token = TokenDescriptor(
        identifier="0xabc123",
        name="Demo",
        symbol="DEMO",
        chain="demo",
        address="0xabc123",
    )
    quote = asyncio.run(provider.current_quote_async(token))
    assert quote.price > 0


def test_provider_selection_prefers_onchain_for_address():
    futures = object()
    onchain = object()
    token = TokenDescriptor(identifier="0xaddr", name="addr", symbol="ADDR", chain="eth", address="0xaddr")
    provider = _select_provider(token, futures, onchain)  # type: ignore[arg-type]
    assert provider is onchain


def test_provider_selection_defaults_to_futures():
    futures = object()
    onchain = object()
    token = TokenDescriptor(identifier="BTCUSDT", name="BTCUSDT", symbol="BTCUSDT")
    provider = _select_provider(token, futures, onchain)  # type: ignore[arg-type]
    assert provider is futures
