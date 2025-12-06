import asyncio
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Optional

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors import register_adapter, sync_registered_tokens
from storage import sqlite_manager
from storage.migrate import initialize_database


class _StubAdapter:
    name = "stubdex"
    rate_limit_seconds = 0.0

    async def fetch_1m_bar(
        self,
        chain: str,
        token_address: str,
        pool_address: Optional[str],
        since_ts: Optional[int],
    ) -> Iterable[Dict[str, object]]:
        assert chain == "BNB"
        assert token_address == "0x123"
        return [
            {
                "open_ts": since_ts or 0,
                "close_ts": (since_ts or 0) + 60,
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume_base": 100.0,
                "volume_quote": 105.0,
                "notional_usd": 105.0,
                "trades": 12,
            }
        ]


@pytest.fixture()
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "dex.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    register_adapter("stubdex", _StubAdapter())
    return db_path


def test_sync_registered_tokens(temp_db: Path) -> None:
    now = int(time.time())
    sqlite_manager.upsert_token(
        {
            "id": "token-stub",
            "source": "dex",
            "exchange": "stubdex",
            "chain": "BNB",
            "symbol": "STUBUSDT",
            "base": "STUB",
            "quote": "USDT",
            "token_address": "0x123",
            "pool_address": "0xabc",
            "decimals": 18,
            "enabled": 1,
            "extra_json": "{}",
            "created_at": now,
        }
    )

    inserted = asyncio.run(sync_registered_tokens(since_ts=now - 60))
    assert inserted == 1

    bars = sqlite_manager.fetch_bars("bars_1m", "STUBUSDT")
    assert len(bars) == 1
    assert bars[0]["source"] == "dex"
    assert bars[0]["exchange"] == "stubdex"
