from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storage import sqlite_manager
from storage.migrate import initialize_database


@pytest.fixture()
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def test_bar_upsert_and_fetch(temp_db: Path) -> None:
    bar = {
        "source": "binance",
        "exchange": "binance",
        "chain": "",
        "symbol": "BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "open_ts": 1000,
        "close_ts": 1060,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume_base": 10.0,
        "volume_quote": 150.0,
        "notional_usd": 150.0,
        "trades": 100,
    }
    sqlite_manager.upsert_bar("bars_1m", bar)

    updated_bar = dict(bar)
    updated_bar["high"] = 2.5
    updated_bar["close"] = 1.6
    sqlite_manager.upsert_bar("bars_1m", updated_bar)

    rows = sqlite_manager.fetch_bars("bars_1m", "BTCUSDT")
    assert len(rows) == 1
    assert rows[0]["high"] == 2.5
    assert rows[0]["close"] == 1.6


def test_event_insert_and_update(temp_db: Path) -> None:
    event = {
        "id": "evt1",
        "ts": 2000,
        "symbol": "BTCUSDT",
        "source": "binance",
        "exchange": "binance",
        "timeframe": "1m",
        "rule": "test",
        "severity": "info",
        "message": "hello",
        "detail_json": "{}",
        "created_at": 2000,
        "delivered": 0,
    }
    sqlite_manager.insert_event(event)

    event_update = dict(event)
    event_update["message"] = "updated"
    sqlite_manager.insert_event(event_update)

    with sqlite3.connect(sqlite_manager.get_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM events WHERE id = ?", ("evt1",)).fetchone()
        assert row["message"] == "updated"


def test_kv_state(temp_db: Path) -> None:
    sqlite_manager.set_kv("last_sync", "100", 123)
    result = sqlite_manager.get_kv("last_sync")
    assert result is not None
    assert result["value"] == "100"

    sqlite_manager.set_kv("last_sync", "200", 456)
    result = sqlite_manager.get_kv("last_sync")
    assert result is not None
    assert result["value"] == "200"
    assert result["updated_at"] == 456


def test_rules_and_tokens(temp_db: Path) -> None:
    now = int(time.time())
    sqlite_manager.upsert_rule(
        {
            "id": "rule1",
            "symbol": "BTCUSDT",
            "type": "above",
            "level": 100.0,
            "pct": None,
            "atr_k": None,
            "direction": "up",
            "hysteresis": 1.0,
            "hysteresis_pct": None,
            "confirm_mode": "time",
            "confirm_seconds": 10,
            "confirm_samples_total": None,
            "confirm_samples_pass": None,
            "confirm_timeframe": None,
            "message": "test",
            "enabled": 1,
            "created_at": now,
        }
    )
    sqlite_manager.upsert_rule(
        {
            "id": "rule2",
            "symbol": "ETHUSDT",
            "type": "above",
            "level": 200.0,
            "pct": None,
            "atr_k": None,
            "direction": "up",
            "hysteresis": None,
            "hysteresis_pct": 0.01,
            "confirm_mode": "samples",
            "confirm_seconds": None,
            "confirm_samples_total": 3,
            "confirm_samples_pass": 2,
            "confirm_timeframe": None,
            "message": "test2",
            "enabled": 0,
            "created_at": now,
        }
    )

    rules_all = sqlite_manager.list_rules()
    assert len(rules_all) == 2
    enabled_rules = sqlite_manager.list_rules(enabled=True)
    assert len(enabled_rules) == 1
    assert enabled_rules[0]["id"] == "rule1"

    sqlite_manager.upsert_token(
        {
            "id": "token1",
            "source": "dex",
            "exchange": "pancake",
            "chain": "BNB",
            "symbol": "TOKENUSDT",
            "base": "TOKEN",
            "quote": "USDT",
            "token_address": "0x123",
            "pool_address": "0x456",
            "decimals": 18,
            "enabled": 1,
            "extra_json": "{}",
            "created_at": now,
        }
    )
    sqlite_manager.upsert_token(
        {
            "id": "token2",
            "source": "dex",
            "exchange": "pancake",
            "chain": "BNB",
            "symbol": "TOKEN2USDT",
            "base": "TOKEN2",
            "quote": "USDT",
            "token_address": "0xabc",
            "pool_address": "0xdef",
            "decimals": 18,
            "enabled": 0,
            "extra_json": "{}",
            "created_at": now,
        }
    )

    all_tokens = sqlite_manager.list_tokens()
    assert len(all_tokens) == 2
    enabled_tokens = sqlite_manager.list_tokens(enabled=True)
    assert len(enabled_tokens) == 1
    assert enabled_tokens[0]["id"] == "token1"


def test_unique_constraint_violation(temp_db: Path) -> None:
    bar = {
        "source": "binance",
        "exchange": "binance",
        "chain": "",
        "symbol": "BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "open_ts": 1000,
        "close_ts": 1060,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume_base": 10.0,
        "volume_quote": 150.0,
        "notional_usd": 150.0,
        "trades": 100,
    }
    sqlite_manager.upsert_bar("bars_1m", bar)
    rows = sqlite_manager.fetch_bars("bars_1m", "BTCUSDT")
    assert len(rows) == 1

    with sqlite3.connect(sqlite_manager.get_db_path()) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO bars_1m (source, exchange, chain, symbol, base, quote, open_ts, close_ts, open, high, low, close, volume_base, volume_quote, notional_usd, trades)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "binance",
                    "binance",
                    "",
                    "BTCUSDT",
                    "BTC",
                    "USDT",
                    1000,
                    1060,
                    1.0,
                    2.0,
                    0.5,
                    1.5,
                    10.0,
                    150.0,
                    150.0,
                    100,
                ),
            )
            conn.commit()


def test_fetch_events_since_and_state(temp_db: Path) -> None:
    now = int(time.time())
    base_event = {
        "id": "evt-state-1",
        "ts": now,
        "symbol": "BTCUSDT",
        "source": "dex",
        "exchange": "pancake",
        "timeframe": "5m",
        "rule": "test",
        "severity": "info",
        "message": "hello",
        "detail_json": "{}",
        "created_at": now,
        "delivered": 0,
    }
    sqlite_manager.insert_event(base_event)
    second = dict(base_event)
    second.update(
        {
            "id": "evt-state-2",
            "severity": "warning",
            "created_at": now + 10,
        }
    )
    sqlite_manager.insert_event(second)

    events = sqlite_manager.fetch_events_since(None, min_severity="warning")
    assert len(events) == 1
    assert events[0]["id"] == "evt-state-2"

    sqlite_manager.update_local_notifier_state("client", second["id"], second["created_at"], now + 20)
    state = sqlite_manager.get_local_notifier_state("client")
    assert state is not None
    assert state["last_event_id"] == "evt-state-2"


def test_rate_limit_helpers(temp_db: Path) -> None:
    key = "BTCUSDT|rule|5m"
    now = int(time.time())
    assert not sqlite_manager.should_rate_limit(key, 60, now)
    sqlite_manager.update_rate_limit_timestamp(key, now)
    assert sqlite_manager.should_rate_limit(key, 60, now + 30)
    assert not sqlite_manager.should_rate_limit(key, 60, now + 61)
