import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rules.price_alerts import scan_price_alerts
from storage.migrate import initialize_database
from storage.sqlite_manager import fetch_undelivered_events, upsert_bar, upsert_rule


@pytest.fixture()
def setup_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "price.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def _bar(close_ts: int, close: float) -> dict:
    return {
        "source": "cex",
        "exchange": "binance",
        "chain": "",
        "symbol": "BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "open_ts": close_ts - 60,
        "close_ts": close_ts,
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume_base": 1,
        "volume_quote": close,
        "notional_usd": close,
        "trades": 1,
    }


def _rule(rule_id: str, level: float, hysteresis: float) -> dict:
    return {
        "id": rule_id,
        "symbol": "BTCUSDT",
        "type": "above",
        "level": level,
        "pct": None,
        "atr_k": None,
        "direction": "above",
        "hysteresis": hysteresis,
        "hysteresis_pct": None,
        "confirm_mode": "time",
        "confirm_seconds": 5,
        "confirm_samples_total": None,
        "confirm_samples_pass": None,
        "confirm_timeframe": None,
        "message": "price breakout",
        "enabled": 1,
        "created_at": 0,
    }


def test_price_alert_time_confirm_and_hysteresis(setup_db):
    for idx in range(10):
        upsert_bar("bars_1m", _bar(100 + idx * 60, 100 + idx))
    upsert_rule(_rule("rule1", level=105, hysteresis=2))

    # first evaluation should only start the timer
    scan_price_alerts(now_ts=200)
    events = fetch_undelivered_events()
    assert not events

    # After confirm window, alert fires
    scan_price_alerts(now_ts=207)
    events = fetch_undelivered_events()
    assert len(events) == 1

    # Price drops below hysteresis to rearm
    upsert_bar("bars_1m", _bar(800, 100))
    scan_price_alerts(now_ts=810)
    scan_price_alerts(now_ts=815)  # re-evaluate after price recovers
    upsert_bar("bars_1m", _bar(820, 110))
    scan_price_alerts(now_ts=826)
    scan_price_alerts(now_ts=833)
    events = fetch_undelivered_events()
    assert len(events) == 2
