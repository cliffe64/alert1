import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rules.config_loader import AppConfig
from rules.volume_spike import run_volume_spike
from storage.migrate import initialize_database
from storage.sqlite_manager import fetch_undelivered_events, upsert_bar


@pytest.fixture()
def setup_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "volume.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def _config() -> AppConfig:
    data = {
        "symbols": ["BTCUSDT"],
        "timeframes": ["5m"],
        "cooldown_minutes": 10,
        "volume_spike": {
            "mode": "zscore",
            "zscore": {
                "lookback_windows": 5,
                "z_thr": 1.0,
                "min_notional_usd": 100,
                "min_abs_return": 0.001,
            },
        },
        "trend_channel": {"window": 20, "r2_min": 0.6},
    }
    return AppConfig.from_dict(data)


def _bar(close_ts: int, close: float, notional: float) -> dict:
    return {
        "source": "cex",
        "exchange": "binance",
        "chain": "",
        "symbol": "BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "open_ts": close_ts - 300,
        "close_ts": close_ts,
        "open": close,
        "high": close + 10,
        "low": close - 10,
        "close": close,
        "volume_base": notional / close,
        "volume_quote": notional,
        "notional_usd": notional,
        "trades": 10,
    }


def test_volume_spike_triggers_and_cooldown(setup_db):
    cfg = _config()
    base_ts = 1_000
    for i in range(5):
        upsert_bar("bars_5m", _bar(base_ts + i * 300, 100 + i, 100 + i * 10))
    upsert_bar("bars_5m", _bar(base_ts + 5 * 300, 120, 500))

    events = run_volume_spike("5m", config=cfg, now_ts=base_ts + 5 * 300 + 1)
    assert len(events) == 1

    stored = fetch_undelivered_events()
    assert stored and json.loads(stored[0]["detail_json"])["z_vol"] > 0

    # second run within cooldown should not create new event
    events_again = run_volume_spike("5m", config=cfg, now_ts=base_ts + 5 * 300 + 30)
    assert not events_again
