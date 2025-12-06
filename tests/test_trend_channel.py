import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rules.config_loader import AppConfig
from rules.trend_channel import scan_trend_channel
from storage.migrate import initialize_database
from storage.sqlite_manager import fetch_undelivered_events, upsert_bar


@pytest.fixture()
def setup_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "trend.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def _bar(close_ts: int, close: float, notional: float) -> dict:
    return {
        "source": "cex",
        "exchange": "binance",
        "chain": "",
        "symbol": "ETHUSDT",
        "base": "ETH",
        "quote": "USDT",
        "open_ts": close_ts - 300,
        "close_ts": close_ts,
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume_base": notional / close,
        "volume_quote": notional,
        "notional_usd": notional,
        "trades": 5,
    }


def _config() -> AppConfig:
    data = {
        "symbols": ["ETHUSDT"],
        "timeframes": ["5m"],
        "cooldown_minutes": 5,
        "volume_spike": {"mode": "zscore"},
        "trend_channel": {
            "window": 10,
            "r2_min": 0.8,
            "slope_norm_min": 0.0001,
            "slope_norm_max": 0.01,
            "resid_atr_max": 1.5,
            "pullback_atr_max": 0.6,
            "breakout_atr_mult": 1.0,
            "vol_confirm_z": 1.0,
        },
    }
    return AppConfig.from_dict(data)


def test_trend_channel_sustain_and_breakout(setup_db):
    cfg = _config()
    base_ts = 1_000
    for i in range(cfg.trend_channel.window):
        upsert_bar("bars_5m", _bar(base_ts + i * 300, 100 + i, 100))

    events = scan_trend_channel("5m", config=cfg, now_ts=base_ts + cfg.trend_channel.window * 300)
    assert events and events[0]["rule"] == "trend_sustain"

    # Add breakout bar with higher close and large notional
    upsert_bar(
        "bars_5m",
        _bar(base_ts + (cfg.trend_channel.window + 1) * 300, 150, 1_000),
    )
    events = scan_trend_channel("5m", config=cfg, now_ts=base_ts + (cfg.trend_channel.window + 1) * 300)
    assert any(event["rule"] == "trend_breakout" for event in events)
    stored = fetch_undelivered_events()
    assert stored
