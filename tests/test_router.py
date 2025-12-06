import asyncio
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alerts import router
from rules.config_loader import (
    AppConfig,
    NotifiersConfig,
    DingtalkNotifierConfig,
    LocalSoundNotifierConfig,
    UIConfig,
    TrendChannelConfig,
    VolumeSpikeConfig,
)
from storage import sqlite_manager
from storage.migrate import initialize_database


@pytest.fixture()
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "router.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def _config() -> AppConfig:
    return AppConfig(
        symbols=["BTCUSDT"],
        timeframes=["5m"],
        volume_spike=VolumeSpikeConfig(),
        trend_channel=TrendChannelConfig(),
        price_alerts={},
        notifiers=NotifiersConfig(
            dingtalk=DingtalkNotifierConfig(enabled=False),
            local_sound=LocalSoundNotifierConfig(enabled=False),
        ),
        cooldown_minutes=10,
        notification_rate_limit_minutes=1,
        ui=UIConfig(),
    )


def test_router_rate_limit(monkeypatch: pytest.MonkeyPatch, temp_db: Path) -> None:
    now = int(time.time())
    event = {
        "id": "evt-1",
        "ts": now,
        "symbol": "BTCUSDT",
        "source": "cex",
        "exchange": "binance",
        "timeframe": "5m",
        "rule": "price_alert",
        "severity": "warning",
        "message": "hit",
        "detail_json": "{}",
        "created_at": now,
        "delivered": 0,
    }
    sqlite_manager.insert_event(event)
    sqlite_manager.insert_event({**event, "id": "evt-2", "created_at": now + 10})

    delivered = asyncio.run(router.dispatch_new_events(config=_config()))
    assert delivered == 1

    undelivered = sqlite_manager.fetch_undelivered_events()
    assert len(undelivered) == 0

    # Follow-up event should be rate limited
    sqlite_manager.insert_event({**event, "id": "evt-3", "created_at": now + 20})
    delivered_second = asyncio.run(router.dispatch_new_events(config=_config()))
    assert delivered_second == 0
