import asyncio
import sys
import time
from pathlib import Path
from typing import Optional

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.local_notifier import LocalNotifier, LocalNotifierSettings
from alerts import local_sound
from rules.config_loader import LocalSoundNotifierConfig
from storage import sqlite_manager
from storage.migrate import initialize_database


@pytest.fixture()
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "notifier.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def test_local_notifier_processes_events(monkeypatch: pytest.MonkeyPatch, temp_db: Path) -> None:
    now = int(time.time())
    event = {
        "id": "evt-test",
        "ts": now,
        "symbol": "BTCUSDT",
        "source": "dex",
        "exchange": "pancake",
        "timeframe": "5m",
        "rule": "test",
        "severity": "warning",
        "message": "hello",
        "detail_json": "{}",
        "created_at": now,
        "delivered": 0,
    }
    sqlite_manager.insert_event(event)

    played: list[tuple[Optional[str], float]] = []

    def _mock_play(path: Optional[str], volume: float) -> None:
        played.append((path, volume))

    monkeypatch.setattr(local_sound, "play", _mock_play)

    sound_cfg = LocalSoundNotifierConfig(enabled=True, sound_file=None, volume=0.5)
    settings = LocalNotifierSettings(client_id="test", poll_interval=0.1, min_severity="info")
    notifier = LocalNotifier(sound_cfg, settings, now_func=lambda: now + 1)

    processed = asyncio.run(notifier.poll_once())
    assert processed == 1
    assert played == [(None, 0.5)]

    state = sqlite_manager.get_local_notifier_state("test")
    assert state is not None
    assert state["last_event_id"] == "evt-test"


def test_local_notifier_respects_severity(monkeypatch: pytest.MonkeyPatch, temp_db: Path) -> None:
    now = int(time.time())
    sqlite_manager.insert_event(
        {
            "id": "evt-low",
            "ts": now,
            "symbol": "BTCUSDT",
            "source": "dex",
            "exchange": "pancake",
            "timeframe": "5m",
            "rule": "test",
            "severity": "info",
            "message": "low",
            "detail_json": "{}",
            "created_at": now,
            "delivered": 0,
        }
    )

    sound_cfg = LocalSoundNotifierConfig(enabled=True, sound_file=None, volume=1.0)
    settings = LocalNotifierSettings(client_id="sev", poll_interval=0.1, min_severity="warning")
    notifier = LocalNotifier(sound_cfg, settings, now_func=lambda: now)

    processed = asyncio.run(notifier.poll_once())
    assert processed == 0
    state = sqlite_manager.get_local_notifier_state("sev")
    assert state is None
