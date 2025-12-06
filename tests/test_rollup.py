import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aggregator.rollup import rollup_bars
from storage.migrate import initialize_database
from storage.sqlite_manager import fetch_bars, upsert_bar


@pytest.fixture()
def setup_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "rollup.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def _make_bar(symbol: str, idx: int, price: float) -> dict:
    minute = idx * 60
    return {
        "source": "cex",
        "exchange": "binance",
        "chain": "",
        "symbol": symbol,
        "base": symbol[:-4],
        "quote": symbol[-4:],
        "open_ts": minute,
        "close_ts": minute + 60,
        "open": price,
        "high": price + 1,
        "low": price - 1,
        "close": price + 0.5,
        "volume_base": 10 + idx,
        "volume_quote": 100 + idx,
        "notional_usd": 100 + idx,
        "trades": 5 + idx,
    }


def test_rollup_basic(setup_db, caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO)
    for i in range(10):
        upsert_bar("bars_1m", _make_bar("BTCUSDT", i, 100 + i))

    stats = rollup_bars(window=5)
    assert stats.aggregated == 2

    bars_5m = fetch_bars("bars_5m", "BTCUSDT")
    assert len(bars_5m) == 2
    assert bars_5m[0]["close_ts"] == 5 * 60
    assert bars_5m[0]["volume_base"] > 0


def test_rollup_gap_logging(setup_db, caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING)
    upsert_bar("bars_1m", _make_bar("ETHUSDT", 0, 100))
    # introduce a gap (skip idx=1)
    upsert_bar("bars_1m", _make_bar("ETHUSDT", 2, 102))

    stats = rollup_bars(window=5)
    assert stats.aggregated == 1
    assert any("Gap detected" in record.message for record in caplog.records)
