import asyncio
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.binance_api import BinanceClients, BinanceStream
from storage.migrate import initialize_database
from storage.sqlite_manager import fetch_bars


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:  # pragma: no cover - nothing to raise
        return None

    def json(self):
        return self._payload


class _FakeHttpClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def get(self, url: str, params):
        self.calls.append(params)
        return _FakeResponse(self.payload)

    async def aclose(self) -> None:  # pragma: no cover - noop
        return None


class _FakeWebsocket:
    def __init__(self, messages):
        self._messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class _FakeConnect:
    def __init__(self, ws):
        self._ws = ws

    def __await__(self):
        async def _coro():
            return self

        return _coro().__await__()

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture()
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "binance.db"
    monkeypatch.setenv("ALERT_DB_PATH", str(db_path))
    initialize_database(str(db_path))
    return db_path


def test_binance_stream_gap_fill(temp_db):
    # first websocket message -> close at 60s
    msg1 = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "k": {
                    "s": "BTCUSDT",
                    "t": 0,
                    "T": 60_000,
                    "o": "100",
                    "h": "110",
                    "l": "90",
                    "c": "105",
                    "v": "5",
                    "q": "525",
                    "n": 10,
                    "x": True,
                }
            },
        }
    )

    # second websocket message skips to close 180s (two missing minutes)
    msg2 = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "k": {
                    "s": "BTCUSDT",
                    "t": 120_000,
                    "T": 180_000,
                    "o": "105",
                    "h": "120",
                    "l": "100",
                    "c": "115",
                    "v": "8",
                    "q": "920",
                    "n": 12,
                    "x": True,
                }
            },
        }
    )

    # REST payload provides the missing candle at 120s
    rest_payload = [
        [
            60_000,
            "105",
            "112",
            "95",
            "107",
            "6",
            120_000,
            "642",
            11,
            "0",
            "0",
            "0",
        ]
    ]

    http_client = _FakeHttpClient(rest_payload)

    def ws_connect(url):
        return _FakeConnect(_FakeWebsocket([msg1, msg2]))

    clients = BinanceClients(http_factory=lambda: http_client, ws_connect=ws_connect)
    stream = BinanceStream(["BTCUSDT"], clients=clients)

    asyncio.run(stream._listen_once())

    rows = fetch_bars("bars_1m", "BTCUSDT")
    assert [row["close_ts"] for row in rows] == [60, 120, 180]
    assert http_client.calls[0]["startTime"] == 60 * 1000
    asyncio.run(stream.close())
