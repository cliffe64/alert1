"""Volume spike rule engine."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from statistics import mean
from typing import Dict, List, Optional

from indicators import zscore_volume
from rules.config_loader import AppConfig, VolumeSpikeMode, load_config
from storage import sqlite_manager

LOGGER = logging.getLogger(__name__)


def _now_ts() -> int:
    return int(time.time())


def _get_table(timeframe: str) -> str:
    if timeframe == "5m":
        return "bars_5m"
    if timeframe == "15m":
        return "bars_15m"
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _cooldown_key(symbol: str, timeframe: str) -> str:
    return f"volume_spike:{symbol}:{timeframe}"


def _passes_cooldown(symbol: str, timeframe: str, cooldown_minutes: int, now_ts: int) -> bool:
    key = _cooldown_key(symbol, timeframe)
    state = sqlite_manager.get_cooldown_state(key)
    if not state:
        return True
    last_fire = int(state.get("last_fire_ts", 0))
    if now_ts - last_fire < cooldown_minutes * 60:
        LOGGER.debug("Cooldown active for %s/%s", symbol, timeframe)
        return False
    return True


def _update_cooldown(symbol: str, timeframe: str, now_ts: int) -> None:
    key = _cooldown_key(symbol, timeframe)
    sqlite_manager.upsert_cooldown_state(key, symbol, "volume_spike", timeframe, now_ts)


def _insert_event(payload: Dict[str, object]) -> None:
    sqlite_manager.insert_event(payload)


def _build_event_id(symbol: str, timeframe: str, close_ts: int) -> str:
    return f"VOL-{symbol}-{timeframe}-{close_ts}"


def _baseline_notional(bars: List[Dict[str, object]]) -> List[float]:
    return [float(bar.get("notional_usd") or 0.0) for bar in bars]


def _handle_zscore(
    config: AppConfig,
    timeframe: str,
    symbol: str,
    bars: List[Dict[str, object]],
    now_ts: int,
) -> Optional[Dict[str, object]]:
    lookback = config.volume_spike.zscore.lookback_windows
    if len(bars) < lookback + 1:
        return None
    baseline = bars[-lookback - 1 : -1]
    current = bars[-1]
    current_notional = float(current.get("notional_usd") or 0.0)
    z = zscore_volume(current_notional, _baseline_notional(baseline))
    if z is None or z < config.volume_spike.zscore.z_thr:
        return None
    prev_close = float(baseline[-1]["close"])
    ret = 0.0
    if prev_close:
        ret = float(current["close"]) / prev_close - 1.0
    if abs(ret) < config.volume_spike.zscore.min_abs_return:
        return None
    if current_notional < config.volume_spike.zscore.min_notional_usd:
        return None
    detail = {
        "mode": "zscore",
        "z_vol": z,
        "ret": ret,
        "notional": current_notional,
        "baseline_mean": mean(_baseline_notional(baseline)) if baseline else 0.0,
    }
    return _create_event(symbol, timeframe, current, detail, now_ts)


def _find_bucket(config: AppConfig, symbol: str):
    for bucket in config.volume_spike.multiplier.buckets.values():
        if symbol in bucket.symbols:
            return bucket
    return None


def _handle_multiplier(
    config: AppConfig,
    timeframe: str,
    symbol: str,
    bars: List[Dict[str, object]],
    now_ts: int,
) -> Optional[Dict[str, object]]:
    lookback = config.volume_spike.zscore.lookback_windows
    if len(bars) < lookback + 1:
        return None
    baseline = bars[-lookback - 1 : -1]
    current = bars[-1]
    current_notional = float(current.get("notional_usd") or 0.0)
    prev_close = float(baseline[-1]["close"])
    ret = 0.0
    if prev_close:
        ret = float(current["close"]) / prev_close - 1.0
    if abs(ret) < config.volume_spike.multiplier.min_abs_return:
        return None
    bucket = _find_bucket(config, symbol)
    if not bucket:
        return None
    baseline_notional = mean(_baseline_notional(baseline))
    if baseline_notional == 0:
        return None
    ratio = current_notional / baseline_notional
    if ratio < bucket.mult or current_notional < bucket.min_notional_usd:
        return None
    detail = {
        "mode": "multiplier",
        "ratio": ratio,
        "ret": ret,
        "notional": current_notional,
        "baseline_mean": baseline_notional,
    }
    return _create_event(symbol, timeframe, current, detail, now_ts)


def _create_event(symbol: str, timeframe: str, bar: Dict[str, object], detail: Dict[str, object], now_ts: int):
    close_ts = int(bar["close_ts"])
    event = {
        "id": _build_event_id(symbol, timeframe, close_ts),
        "ts": close_ts,
        "symbol": symbol,
        "source": bar.get("source", "cex"),
        "exchange": bar.get("exchange", "binance"),
        "timeframe": timeframe,
        "rule": "volume_spike",
        "severity": "warning",
        "message": "Volume spike detected",
        "detail_json": json.dumps(detail),
        "created_at": now_ts,
        "delivered": 0,
    }
    return event


def run_volume_spike(
    timeframe: str,
    config: Optional[AppConfig] = None,
    now_ts: Optional[int] = None,
) -> List[Dict[str, object]]:
    config = config or load_config()
    if timeframe not in {"5m", "15m"}:
        raise ValueError("timeframe must be 5m or 15m")
    table = _get_table(timeframe)
    now = now_ts or _now_ts()
    events: List[Dict[str, object]] = []
    for symbol in config.symbols:
        bars = sqlite_manager.fetch_recent_bars(table, symbol, config.volume_spike.zscore.lookback_windows + 1)
        if len(bars) < config.volume_spike.zscore.lookback_windows + 1:
            continue
        if not _passes_cooldown(symbol, timeframe, config.cooldown_minutes, now):
            continue
        if config.volume_spike.mode is VolumeSpikeMode.ZSCORE:
            event = _handle_zscore(config, timeframe, symbol, bars, now)
        else:
            event = _handle_multiplier(config, timeframe, symbol, bars, now)
        if event:
            _insert_event(event)
            _update_cooldown(symbol, timeframe, now)
            events.append(event)
            LOGGER.info("Volume spike event inserted for %s/%s", symbol, timeframe)
    return events


__all__ = ["run_volume_spike"]
