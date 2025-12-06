"""Trend channel continuation/breakout detection."""

from __future__ import annotations

import json
import logging
import time
from statistics import mean
from typing import Dict, List, Optional

from indicators import atr as atr_indicator
from indicators import linreg_features
from indicators import zscore_volume
from rules.config_loader import AppConfig, load_config
from storage import sqlite_manager

LOGGER = logging.getLogger(__name__)


def _now_ts() -> int:
    return int(time.time())


def _table(timeframe: str) -> str:
    if timeframe == "5m":
        return "bars_5m"
    if timeframe == "15m":
        return "bars_15m"
    raise ValueError("Unsupported timeframe")


def _cooldown_key(symbol: str, timeframe: str, label: str) -> str:
    return f"trend:{symbol}:{timeframe}:{label}"


def _passes_cooldown(symbol: str, timeframe: str, label: str, cooldown: int, now_ts: int) -> bool:
    state = sqlite_manager.get_cooldown_state(_cooldown_key(symbol, timeframe, label))
    if not state:
        return True
    return now_ts - int(state.get("last_fire_ts", 0)) >= cooldown * 60


def _update_cooldown(symbol: str, timeframe: str, label: str, now_ts: int) -> None:
    key = _cooldown_key(symbol, timeframe, label)
    sqlite_manager.upsert_cooldown_state(key, symbol, label, timeframe, now_ts)


def _build_event(symbol: str, timeframe: str, bar: Dict[str, object], label: str, detail: Dict[str, object], now_ts: int) -> Dict[str, object]:
    return {
        "id": f"TC-{label}-{symbol}-{bar['close_ts']}",
        "ts": int(bar["close_ts"]),
        "symbol": symbol,
        "source": bar.get("source", "cex"),
        "exchange": bar.get("exchange", "binance"),
        "timeframe": timeframe,
        "rule": f"trend_{label.lower()}",
        "severity": "info" if label == "SUSTAIN" else "warning",
        "message": f"Trend channel {label.lower()}",
        "detail_json": json.dumps(detail),
        "created_at": now_ts,
        "delivered": 0,
    }


def _atr_value(bars: List[Dict[str, object]]) -> Optional[float]:
    highs = [bar["high"] for bar in bars]
    lows = [bar["low"] for bar in bars]
    closes = [bar["close"] for bar in bars]
    atr_values = atr_indicator(highs, lows, closes, period=min(14, len(bars)))
    return atr_values[-1]


def _zscore_notional(bars: List[Dict[str, object]]) -> Optional[float]:
    baseline = [bar.get("notional_usd") or 0.0 for bar in bars[:-1]]
    current = bars[-1].get("notional_usd") or 0.0
    return zscore_volume(current, baseline)


def scan_trend_channel(
    timeframe: str,
    config: Optional[AppConfig] = None,
    now_ts: Optional[int] = None,
) -> List[Dict[str, object]]:
    config = config or load_config()
    table = _table(timeframe)
    now = now_ts or _now_ts()
    window = config.trend_channel.window
    events: List[Dict[str, object]] = []
    for symbol in config.symbols:
        bars = sqlite_manager.fetch_recent_bars(table, symbol, window + 1)
        if len(bars) < window:
            continue
        if len(bars) == window:
            history = bars
            current_bar = bars[-1]
            future_pred = None
        else:
            history = bars[:-1]
            current_bar = bars[-1]
            future_pred = True
        slope, r2, resid_std, mid_price = linreg_features([bar["close"] for bar in history], window)
        if None in (slope, r2, resid_std, mid_price):
            continue
        atr_last = _atr_value(history)
        if atr_last is None:
            continue
        slope_norm = abs(float(slope) / float(history[-1]["close"]))
        if r2 < config.trend_channel.r2_min:
            continue
        if slope_norm < config.trend_channel.slope_norm_min or slope_norm > config.trend_channel.slope_norm_max:
            continue
        if float(resid_std) > atr_last * config.trend_channel.resid_atr_max:
            continue
        predicted_next = float(mid_price) + float(slope) if future_pred else float(mid_price)
        deviation = float(current_bar["close"]) - predicted_next
        label: Optional[str] = None
        detail = {
            "slope": slope,
            "r2": r2,
            "resid_std": resid_std,
            "mid_price": mid_price,
            "atr": atr_last,
            "deviation": deviation,
        }

        if abs(deviation) <= atr_last * config.trend_channel.pullback_atr_max:
            label = "SUSTAIN"
        else:
            z_notional = _zscore_notional(history + [current_bar])
            if z_notional is None:
                baseline = [bar.get("notional_usd") or 0.0 for bar in history]
                baseline_mean = mean(baseline) if baseline else 0.0
                current_notional = current_bar.get("notional_usd") or 0.0
                if baseline_mean and current_notional >= baseline_mean * config.trend_channel.vol_confirm_z:
                    z_notional = float(config.trend_channel.vol_confirm_z)
                else:
                    continue
            if z_notional < config.trend_channel.vol_confirm_z:
                continue
            if deviation >= config.trend_channel.breakout_atr_mult * atr_last:
                label = "BREAKOUT"
                detail["z_vol"] = z_notional
                detail["direction"] = "up"
            elif deviation <= -config.trend_channel.breakout_atr_mult * atr_last:
                label = "BREAKOUT"
                detail["z_vol"] = z_notional
                detail["direction"] = "down"

        if not label:
            continue
        if not _passes_cooldown(symbol, timeframe, label, config.cooldown_minutes, now):
            continue
        event = _build_event(symbol, timeframe, current_bar, label, detail, now)
        sqlite_manager.insert_event(event)
        _update_cooldown(symbol, timeframe, label, now)
        events.append(event)
        LOGGER.info("Trend channel %s for %s/%s", label, symbol, timeframe)
    return events


__all__ = ["scan_trend_channel"]
