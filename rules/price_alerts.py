"""Price alert scanning and event generation."""

from __future__ import annotations

import json
import logging
import math
import time
from typing import Dict, List, Optional

from indicators import atr as atr_indicator
from indicators import ema
from storage import sqlite_manager

LOGGER = logging.getLogger(__name__)


def _now_ts() -> int:
    return int(time.time())


def _load_state(rule_id: str) -> Dict[str, object]:
    kv = sqlite_manager.get_kv(f"price_state:{rule_id}")
    if kv and kv.get("value"):
        try:
            return json.loads(kv["value"])
        except json.JSONDecodeError:  # pragma: no cover - defensive
            LOGGER.warning("Invalid state JSON for %s", rule_id)
    return {"armed": True, "samples": []}


def _save_state(rule_id: str, state: Dict[str, object], now_ts: int) -> None:
    sqlite_manager.set_kv(f"price_state:{rule_id}", json.dumps(state), now_ts)


def _latest_price(symbol: str) -> Optional[float]:
    bar = sqlite_manager.fetch_latest_bar("bars_1m", symbol)
    return float(bar["close"]) if bar else None


def _evaluate_condition(rule: Dict[str, object], price: float, state: Dict[str, object]) -> bool:
    rule_type = rule["type"]
    level = rule.get("level")
    pct = rule.get("pct")
    baseline = state.get("baseline", price)
    if rule_type == "above":
        return price >= float(level)
    if rule_type == "below":
        return price <= float(level)
    if rule_type == "pct_up":
        return price >= baseline * (1 + float(pct))
    if rule_type == "pct_down":
        return price <= baseline * (1 - float(pct))
    if rule_type == "atr_breakout":
        return _atr_breakout(rule, price)
    raise ValueError(f"Unsupported rule type: {rule_type}")


def _atr_breakout(rule: Dict[str, object], price: float) -> bool:
    symbol = rule["symbol"]
    bars = sqlite_manager.fetch_recent_bars("bars_1m", symbol, limit=60)
    if len(bars) < 20:
        return False
    closes = [bar["close"] for bar in bars]
    highs = [bar["high"] for bar in bars]
    lows = [bar["low"] for bar in bars]
    ema_values = ema(closes, span=20)
    atr_values = atr_indicator(highs, lows, closes, period=14)
    ema_last = ema_values[-1]
    atr_last = atr_values[-1]
    if ema_last is None or atr_last is None:
        return False
    k = float(rule.get("atr_k") or 1.0)
    direction = rule.get("direction") or "above"
    if direction == "above":
        return price >= ema_last + k * atr_last
    if direction == "below":
        return price <= ema_last - k * atr_last
    return abs(price - ema_last) >= k * atr_last


def _apply_hysteresis(rule: Dict[str, object], price: float, state: Dict[str, object]) -> bool:
    was_armed = state.get("armed", True)
    if was_armed:
        return False
    level = float(rule.get("level") or price)
    hysteresis = rule.get("hysteresis")
    hysteresis_pct = rule.get("hysteresis_pct")
    rule_type = rule["type"]
    threshold = level
    if hysteresis_pct:
        if rule_type == "above":
            threshold = level * (1 - float(hysteresis_pct))
        elif rule_type == "below":
            threshold = level * (1 + float(hysteresis_pct))
    elif hysteresis:
        if rule_type == "above":
            threshold = level - float(hysteresis)
        elif rule_type == "below":
            threshold = level + float(hysteresis)

    if rule_type == "above" and price <= threshold:
        state["armed"] = True
    elif rule_type == "below" and price >= threshold:
        state["armed"] = True
    elif rule_type.startswith("pct"):
        baseline = state.get("baseline", level)
        state["armed"] = price <= baseline if "up" in rule_type else price >= baseline

    return (not was_armed) and state.get("armed", True)


def _confirm(rule: Dict[str, object], condition: bool, state: Dict[str, object], now_ts: int) -> bool:
    mode = rule.get("confirm_mode")
    if not mode:
        return condition
    if mode == "time":
        if condition:
            since = state.get("condition_since") or now_ts
            state["condition_since"] = since
            return now_ts - since >= int(rule.get("confirm_seconds", 0))
        state.pop("condition_since", None)
        return False
    if mode == "samples":
        samples: List[bool] = list(state.get("samples", []))
        samples.append(bool(condition))
        total = int(rule.get("confirm_samples_total", 0))
        if len(samples) > total:
            samples = samples[-total:]
        state["samples"] = samples
        if len(samples) < total:
            return False
        passes = sum(1 for sample in samples if sample)
        return passes >= int(rule.get("confirm_samples_pass", total))
    if mode == "bar_close":
        tf = rule.get("confirm_timeframe") or "5m"
        table = "bars_5m" if tf == "5m" else "bars_15m"
        bar = sqlite_manager.fetch_latest_bar(table, rule["symbol"])
        if not bar:
            return False
        return _evaluate_condition(rule, float(bar["close"]), state)
    return condition


def _build_event(rule: Dict[str, object], price: float, now_ts: int) -> Dict[str, object]:
    return {
        "id": f"PRICE-{rule['id']}-{now_ts}",
        "ts": now_ts,
        "symbol": rule["symbol"],
        "source": "cex",
        "exchange": rule.get("exchange", "binance"),
        "timeframe": "1m",
        "rule": f"price_{rule['type']}",
        "severity": "info",
        "message": rule.get("message", ""),
        "detail_json": json.dumps({"price": price, "rule": rule["type"]}),
        "created_at": now_ts,
        "delivered": 0,
    }


def scan_price_alerts(now_ts: Optional[int] = None) -> List[Dict[str, object]]:
    now = now_ts or _now_ts()
    rules = sqlite_manager.list_rules(enabled=True)
    events: List[Dict[str, object]] = []
    for rule in rules:
        price = _latest_price(rule["symbol"])
        if price is None:
            continue
        state = _load_state(rule["id"])
        rule_type = rule["type"]
        baseline_initialized = "baseline" in state
        rearmed = _apply_hysteresis(rule, price, state)
        if rule_type in {"pct_up", "pct_down"}:
            if rearmed or not baseline_initialized:
                state["baseline"] = price
                state["baseline_ts"] = now
                _save_state(rule["id"], state, now)
            if not baseline_initialized:
                # Establish a baseline before the first evaluation so future
                # samples compare against a fixed reference.
                continue
        if not state.get("armed", True):
            _save_state(rule["id"], state, now)
            continue
        condition = _evaluate_condition(rule, price, state)
        if not _confirm(rule, condition, state, now):
            _save_state(rule["id"], state, now)
            continue
        event = _build_event(rule, price, now)
        sqlite_manager.insert_event(event)
        events.append(event)
        state["armed"] = False
        state["baseline"] = price
        state["last_trigger_ts"] = now
        state.pop("condition_since", None)
        state["samples"] = []
        _save_state(rule["id"], state, now)
        LOGGER.info("Price alert triggered for %s (%s)", rule["symbol"], rule["type"])
    return events


__all__ = ["scan_price_alerts"]
