"""Utility functions for computing backtest performance metrics."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence

import matplotlib.pyplot as plt


def compute_forward_metrics(
    bars: Sequence[Dict[str, float]],
    event_ts: int,
    horizons: Sequence[int],
) -> Optional[Dict[str, float]]:
    """Compute forward returns and drawdown for a given event."""

    if not bars:
        return None
    horizons_set = set(horizons)
    metrics: Dict[str, float] = {"event_ts": float(event_ts)}
    base_price: Optional[float] = None
    base_ts: Optional[int] = None
    max_drawdown = 0.0
    peak_price: Optional[float] = None
    pending = {h: True for h in horizons}
    for bar in bars:
        close_ts = int(bar["close_ts"])
        close_price = float(bar["close"])
        if close_ts < event_ts:
            continue
        if base_price is None:
            base_price = close_price
            base_ts = close_ts
            peak_price = close_price
        if peak_price is not None:
            if close_price > peak_price:
                peak_price = close_price
            drawdown = (close_price - peak_price) / peak_price if peak_price else 0.0
            if drawdown < max_drawdown:
                max_drawdown = drawdown
        for horizon in list(pending):
            target_ts = event_ts + horizon * 60
            if close_ts >= target_ts and base_price:
                metrics[f"ret_{horizon}"] = (close_price - base_price) / base_price
                pending.pop(horizon, None)
        if not pending and close_ts - event_ts > max(horizons_set) * 60:
            break
    if base_price is None:
        return None
    metrics["base_price"] = base_price
    metrics["base_ts"] = float(base_ts or event_ts)
    metrics["max_drawdown"] = max_drawdown
    for horizon in horizons:
        metrics.setdefault(f"ret_{horizon}", float("nan"))
    return metrics


def aggregate_metrics(
    metrics: Iterable[Dict[str, float]],
    horizons: Sequence[int],
) -> Dict[str, float]:
    """Aggregate average metrics across events."""

    totals: Dict[str, float] = {f"ret_{h}": 0.0 for h in horizons}
    counts: Dict[str, int] = {f"ret_{h}": 0 for h in horizons}
    drawdown_total = 0.0
    drawdown_count = 0
    for metric in metrics:
        for horizon in horizons:
            key = f"ret_{horizon}"
            value = metric.get(key)
            if value is None or value != value:  # NaN check
                continue
            totals[key] += value
            counts[key] += 1
        dd = metric.get("max_drawdown")
        if dd is not None:
            drawdown_total += dd
            drawdown_count += 1
    summary = {
        f"avg_ret_{h}": (totals[f"ret_{h}"] / counts[f"ret_{h}"])
        if counts[f"ret_{h}"]
        else float("nan")
        for h in horizons
    }
    summary["avg_max_drawdown"] = drawdown_total / drawdown_count if drawdown_count else float("nan")
    summary["samples"] = drawdown_count
    return summary


def write_csv(path: Path, rows: Iterable[Dict[str, float]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def plot_distribution(returns: Iterable[float], output: Path) -> None:
    values = [value for value in returns if value == value]
    if not values:
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(6, 4))
    plt.hist(values, bins=20, alpha=0.7, color="#1f77b4")
    plt.title("Forward Return Distribution")
    plt.xlabel("Return")
    plt.ylabel("Frequency")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output)
    plt.close()


__all__ = [
    "compute_forward_metrics",
    "aggregate_metrics",
    "write_csv",
    "plot_distribution",
]
