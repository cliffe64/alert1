"""Z-score helper for volume analysis."""

from __future__ import annotations

from statistics import mean, pstdev
from typing import Iterable, Optional


def zscore_volume(current_notional: float, baseline_series: Iterable[float]) -> Optional[float]:
    """Return the z-score of ``current_notional`` against ``baseline_series``."""

    samples = [float(value) for value in baseline_series if value is not None]
    if len(samples) < 2:
        return None
    mu = mean(samples)
    sigma = pstdev(samples)
    if sigma == 0:
        return None
    return (float(current_notional) - mu) / sigma


__all__ = ["zscore_volume"]
