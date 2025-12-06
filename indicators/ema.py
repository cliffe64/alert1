"""Exponential moving average indicator."""

from __future__ import annotations

from typing import Iterable, List, Optional


def ema(series: Iterable[float], span: int) -> List[Optional[float]]:
    """Compute the exponential moving average for ``series``.

    Parameters
    ----------
    series:
        Iterable of float values.
    span:
        Window span. Must be positive.

    Returns
    -------
    list of float | None
        EMA values for each observation. ``None`` is returned for empty input.
    """

    if span <= 0:
        raise ValueError("span must be positive")

    values = list(series)
    if not values:
        return []

    alpha = 2.0 / (span + 1.0)
    ema_values: List[Optional[float]] = []
    prev: Optional[float] = None
    for value in values:
        if prev is None:
            prev = float(value)
        else:
            prev = prev + alpha * (float(value) - prev)
        ema_values.append(prev)
    return ema_values


__all__ = ["ema"]
