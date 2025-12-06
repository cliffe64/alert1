"""Average True Range indicator."""

from __future__ import annotations

from typing import Iterable, List, Optional


def atr(
    high: Iterable[float],
    low: Iterable[float],
    close: Iterable[float],
    period: int,
) -> List[Optional[float]]:
    """Compute the Average True Range (ATR).

    Parameters
    ----------
    high, low, close:
        Price series of equal length.
    period:
        Smoothing window (typically 14).
    """

    if period <= 0:
        raise ValueError("period must be positive")

    highs = list(map(float, high))
    lows = list(map(float, low))
    closes = list(map(float, close))
    if not highs or len(highs) != len(lows) or len(highs) != len(closes):
        raise ValueError("high, low and close must be non-empty and of equal length")

    tr_values: List[float] = []
    for idx in range(len(highs)):
        hl = highs[idx] - lows[idx]
        if idx == 0:
            tr = hl
        else:
            prev_close = closes[idx - 1]
            tr = max(hl, abs(highs[idx] - prev_close), abs(lows[idx] - prev_close))
        tr_values.append(tr)

    atr_values: List[Optional[float]] = []
    prev_atr: Optional[float] = None
    for idx, tr in enumerate(tr_values):
        if idx + 1 < period:
            atr_values.append(None)
            continue
        if prev_atr is None:
            prev_atr = sum(tr_values[: period]) / period
        else:
            prev_atr = (prev_atr * (period - 1) + tr) / period
        atr_values.append(prev_atr)
    return atr_values


__all__ = ["atr"]
