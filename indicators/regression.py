"""Linear regression helpers used by trend rules."""

from __future__ import annotations

import math
from typing import Iterable, Optional, Sequence, Tuple

import numpy as np


def linreg_features(close: Sequence[float], window: int) -> Tuple[Optional[float], ...]:
    """Return slope, R², residual std and mid price for ``close``.

    Parameters
    ----------
    close:
        Price sequence.
    window:
        Number of trailing observations to analyse.
    """

    if window <= 1:
        raise ValueError("window must be greater than 1")
    if len(close) < window:
        return (None, None, None, None)

    y = np.asarray(close[-window:], dtype=float)
    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    y_mean = y.mean()
    cov = np.dot(x - x_mean, y - y_mean)
    var = np.dot(x - x_mean, x - x_mean)
    if var == 0:
        return (None, None, None, None)
    slope = cov / var
    intercept = y_mean - slope * x_mean
    fitted = intercept + slope * x
    residuals = y - fitted
    ss_res = float(np.dot(residuals, residuals))
    ss_tot = float(np.dot(y - y_mean, y - y_mean))
    r2 = 1.0 - ss_res / ss_tot if ss_tot else 0.0
    resid_std = math.sqrt(ss_res / window)
    mid_price = float(fitted[-1])
    return (float(slope), float(r2), float(resid_std), mid_price)


__all__ = ["linreg_features"]
