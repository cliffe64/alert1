import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from indicators import atr, ema, linreg_features, zscore_volume


def test_ema_matches_pandas():
    data = np.random.default_rng(1).normal(100, 2, size=20)
    ours = ema(data, span=5)
    expected = pd.Series(data).ewm(span=5, adjust=False).mean().tolist()
    assert len(ours) == len(expected)
    for a, b in zip(ours, expected):
        assert pytest.approx(a, rel=0.01) == b


def test_atr_matches_reference():
    high = [12, 12.5, 13, 12.8, 13.2, 13.5]
    low = [10, 10.5, 11, 11.2, 11.8, 12.1]
    close = [11, 11.7, 12.4, 12.0, 12.6, 13.0]
    result = atr(high, low, close, period=3)
    assert result[-1] is not None
    df = pd.DataFrame({"high": high, "low": low, "close": close})
    df["prev_close"] = df["close"].shift(1)
    true_range = pd.concat(
        [
            (df["high"] - df["low"]),
            (df["high"] - df["prev_close"]).abs(),
            (df["low"] - df["prev_close"]).abs(),
        ],
        axis=1,
    ).max(axis=1)
    expected = true_range.ewm(alpha=1 / 3, adjust=False).mean()
    assert pytest.approx(result[-1], rel=0.05) == expected.iloc[-1]


def test_linreg_features_linear_series():
    close = [i * 2 for i in range(30)]
    slope, r2, resid_std, mid = linreg_features(close, window=20)
    assert pytest.approx(slope, rel=0.01) == 2.0
    assert pytest.approx(r2, rel=1e-3) == 1.0
    assert pytest.approx(resid_std, abs=1e-6) == 0.0
    assert pytest.approx(mid, rel=0.01) == close[-1]


def test_zscore_volume_basic():
    baseline = [100, 105, 98, 110, 102]
    z = zscore_volume(130, baseline)
    assert z is not None and z > 0
