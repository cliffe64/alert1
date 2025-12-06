import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest import stats


def test_compute_forward_metrics() -> None:
    base_ts = 1000
    bars = [
        {"close_ts": base_ts, "close": 100.0},
        {"close_ts": base_ts + 60, "close": 102.0},
        {"close_ts": base_ts + 120, "close": 101.0},
        {"close_ts": base_ts + 180, "close": 105.0},
    ]
    metrics = stats.compute_forward_metrics(bars, base_ts, [1, 3])
    assert metrics is not None
    assert math.isclose(metrics["ret_1"], 0.02, rel_tol=1e-6)
    assert math.isclose(metrics["ret_3"], 0.05, rel_tol=1e-6)
    assert metrics["max_drawdown"] <= 0.0


def test_aggregate_metrics() -> None:
    metrics = [
        {"ret_30": 0.1, "ret_60": 0.2, "max_drawdown": -0.05},
        {"ret_30": 0.2, "ret_60": 0.3, "max_drawdown": -0.1},
    ]
    summary = stats.aggregate_metrics(metrics, [30, 60])
    assert math.isclose(summary["avg_ret_30"], 0.15, rel_tol=1e-6)
    assert math.isclose(summary["avg_max_drawdown"], -0.075, rel_tol=1e-6)
