import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rules.config_loader import (
    ConfirmMode,
    PriceAlertType,
    load_config,
)


@pytest.fixture()
def temp_files(tmp_path: Path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "DINGTALK_WEBHOOK=https://example.com/hook",
                "DINGTALK_SECRET=supersecret",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
symbols: ["BTCUSDT"]
timeframes: ["5m", "15m"]
volume_spike:
  mode: "zscore"
trend_channel:
  window: 40
  r2_min: 0.7
price_alerts:
  BTCUSDT:
    - type: "above"
      level: 68000
      hysteresis: 150
      confirm:
        mode: "time"
        seconds: 30
      message: "above test"
    - type: "pct_up"
      pct: 0.05
      confirm:
        mode: "samples"
        total: 4
        pass: 3
notifiers:
  dingtalk:
    enabled: true
    webhook_env: "DINGTALK_WEBHOOK"
    secret_env: "DINGTALK_SECRET"
  local_sound:
    enabled: true
    sound_file: "./alert.wav"
    volume: 0.5
cooldown_minutes: 15
ui:
  timezone_display: "local"
""",
        encoding="utf-8",
    )

    yield config_path, env_path

    if "DINGTALK_WEBHOOK" in os.environ:
        del os.environ["DINGTALK_WEBHOOK"]
    if "DINGTALK_SECRET" in os.environ:
        del os.environ["DINGTALK_SECRET"]


def test_load_config_with_env(temp_files):
    config_path, env_path = temp_files
    config = load_config(config_path=config_path, env_path=env_path)

    assert config.symbols == ["BTCUSDT"]
    assert config.trend_channel.window == 40
    assert config.cooldown_minutes == 15

    dingtalk = config.notifiers.dingtalk
    assert dingtalk.enabled is True
    assert dingtalk.webhook == "https://example.com/hook"
    assert dingtalk.secret == "supersecret"

    sound = config.notifiers.local_sound
    assert sound.enabled is True
    assert sound.sound_file == "./alert.wav"
    assert sound.volume == 0.5

    rules = config.price_alerts["BTCUSDT"]
    assert rules[0].type is PriceAlertType.ABOVE
    assert rules[0].confirm and rules[0].confirm.mode is ConfirmMode.TIME
    assert rules[1].type is PriceAlertType.PCT_UP
    assert rules[1].confirm and rules[1].confirm.mode is ConfirmMode.SAMPLES
    assert rules[1].confirm.total == 4
    assert rules[1].confirm.pass_required == 3


def test_defaults_when_sections_missing(tmp_path: Path):
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        """
symbols: ["ETHUSDT"]
timeframes: ["5m"]
""",
        encoding="utf-8",
    )

    config = load_config(config_path=cfg_path)

    assert config.volume_spike.mode.value == "zscore"
    assert config.trend_channel.r2_min == 0.6
    assert config.cooldown_minutes == 10
    assert config.price_alerts == {}
    assert config.notifiers.dingtalk.enabled is False
    assert config.ui.timezone_display == "UTC"
