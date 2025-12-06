"""Configuration loader for alerting system."""
from __future__ import annotations

import ast
import os
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

try:  # pragma: no cover - optional dependency
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback path exercised in tests
    yaml = None

try:  # pragma: no cover - optional dependency
    from dotenv import load_dotenv as _load_dotenv
except ModuleNotFoundError:  # pragma: no cover - fallback path exercised in tests

    def _load_dotenv(dotenv_path: Optional[Path] = None, override: bool = False) -> bool:
        """Minimal .env loader used when python-dotenv is unavailable."""

        if dotenv_path is None:
            return False
        path = Path(dotenv_path)
        if not path.exists():
            return False
        loaded = False
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if override or key not in os.environ:
                os.environ[key] = value
            loaded = True
        return loaded


def load_dotenv(dotenv_path: Optional[Path] = None, override: bool = False) -> bool:
    """Wrapper around python-dotenv or the local fallback implementation."""

    return _load_dotenv(dotenv_path=dotenv_path, override=override)


class VolumeSpikeMode(str, Enum):
    """Supported volume spike detection modes."""

    ZSCORE = "zscore"
    MULTIPLIER = "multiplier"


@dataclass
class VolumeSpikeZScoreConfig:
    """Configuration for z-score based volume spike detection."""

    lookback_windows: int = 96
    z_thr: float = 3.0
    min_notional_usd: float = 50_000.0
    min_abs_return: float = 0.005

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> "VolumeSpikeZScoreConfig":
        if not data:
            return cls()
        return cls(**data)


@dataclass
class VolumeSpikeBucketConfig:
    """Multiplier configuration bucket."""

    symbols: List[str] = field(default_factory=list)
    mult: float = 1.5
    min_notional_usd: float = 100_000.0

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> "VolumeSpikeBucketConfig":
        if not data:
            return cls()
        return cls(**data)


@dataclass
class VolumeSpikeMultiplierConfig:
    """Configuration for multiplier based volume spike detection."""

    buckets: Dict[str, VolumeSpikeBucketConfig] = field(default_factory=dict)
    min_abs_return: float = 0.005

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> "VolumeSpikeMultiplierConfig":
        if not data:
            return cls()
        buckets_data = data.get("buckets", {}) if isinstance(data, dict) else {}
        buckets = {
            name: VolumeSpikeBucketConfig.from_dict(value)
            for name, value in buckets_data.items()
        }
        min_abs_return = data.get("min_abs_return", 0.005)
        return cls(buckets=buckets, min_abs_return=min_abs_return)


@dataclass
class VolumeSpikeConfig:
    """Top level configuration for volume spike rules."""

    mode: VolumeSpikeMode = VolumeSpikeMode.ZSCORE
    zscore: VolumeSpikeZScoreConfig = field(default_factory=VolumeSpikeZScoreConfig)
    multiplier: VolumeSpikeMultiplierConfig = field(default_factory=VolumeSpikeMultiplierConfig)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> "VolumeSpikeConfig":
        if not data:
            return cls()
        mode = VolumeSpikeMode(data.get("mode", "zscore"))
        zscore = VolumeSpikeZScoreConfig.from_dict(data.get("zscore"))
        multiplier = VolumeSpikeMultiplierConfig.from_dict(data.get("multiplier"))
        return cls(mode=mode, zscore=zscore, multiplier=multiplier)


@dataclass
class TrendChannelConfig:
    """Configuration for trend channel rules."""

    window: int = 30
    r2_min: float = 0.6
    slope_norm_min: float = 0.0003
    slope_norm_max: float = 0.003
    resid_atr_max: float = 1.0
    pullback_atr_max: float = 0.5
    breakout_atr_mult: float = 1.5
    vol_confirm_z: float = 2.0

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> "TrendChannelConfig":
        if not data:
            return cls()
        return cls(**data)


class ConfirmMode(str, Enum):
    """Confirmation strategies for price alerts."""

    TIME = "time"
    SAMPLES = "samples"
    BAR_CLOSE = "bar_close"


@dataclass
class ConfirmConfig:
    """Confirmation parameters for price alerts."""

    mode: ConfirmMode
    seconds: Optional[int] = None
    total: Optional[int] = None
    pass_required: Optional[int] = None
    timeframe: Optional[str] = None

    def __post_init__(self) -> None:
        if self.mode == ConfirmMode.TIME and self.seconds is None:
            raise ValueError("time confirmation requires 'seconds'")
        if self.mode == ConfirmMode.SAMPLES:
            if self.total is None or self.pass_required is None:
                raise ValueError("samples confirmation requires 'total' and 'pass'")
        if self.mode == ConfirmMode.BAR_CLOSE and self.timeframe is None:
            raise ValueError("bar_close confirmation requires 'timeframe'")

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> Optional["ConfirmConfig"]:
        if not data:
            return None
        payload = dict(data)
        payload.setdefault("pass_required", payload.pop("pass", None))
        payload["mode"] = ConfirmMode(payload["mode"])
        return cls(**payload)


class PriceAlertType(str, Enum):
    """Supported price alert rule types."""

    ABOVE = "above"
    BELOW = "below"
    PCT_UP = "pct_up"
    PCT_DOWN = "pct_down"
    ATR_BREAKOUT = "atr_breakout"


@dataclass
class PriceAlertRuleConfig:
    """Configuration for a single price alert rule."""

    type: PriceAlertType
    level: Optional[float] = None
    pct: Optional[float] = None
    atr_k: Optional[float] = None
    direction: Optional[str] = None
    hysteresis: Optional[float] = None
    hysteresis_pct: Optional[float] = None
    confirm: Optional[ConfirmConfig] = None
    message: str = ""
    enabled: bool = True

    def __post_init__(self) -> None:
        if self.type in {PriceAlertType.ABOVE, PriceAlertType.BELOW} and self.level is None:
            raise ValueError("above/below rules require 'level'")
        if self.type in {PriceAlertType.PCT_UP, PriceAlertType.PCT_DOWN} and self.pct is None:
            raise ValueError("pct_up/pct_down rules require 'pct'")
        if self.type == PriceAlertType.ATR_BREAKOUT and self.atr_k is None:
            raise ValueError("atr_breakout rules require 'atr_k'")

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "PriceAlertRuleConfig":
        payload = dict(data)
        payload["type"] = PriceAlertType(payload["type"])
        if "confirm" in payload:
            payload["confirm"] = ConfirmConfig.from_dict(payload.get("confirm"))
        if "hysteresis_pct" in payload:
            payload["hysteresis_pct"] = payload.get("hysteresis_pct")
        return cls(**payload)


@dataclass
class DingtalkNotifierConfig:
    """DingTalk notifier configuration with environment indirection."""

    enabled: bool = False
    webhook_env: Optional[str] = None
    secret_env: Optional[str] = None

    @property
    def webhook(self) -> Optional[str]:
        return os.getenv(self.webhook_env) if self.webhook_env else None

    @property
    def secret(self) -> Optional[str]:
        return os.getenv(self.secret_env) if self.secret_env else None


@dataclass
class LocalSoundNotifierConfig:
    """Local sound notifier configuration."""

    enabled: bool = False
    sound_file: Optional[str] = None
    volume: float = 1.0


@dataclass
class NotifiersConfig:
    """Notifier collection configuration."""

    dingtalk: DingtalkNotifierConfig = field(default_factory=DingtalkNotifierConfig)
    local_sound: LocalSoundNotifierConfig = field(default_factory=LocalSoundNotifierConfig)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> "NotifiersConfig":
        if not data:
            return cls()
        dingtalk = DingtalkNotifierConfig(**data.get("dingtalk", {}))
        local_sound = LocalSoundNotifierConfig(**data.get("local_sound", {}))
        return cls(dingtalk=dingtalk, local_sound=local_sound)


@dataclass
class UIConfig:
    """UI presentation configuration."""

    timezone_display: str = "UTC"

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, object]]) -> "UIConfig":
        if not data:
            return cls()
        return cls(**data)


@dataclass
class AppConfig:
    """Top level configuration model."""

    symbols: List[str]
    timeframes: List[str]
    volume_spike: VolumeSpikeConfig = field(default_factory=VolumeSpikeConfig)
    trend_channel: TrendChannelConfig = field(default_factory=TrendChannelConfig)
    price_alerts: Dict[str, List[PriceAlertRuleConfig]] = field(default_factory=dict)
    notifiers: NotifiersConfig = field(default_factory=NotifiersConfig)
    cooldown_minutes: int = 10
    notification_rate_limit_minutes: int = 5
    ui: UIConfig = field(default_factory=UIConfig)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "AppConfig":
        if "symbols" not in data or "timeframes" not in data:
            raise ValueError("symbols and timeframes must be configured")

        volume_spike = VolumeSpikeConfig.from_dict(data.get("volume_spike"))
        trend_channel = TrendChannelConfig.from_dict(data.get("trend_channel"))
        raw_price_alerts = data.get("price_alerts") or {}
        price_alerts: Dict[str, List[PriceAlertRuleConfig]] = {}
        for symbol, rules in raw_price_alerts.items():
            price_alerts[symbol] = [PriceAlertRuleConfig.from_dict(rule) for rule in rules]
        notifiers = NotifiersConfig.from_dict(data.get("notifiers"))
        cooldown = data.get("cooldown_minutes", 10)
        rate_limit = data.get("notification_rate_limit_minutes", 5)
        ui = UIConfig.from_dict(data.get("ui"))
        return cls(
            symbols=list(data["symbols"]),
            timeframes=list(data["timeframes"]),
            volume_spike=volume_spike,
            trend_channel=trend_channel,
            price_alerts=price_alerts,
            notifiers=notifiers,
            cooldown_minutes=cooldown,
            notification_rate_limit_minutes=rate_limit,
            ui=ui,
        )


def _coerce_scalar(value: str) -> object:
    value = value.strip()
    if value == "" or value == "~" or value.lower() == "null":
        return None
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if value.startswith("0") and len(value) > 1 and value.replace("0", "", 1).isdigit():
            raise ValueError
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            pass

    if value.startswith("[") and value.endswith("]"):
        literal = value.replace("true", "True").replace("false", "False").replace("null", "None")
        return ast.literal_eval(literal)
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    return value


def _parse_simple_yaml(text: str) -> object:
    def _split(line: str) -> Tuple[int, str]:
        indent = len(line) - len(line.lstrip(" "))
        return indent, line.strip()

    lines: Deque[Tuple[int, str]] = deque(
        _split(line) for line in text.splitlines() if line.strip() and not line.strip().startswith("#")
    )

    def _parse_block(current_indent: int) -> object:
        result: Optional[object] = None
        while lines:
            indent, content = lines[0]
            if indent < current_indent:
                break
            if indent > current_indent:
                raise ValueError(f"Invalid indentation near: {content}")
            lines.popleft()

            if content.startswith("- "):
                if result is None:
                    result = []
                elif not isinstance(result, list):
                    raise ValueError("Mixed list/dict levels are not supported")
                item_content = content[2:].strip()
                if not item_content:
                    item_value = _parse_block(current_indent + 2)
                    result.append(item_value)
                    continue

                if ":" in item_content:
                    key, remainder = item_content.split(":", 1)
                    item: Dict[str, object] = {}
                    item[key.strip()] = _coerce_scalar(remainder.strip()) if remainder.strip() else None
                    nested = _parse_block(current_indent + 2)
                    if isinstance(nested, dict):
                        item.update(nested)
                    elif nested is not None:
                        raise ValueError("Unexpected nested structure under list item")
                    result.append(item)
                else:
                    result.append(_coerce_scalar(item_content))
            else:
                if result is None:
                    result = {}
                elif not isinstance(result, dict):
                    raise ValueError("Mixed dict/list levels are not supported")

                if content.endswith(":"):
                    key = content[:-1].strip()
                    result[key] = _parse_block(current_indent + 2)
                else:
                    key, remainder = content.split(":", 1)
                    result[key.strip()] = _coerce_scalar(remainder.strip())

        return result

    parsed = _parse_block(0)
    return parsed or {}


def load_config(
    config_path: Optional[Path] = None,
    env_path: Optional[Path] = None,
) -> AppConfig:
    """Load application configuration from YAML and environment variables."""

    base_path = Path(__file__).resolve().parents[1]
    if config_path is None:
        config_path = base_path / "config.yaml"
    if env_path is None:
        default_env = base_path / ".env"
        if default_env.exists():
            env_path = default_env

    load_dotenv(dotenv_path=env_path, override=False)

    with open(config_path, "r", encoding="utf-8") as fp:
        raw_text = fp.read()

    if yaml is not None:
        data = yaml.safe_load(raw_text) or {}
    else:
        data = _parse_simple_yaml(raw_text)

    return AppConfig.from_dict(data)
