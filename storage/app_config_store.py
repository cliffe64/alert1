"""App configuration persistence for UI-driven settings.

This module keeps the front-end editable configuration in a JSON file under
``storage/app_config.json`` so that Streamlit panels can create/update
endpoints, monitoring targets, and notifier switches without touching backend
code. The storage format mirrors the dataclasses defined in
``core.config_models`` and intentionally keeps defaults minimal to align with
the "front-end first" requirement.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, List

from core.config_models import AppConfig, EndpointEntry, MonitoredTarget, NotifierSwitch, ThresholdRule
from core.providers import TokenDescriptor

CONFIG_PATH = Path(__file__).resolve().parent / "app_config.json"


def _default_config() -> AppConfig:
    """Provide a sensible default config when no persisted file exists."""

    return AppConfig(
        endpoints=[
            EndpointEntry(name="Binance Futures", base_url="https://fapi.binance.com", priority=0),
            EndpointEntry(name="Binance Spot", base_url="https://api.binance.com", priority=1),
        ],
        targets=[],
        notifiers=[
            NotifierSwitch(name="dingtalk", enabled=False, testable=True),
            NotifierSwitch(name="local_sound", enabled=False, testable=True),
            NotifierSwitch(name="telegram", enabled=False, testable=False),
        ],
    )


def _from_dict(data: dict) -> AppConfig:
    endpoints = [EndpointEntry(**ep) for ep in data.get("endpoints", [])]
    notifiers = [NotifierSwitch(**nf) for nf in data.get("notifiers", [])]
    targets: List[MonitoredTarget] = []
    for raw_target in data.get("targets", []):
        token = TokenDescriptor(**raw_target["token"])
        rules = [ThresholdRule(**rule) for rule in raw_target.get("rules", [])]
        targets.append(MonitoredTarget(token=token, rules=rules, enabled=raw_target.get("enabled", True)))
    return AppConfig(endpoints=endpoints, targets=targets, notifiers=notifiers)


def load_app_config() -> AppConfig:
    """Load configuration from disk, falling back to defaults when missing."""

    if not CONFIG_PATH.exists():
        return _default_config()
    data = json.loads(CONFIG_PATH.read_text())
    return _from_dict(data)


def save_app_config(config: AppConfig) -> None:
    """Persist configuration to disk in a JSON-friendly shape."""

    payload = asdict(config)
    CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def upsert_endpoint(config: AppConfig, entry: EndpointEntry) -> AppConfig:
    """Insert or replace an endpoint by name and return the updated config."""

    remaining = [ep for ep in config.endpoints if ep.name != entry.name]
    updated = AppConfig(endpoints=remaining + [entry], targets=config.targets, notifiers=config.notifiers)
    save_app_config(updated)
    return updated


def delete_endpoint(config: AppConfig, name: str) -> AppConfig:
    """Remove an endpoint from the pool."""

    updated = AppConfig(
        endpoints=[ep for ep in config.endpoints if ep.name != name],
        targets=config.targets,
        notifiers=config.notifiers,
    )
    save_app_config(updated)
    return updated


def upsert_target(config: AppConfig, target: MonitoredTarget) -> AppConfig:
    """Insert or replace a monitored target by identifier."""

    remaining = [t for t in config.targets if t.token.identifier != target.token.identifier]
    updated = AppConfig(endpoints=config.endpoints, targets=remaining + [target], notifiers=config.notifiers)
    save_app_config(updated)
    return updated


def delete_target(config: AppConfig, identifier: str) -> AppConfig:
    """Delete a monitored target by its identifier."""

    updated = AppConfig(
        endpoints=config.endpoints,
        targets=[t for t in config.targets if t.token.identifier != identifier],
        notifiers=config.notifiers,
    )
    save_app_config(updated)
    return updated


def update_notifier(config: AppConfig, name: str, enabled: bool) -> AppConfig:
    """Toggle notifier switches while preserving other fields."""

    updated_notifiers: List[NotifierSwitch] = []
    for nf in config.notifiers:
        if nf.name == name:
            updated_notifiers.append(NotifierSwitch(name=nf.name, enabled=enabled, testable=nf.testable))
        else:
            updated_notifiers.append(nf)
    updated = AppConfig(endpoints=config.endpoints, targets=config.targets, notifiers=updated_notifiers)
    save_app_config(updated)
    return updated


__all__ = [
    "CONFIG_PATH",
    "load_app_config",
    "save_app_config",
    "upsert_endpoint",
    "delete_endpoint",
    "upsert_target",
    "delete_target",
    "update_notifier",
]
