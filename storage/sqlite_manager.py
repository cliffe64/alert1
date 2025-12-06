"""SQLite database management utilities for the alerting service."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional

_SEVERITY_ORDER = {"info": 0, "warning": 1, "error": 2, "critical": 3}

_DB_PATH_ENV = "ALERT_DB_PATH"
_DEFAULT_DB_FILENAME = "alert.db"
_ALLOWED_BAR_TABLES = {"bars_1m", "bars_5m", "bars_15m"}

_connection_lock = Lock()


def get_db_path() -> str:
    """Return the configured SQLite database path."""
    env_path = os.environ.get(_DB_PATH_ENV)
    if env_path:
        return env_path
    storage_dir = Path(__file__).resolve().parent
    return str(storage_dir / _DEFAULT_DB_FILENAME)


def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = Path(db_path or get_db_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    # SQLite autocommit is enabled when isolation_level=None. We keep the default
    # behaviour (implicit transactions) to ensure atomicity of batch upserts.
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _execute(query: str, params: Iterable[Any] | Dict[str, Any] | None = None) -> None:
    with _connection_lock:
        with _connect() as conn:
            conn.execute(query, params or [])
            conn.commit()


def _executemany(query: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
    with _connection_lock:
        with _connect() as conn:
            conn.executemany(query, seq_of_params)
            conn.commit()


def _query(
    query: str,
    params: Iterable[Any] | Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    with _connection_lock:
        with _connect() as conn:
            cursor = conn.execute(query, params or [])
            rows = cursor.fetchall()
    return [dict(row) for row in rows]


def _build_upsert_sql(
    table: str,
    payload: Dict[str, Any],
    conflict_columns: Iterable[str],
    skip_update: Iterable[str] | None = None,
) -> tuple[str, list[Any]]:
    columns = list(payload.keys())
    if not columns:
        raise ValueError("payload must include at least one column")
    placeholders = ", ".join(["?" for _ in columns])
    columns_sql = ", ".join(columns)
    skip = set(skip_update or [])
    conflict_set = set(conflict_columns)
    update_columns = [col for col in columns if col not in conflict_set and col not in skip]
    if update_columns:
        update_clause = ", ".join([f"{col}=excluded.{col}" for col in update_columns])
        sql = (
            f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(conflict_columns)}) DO UPDATE SET {update_clause}"
        )
    else:
        sql = (
            f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(conflict_columns)}) DO NOTHING"
        )
    params = [payload[col] for col in columns]
    return sql, params


def upsert_bar(table: str, bar: Dict[str, Any]) -> None:
    """Insert or update a bar record in the specified timeframe table."""
    if table not in _ALLOWED_BAR_TABLES:
        raise ValueError(f"Unsupported bars table: {table}")
    if not bar:
        raise ValueError("bar payload is empty")

    sql, params = _build_upsert_sql(
        table,
        bar,
        conflict_columns=("source", "exchange", "chain", "symbol", "close_ts"),
        skip_update={"bid"},
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def fetch_bars(
    table: str,
    symbol: str,
    since_ts: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch bars ordered by close_ts ascending."""
    if table not in _ALLOWED_BAR_TABLES:
        raise ValueError(f"Unsupported bars table: {table}")

    query = f"SELECT * FROM {table} WHERE symbol = ?"
    params: List[Any] = [symbol]
    if since_ts is not None:
        query += " AND close_ts >= ?"
        params.append(since_ts)
    query += " ORDER BY close_ts ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return _query(query, params)


def fetch_recent_bars(table: str, symbol: str, limit: int) -> List[Dict[str, Any]]:
    """Return the most recent ``limit`` bars for ``symbol`` ordered ascending."""

    if table not in _ALLOWED_BAR_TABLES:
        raise ValueError(f"Unsupported bars table: {table}")
    if limit <= 0:
        return []
    query = (
        f"SELECT * FROM {table} WHERE symbol = ? ORDER BY close_ts DESC LIMIT ?"
    )
    rows = _query(query, (symbol, limit))
    return list(reversed(rows))


def fetch_latest_bar(table: str, symbol: str) -> Optional[Dict[str, Any]]:
    """Return the latest bar for a symbol or ``None`` when unavailable."""

    result = fetch_recent_bars(table, symbol, limit=1)
    return result[0] if result else None


def insert_event(event: Dict[str, Any]) -> None:
    if "id" not in event:
        raise ValueError("event must include 'id'")
    sql, params = _build_upsert_sql(
        "events",
        event,
        conflict_columns=("id",),
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def set_kv(key: str, value: str, updated_at: int) -> None:
    sql = (
        "INSERT INTO kv_state (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at"
    )
    _execute(sql, (key, value, updated_at))


def get_kv(key: str) -> Optional[Dict[str, Any]]:
    rows = _query("SELECT key, value, updated_at FROM kv_state WHERE key = ?", (key,))
    return rows[0] if rows else None


def upsert_rule(rule: Dict[str, Any]) -> None:
    if "id" not in rule:
        raise ValueError("rule must include 'id'")
    sql, params = _build_upsert_sql(
        "price_alert_rules",
        rule,
        conflict_columns=("id",),
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def list_rules(symbol: Optional[str] = None, enabled: Optional[bool] = None) -> List[Dict[str, Any]]:
    query = "SELECT * FROM price_alert_rules"
    conditions: List[str] = []
    params: List[Any] = []
    if symbol is not None:
        conditions.append("symbol = ?")
        params.append(symbol)
    if enabled is not None:
        conditions.append("enabled = ?")
        params.append(1 if enabled else 0)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY symbol, level"
    return _query(query, params)


def upsert_token(token: Dict[str, Any]) -> None:
    if "id" not in token:
        raise ValueError("token must include 'id'")
    sql, params = _build_upsert_sql(
        "token_registry",
        token,
        conflict_columns=("id",),
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def list_tokens(enabled: Optional[bool] = None) -> List[Dict[str, Any]]:
    query = "SELECT * FROM token_registry"
    params: List[Any] = []
    if enabled is not None:
        query += " WHERE enabled = ?"
        params.append(1 if enabled else 0)
    query += " ORDER BY symbol"
    return _query(query, params)


def fetch_undelivered_events(limit: int = 100) -> List[Dict[str, Any]]:
    query = (
        "SELECT * FROM events WHERE delivered = 0 ORDER BY created_at ASC LIMIT ?"
    )
    return _query(query, (limit,))


def mark_event_delivered(event_id: str) -> None:
    _execute("UPDATE events SET delivered = 1 WHERE id = ?", (event_id,))


def fetch_events_since(
    created_after: Optional[int],
    limit: int = 100,
    min_severity: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch events created after ``created_after`` sorted ascending."""

    query = "SELECT * FROM events WHERE 1=1"
    params: List[Any] = []
    if created_after is not None:
        query += " AND created_at > ?"
        params.append(created_after)
    query += " ORDER BY created_at ASC LIMIT ?"
    params.append(limit)
    events = _query(query, params)
    if min_severity is None:
        return events
    threshold = _SEVERITY_ORDER.get(min_severity.lower(), 0)
    filtered: List[Dict[str, Any]] = []
    for event in events:
        rank = _SEVERITY_ORDER.get(str(event.get("severity", "")).lower(), 0)
        if rank >= threshold:
            filtered.append(event)
    return filtered


def list_events(
    timeframe: Optional[str] = None,
    symbols: Optional[Iterable[str]] = None,
    since_ts: Optional[int] = None,
    rules: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """List events ordered by timestamp."""

    query = "SELECT * FROM events WHERE 1=1"
    params: List[Any] = []
    if timeframe:
        query += " AND timeframe = ?"
        params.append(timeframe)
    if symbols:
        placeholders = ",".join(["?" for _ in symbols])
        query += f" AND symbol IN ({placeholders})"
        params.extend(symbols)
    if rules:
        placeholders = ",".join(["?" for _ in rules])
        query += f" AND rule IN ({placeholders})"
        params.extend(rules)
    if since_ts is not None:
        query += " AND ts >= ?"
        params.append(since_ts)
    query += " ORDER BY ts ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return _query(query, params)


def get_local_notifier_state(client_id: str) -> Optional[Dict[str, Any]]:
    rows = _query(
        "SELECT * FROM local_notifier_state WHERE id = ?",
        (client_id,),
    )
    return rows[0] if rows else None


def update_local_notifier_state(
    client_id: str,
    last_event_id: Optional[str],
    last_created_at: Optional[int],
    updated_at: int,
) -> None:
    sql = (
        "INSERT INTO local_notifier_state (id, last_event_id, last_created_at, updated_at)"
        " VALUES (?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "last_event_id=excluded.last_event_id, "
        "last_created_at=excluded.last_created_at, "
        "updated_at=excluded.updated_at"
    )
    _execute(sql, (client_id, last_event_id, last_created_at, updated_at))


def should_rate_limit(key: str, window_seconds: int, now_ts: int) -> bool:
    if window_seconds <= 0:
        return False
    rows = _query(
        "SELECT last_sent_ts FROM notification_limiter WHERE id = ?",
        (key,),
    )
    if not rows:
        return False
    last_sent = rows[0]["last_sent_ts"] or 0
    return now_ts - int(last_sent) < window_seconds


def update_rate_limit_timestamp(key: str, now_ts: int) -> None:
    sql = (
        "INSERT INTO notification_limiter (id, last_sent_ts) VALUES (?, ?) "
        "ON CONFLICT(id) DO UPDATE SET last_sent_ts=excluded.last_sent_ts"
    )
    _execute(sql, (key, now_ts))


def get_cooldown_state(key: str) -> Optional[Dict[str, Any]]:
    rows = _query("SELECT * FROM cooldown_state WHERE id = ?", (key,))
    return rows[0] if rows else None


def upsert_cooldown_state(
    key: str,
    symbol: str,
    rule: str,
    timeframe: str,
    last_fire_ts: int,
) -> None:
    sql = (
        "INSERT INTO cooldown_state (id, symbol, rule, timeframe, last_fire_ts)"
        " VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET last_fire_ts=excluded.last_fire_ts"
    )
    _execute(sql, (key, symbol, rule, timeframe, last_fire_ts))


__all__ = [
    "get_db_path",
    "upsert_bar",
    "fetch_bars",
    "fetch_recent_bars",
    "fetch_latest_bar",
    "insert_event",
    "set_kv",
    "get_kv",
    "upsert_rule",
    "list_rules",
    "upsert_token",
    "list_tokens",
    "fetch_undelivered_events",
    "mark_event_delivered",
    "fetch_events_since",
    "list_events",
    "get_local_notifier_state",
    "update_local_notifier_state",
    "should_rate_limit",
    "update_rate_limit_timestamp",
    "get_cooldown_state",
    "upsert_cooldown_state",
]
