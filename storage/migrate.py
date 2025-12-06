"""Database migration and initialization utilities."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable

from .sqlite_manager import get_db_path, _connect

LOGGER = logging.getLogger(__name__)


CREATE_TABLE_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS bars_1m (
        bid INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        exchange TEXT,
        chain TEXT,
        symbol TEXT,
        base TEXT,
        quote TEXT,
        open_ts INTEGER,
        close_ts INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume_base REAL,
        volume_quote REAL,
        notional_usd REAL,
        trades INTEGER,
        UNIQUE(source, exchange, chain, symbol, close_ts)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bars_5m (
        bid INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        exchange TEXT,
        chain TEXT,
        symbol TEXT,
        base TEXT,
        quote TEXT,
        open_ts INTEGER,
        close_ts INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume_base REAL,
        volume_quote REAL,
        notional_usd REAL,
        trades INTEGER,
        UNIQUE(source, exchange, chain, symbol, close_ts)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bars_15m (
        bid INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        exchange TEXT,
        chain TEXT,
        symbol TEXT,
        base TEXT,
        quote TEXT,
        open_ts INTEGER,
        close_ts INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume_base REAL,
        volume_quote REAL,
        notional_usd REAL,
        trades INTEGER,
        UNIQUE(source, exchange, chain, symbol, close_ts)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ts INTEGER,
        symbol TEXT,
        source TEXT,
        exchange TEXT,
        timeframe TEXT,
        rule TEXT,
        severity TEXT,
        message TEXT,
        detail_json TEXT,
        created_at INTEGER,
        delivered INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS local_notifier_state (
        id TEXT PRIMARY KEY,
        last_event_id TEXT,
        last_created_at INTEGER,
        updated_at INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS notification_limiter (
        id TEXT PRIMARY KEY,
        last_sent_ts INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS price_alert_rules (
        id TEXT PRIMARY KEY,
        symbol TEXT,
        type TEXT,
        level REAL,
        pct REAL,
        atr_k REAL,
        direction TEXT,
        hysteresis REAL,
        hysteresis_pct REAL,
        confirm_mode TEXT,
        confirm_seconds INTEGER,
        confirm_samples_total INTEGER,
        confirm_samples_pass INTEGER,
        confirm_timeframe TEXT,
        message TEXT,
        enabled INTEGER,
        created_at INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS token_registry (
        id TEXT PRIMARY KEY,
        source TEXT,
        exchange TEXT,
        chain TEXT,
        symbol TEXT,
        base TEXT,
        quote TEXT,
        token_address TEXT,
        pool_address TEXT,
        decimals INTEGER,
        enabled INTEGER,
        extra_json TEXT,
        created_at INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cooldown_state (
        id TEXT PRIMARY KEY,
        symbol TEXT,
        rule TEXT,
        timeframe TEXT,
        last_fire_ts INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS kv_state (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at INTEGER
    )
    """,
)


def initialize_database(db_path: str | None = None) -> None:
    """Initialize all required tables in the SQLite database."""
    path = Path(db_path or get_db_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(str(path)) as conn:
        cursor = conn.cursor()
        for statement in CREATE_TABLE_STATEMENTS:
            cursor.execute(statement)
        conn.commit()
    LOGGER.info("Database initialized at %s", path)


def _parse_args(args: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQLite migration helper")
    parser.add_argument("--init", action="store_true", help="initialize database tables")
    parser.add_argument("--db-path", help="override database path", default=None)
    return parser.parse_args(args)


def main(argv: Iterable[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.init:
        initialize_database(args.db_path)
    else:
        LOGGER.info("No action specified. Use --init to create tables.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
