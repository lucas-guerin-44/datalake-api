"""
DuckDB-based OHLC and tick data storage.

This module manages the database connection lifecycle and schema. All data
operations are split into focused modules:
  - src.core.queries  — read-only operations (list, get, find_gaps)
  - src.core.writes   — write operations (upsert, delete, migrate)
  - src.core.derive   — timeframe derivation
"""
import os
import threading
from datetime import datetime

import duckdb
import pandas as pd
from typing import Optional
from contextlib import contextmanager

from src.middleware.logging_config import get_logger
from src.config import DUCKDB_PATH, DUCKDB_MEMORY_LIMIT
from src.core.migrations import run_migrations

logger = get_logger(__name__)


def to_naive_utc(ts) -> datetime:
    """Convert any timestamp-like value to a naive-UTC datetime for DuckDB's TIMESTAMP column."""
    t = pd.Timestamp(ts)
    if t.tz is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    return t.to_pydatetime()

DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

MIGRATION_MEMORY_LIMIT = os.getenv("MIGRATION_MEMORY_LIMIT", DUCKDB_MEMORY_LIMIT)

# --- Connection management ----------------------------------------------------

_db_lock = threading.Lock()
_db_connection: Optional[duckdb.DuckDBPyConnection] = None


def _get_shared_connection() -> duckdb.DuckDBPyConnection:
    """Get or create the shared DuckDB connection (the write/transaction connection)."""
    global _db_connection
    if _db_connection is None:
        with _db_lock:
            if _db_connection is None:
                _db_connection = duckdb.connect(str(DUCKDB_PATH))
                _db_connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
                tmp_dir = DUCKDB_PATH.parent / "duckdb_tmp"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                _db_connection.execute(f"SET temp_directory = '{tmp_dir.as_posix()}'")
                logger.info("DuckDB connection opened", extra={
                    "path": str(DUCKDB_PATH),
                    "memory_limit": DUCKDB_MEMORY_LIMIT,
                    "temp_directory": str(tmp_dir),
                })
    return _db_connection


@contextmanager
def get_db_connection():
    """Yield the shared DuckDB connection (the write connection)."""
    con = _get_shared_connection()
    yield con


@contextmanager
def read_connection():
    """
    Yield an independent DuckDB cursor for READ-ONLY queries.
    Concurrent reads run in parallel via DuckDB's MVCC snapshots.
    """
    con = _get_shared_connection().cursor()
    try:
        yield con
    finally:
        con.close()


# --- Data version (cache invalidation) ----------------------------------------

_data_version = 0
_data_version_lock = threading.Lock()


def get_data_version() -> int:
    """Current data-version token. Changes whenever ohlc_data/tick_data is written."""
    return _data_version


def bump_data_version() -> None:
    """Invalidate version-keyed read caches."""
    global _data_version
    with _data_version_lock:
        _data_version += 1


def duckdb_ready() -> bool:
    """Cheap readiness signal: True once the database instance is open."""
    return _db_connection is not None


# --- Write transaction --------------------------------------------------------

_write_tx_lock = threading.Lock()


@contextmanager
def write_transaction():
    """
    Wrap a group of writes in an atomic transaction. Rolls back on any exception.
    """
    with _write_tx_lock:
        con = _get_shared_connection()
        con.execute("BEGIN TRANSACTION")
        try:
            yield con
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        else:
            bump_data_version()


# --- Schema initialization ----------------------------------------------------

_initialized = False


def init_duckdb():
    """Create the ohlc_data and tick_data tables and indexes if they don't exist."""
    global _initialized
    if _initialized and _db_connection is not None:
        return
    _initialized = False
    with get_db_connection() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ohlc_data (
                instrument VARCHAR NOT NULL,
                timeframe VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                source VARCHAR NOT NULL DEFAULT 'raw',
                PRIMARY KEY (instrument, timeframe, timestamp)
            )
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlc_pk ON ohlc_data(instrument, timeframe, timestamp)")

        run_migrations(con)
        con.execute("CREATE INDEX IF NOT EXISTS idx_instrument_timeframe ON ohlc_data(instrument, timeframe)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON ohlc_data(timestamp)")

        con.execute("""
            CREATE TABLE IF NOT EXISTS tick_data (
                instrument VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                price DOUBLE NOT NULL,
                volume DOUBLE DEFAULT 0.0,
                bid DOUBLE,
                ask DOUBLE,
                PRIMARY KEY (instrument, timestamp)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_tick_instrument ON tick_data(instrument)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_tick_ts ON tick_data(instrument, timestamp)")

        logger.info("DuckDB initialized", extra={"path": str(DUCKDB_PATH)})
    _initialized = True


# --- Timeframe constants (shared by derive.py and queries.py) -----------------

_TF_SECONDS = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400,
}
