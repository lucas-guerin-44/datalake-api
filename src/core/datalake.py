"""
DuckDB-based OHLC and tick data storage.
All data lives in a single embedded database file.
"""
import duckdb
import pandas as pd
from typing import Optional, List
from contextlib import contextmanager

from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.config import DUCKDB_PATH

logger = get_logger(__name__)

DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_db_connection():
    """Context manager for DuckDB connections."""
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        yield con
    finally:
        con.close()


def init_duckdb():
    """Create the ohlc_data and tick_data tables and indexes if they don't exist."""
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
                PRIMARY KEY (instrument, timeframe, timestamp)
            )
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlc_pk ON ohlc_data(instrument, timeframe, timestamp)")
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


def list_instruments() -> List[str]:
    """List all unique instruments in the database."""
    with get_db_connection() as con:
        rows = con.execute("SELECT DISTINCT instrument FROM ohlc_data ORDER BY instrument").fetchall()
        return [r[0] for r in rows]


def list_timeframes(instrument: Optional[str] = None) -> List[str]:
    """List unique timeframes, optionally filtered by instrument."""
    if instrument:
        instrument = validate_instrument(instrument)

    with get_db_connection() as con:
        if instrument:
            rows = con.execute(
                "SELECT DISTINCT timeframe FROM ohlc_data WHERE instrument = ? ORDER BY timeframe",
                [instrument],
            ).fetchall()
        else:
            rows = con.execute("SELECT DISTINCT timeframe FROM ohlc_data ORDER BY timeframe").fetchall()
        return [r[0] for r in rows]


def upsert_ohlc_data(df: pd.DataFrame, instrument: str, timeframe: str) -> int:
    """
    Upsert OHLC data — updates existing rows, inserts new ones.
    Returns the number of rows affected.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    if df.empty:
        return 0

    required = ["timestamp", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    insert_df = df[required].copy()
    insert_df["instrument"] = instrument
    insert_df["timeframe"] = timeframe
    insert_df["timestamp"] = pd.to_datetime(insert_df["timestamp"], utc=True)

    with get_db_connection() as con:
        con.execute("""
            INSERT OR REPLACE INTO ohlc_data
            (instrument, timeframe, timestamp, open, high, low, close)
            SELECT instrument, timeframe, timestamp, open, high, low, close
            FROM insert_df
        """)

    return len(insert_df)


def get_data_range(instrument: str, timeframe: str) -> Optional[dict]:
    """Get min/max date and row count for an instrument/timeframe pair."""
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    with get_db_connection() as con:
        result = con.execute("""
            SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
            FROM ohlc_data
            WHERE instrument = ? AND timeframe = ?
        """, [instrument, timeframe]).fetchone()

    if result and result[2] > 0:
        return {"min_date": result[0], "max_date": result[1], "count": result[2]}
    return None


def get_database_stats() -> dict:
    """Get overall database statistics."""
    with get_db_connection() as con:
        total_rows = con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0]

        instruments = con.execute("""
            SELECT instrument, COUNT(*) as count FROM ohlc_data
            GROUP BY instrument ORDER BY count DESC
        """).fetchall()

        timeframes = con.execute("""
            SELECT timeframe, COUNT(*) as count FROM ohlc_data
            GROUP BY timeframe ORDER BY count DESC
        """).fetchall()

        date_range = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM ohlc_data").fetchone()

    return {
        "database_path": str(DUCKDB_PATH),
        "total_rows": total_rows,
        "instruments": [{"instrument": r[0], "count": r[1]} for r in instruments],
        "timeframes": [{"timeframe": r[0], "count": r[1]} for r in timeframes],
        "date_range": {
            "min": str(date_range[0]) if date_range[0] else None,
            "max": str(date_range[1]) if date_range[1] else None,
        },
    }


# --- Tick data functions ---


def upsert_tick_data(df: pd.DataFrame, instrument: str) -> int:
    """
    Upsert tick data — updates existing rows, inserts new ones.
    Returns the number of rows affected.
    """
    instrument = validate_instrument(instrument)

    if df.empty:
        return 0

    required = ["timestamp", "price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    cols = ["timestamp", "price"]
    for optional_col in ["volume", "bid", "ask"]:
        if optional_col in df.columns:
            cols.append(optional_col)

    insert_df = df[cols].copy()
    insert_df["instrument"] = instrument
    insert_df["timestamp"] = pd.to_datetime(insert_df["timestamp"], utc=True)

    # Fill missing optional columns with defaults
    if "volume" not in insert_df.columns:
        insert_df["volume"] = 0.0
    if "bid" not in insert_df.columns:
        insert_df["bid"] = None
    if "ask" not in insert_df.columns:
        insert_df["ask"] = None

    with get_db_connection() as con:
        con.execute("""
            INSERT OR REPLACE INTO tick_data
            (instrument, timestamp, price, volume, bid, ask)
            SELECT instrument, timestamp, price, volume, bid, ask
            FROM insert_df
        """)

    return len(insert_df)


def list_tick_instruments() -> List[str]:
    """List all unique instruments in the tick_data table."""
    with get_db_connection() as con:
        rows = con.execute("SELECT DISTINCT instrument FROM tick_data ORDER BY instrument").fetchall()
        return [r[0] for r in rows]


def get_tick_coverage(instrument: str) -> Optional[dict]:
    """Get min/max timestamp and tick count for an instrument."""
    instrument = validate_instrument(instrument)

    with get_db_connection() as con:
        result = con.execute("""
            SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
            FROM tick_data
            WHERE instrument = ?
        """, [instrument]).fetchone()

    if result and result[2] > 0:
        return {"min_date": result[0], "max_date": result[1], "count": result[2]}
    return None


def get_tick_database_stats() -> dict:
    """Get tick_data table statistics."""
    with get_db_connection() as con:
        total_rows = con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0]

        instruments = con.execute("""
            SELECT instrument, COUNT(*) as count FROM tick_data
            GROUP BY instrument ORDER BY count DESC
        """).fetchall()

        date_range = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM tick_data").fetchone()

    return {
        "total_ticks": total_rows,
        "instruments": [{"instrument": r[0], "count": r[1]} for r in instruments],
        "date_range": {
            "min": str(date_range[0]) if date_range[0] else None,
            "max": str(date_range[1]) if date_range[1] else None,
        },
    }
