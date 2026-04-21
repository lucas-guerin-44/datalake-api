"""
Data ingestion pipeline for OHLC and tick data.
Reads CSV/Excel files and inserts data into DuckDB.
"""
import time
from pathlib import Path
from typing import Optional, Dict

import pandas as pd

from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.core.datalake import (
    upsert_ohlc_data,
    upsert_tick_data,
    init_duckdb,
    derive_ohlc_timeframes,
    derive_ohlc_from_ticks,
    write_transaction,
    get_db_connection,
)


def _count_existing_ohlc(instrument: str, timeframe: str, start, end) -> int:
    with get_db_connection() as con:
        return con.execute(
            """SELECT COUNT(*) FROM ohlc_data
               WHERE instrument = ? AND timeframe = ?
                 AND timestamp >= ? AND timestamp <= ?""",
            [instrument, timeframe, start, end],
        ).fetchone()[0]


def _count_existing_ticks(instrument: str, start, end) -> int:
    with get_db_connection() as con:
        return con.execute(
            """SELECT COUNT(*) FROM tick_data
               WHERE instrument = ?
                 AND timestamp >= ? AND timestamp <= ?""",
            [instrument, start, end],
        ).fetchone()[0]

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
DEFAULT_STAGING = PROJECT_ROOT / "staging"

REQUIRED_COLS = ["timestamp", "open", "high", "low", "close"]


def _read_raw(path: Path) -> pd.DataFrame:
    """Read a raw CSV/Excel file, auto-detecting MetaTrader export format."""
    with open(path, "rb") as f:
        first_line = f.readline().decode(errors="ignore")

    # MetaTrader export format: columns like <DATE>, <OPEN>, etc.
    if "<DATE>" in first_line:
        df = pd.read_csv(path, sep=r"\s+", engine="python")
        if "<TIME>" in df.columns:
            df["timestamp"] = pd.to_datetime(df["<DATE>"] + " " + df["<TIME>"], utc=True)
        else:
            df["timestamp"] = pd.to_datetime(df["<DATE>"], utc=True)

        df = df.rename(columns={
            "<OPEN>": "open", "<HIGH>": "high", "<LOW>": "low", "<CLOSE>": "close",
        })
        return df[[c for c in df.columns if c in REQUIRED_COLS]]

    ext = path.suffix.lower()
    if ext == ".csv":
        try:
            return pd.read_csv(path)
        except UnicodeDecodeError:
            return pd.read_csv(path, encoding="utf-16")
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def _standardize(df: pd.DataFrame, column_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Normalize column names, validate required columns, and coerce types."""
    df.columns = [c.strip().lower() for c in df.columns]

    if column_map:
        df = df.rename(columns=column_map)

    # Drop common non-OHLC columns from broker exports
    for c in ["tickvol", "vol", "spread", "volume"]:
        if c in df.columns:
            df = df.drop(columns=c)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after mapping: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if df["timestamp"].isna().any():
        bad = df[df["timestamp"].isna()].shape[0]
        raise ValueError(f"Found {bad} rows with invalid timestamps")

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"]).sort_values("timestamp")
    return df


def parse_filename_meta(path: Path):
    """
    Parse instrument and timeframe from filenames like:
    XAUUSD_M5_201801020100_202507112355.csv
    """
    parts = path.stem.split("_")
    if len(parts) < 2:
        raise ValueError(f"Filename {path} doesn't match expected pattern INSTRUMENT_TIMEFRAME_*.csv")
    return parts[0], parts[1]


def ingest_single_file(file: Path, instrument: str, timeframe: str, derive: bool = True) -> int:
    """
    Ingest a single CSV/Excel file into DuckDB.

    If `derive=True`, automatically materialize higher timeframes from the ingested window.
    Returns the number of rows inserted.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    logger.info("Starting file ingestion", extra={"file": str(file), "instrument": instrument, "timeframe": timeframe})

    t_total = time.perf_counter()
    init_duckdb()

    t_read = time.perf_counter()
    raw = _read_raw(file)
    df = _standardize(raw)
    ms_read = int((time.perf_counter() - t_read) * 1000)

    rows_in_file = len(df)
    rows_matched = 0
    derive_result: Dict = {}
    ms_upsert = ms_derive = 0

    if rows_in_file > 0:
        win_start, win_end = df["timestamp"].min(), df["timestamp"].max()
        rows_matched = _count_existing_ohlc(instrument, timeframe, win_start, win_end)

    with write_transaction():
        t_upsert = time.perf_counter()
        rows_inserted = upsert_ohlc_data(df, instrument, timeframe)
        ms_upsert = int((time.perf_counter() - t_upsert) * 1000)

        if derive and not df.empty:
            t_derive = time.perf_counter()
            derive_result = derive_ohlc_timeframes(instrument, timeframe, df["timestamp"].min(), df["timestamp"].max())
            ms_derive = int((time.perf_counter() - t_derive) * 1000)

    after = _count_existing_ohlc(instrument, timeframe, df["timestamp"].min(), df["timestamp"].max()) if rows_in_file > 0 else 0
    rows_new = max(0, after - rows_matched)

    logger.info("File ingestion completed", extra={
        "file": str(file),
        "instrument": instrument,
        "timeframe": timeframe,
        "rows_in_file": rows_in_file,
        "rows_new": rows_new,
        "rows_matched": max(0, rows_inserted - rows_new),
        "derived_targets": derive_result,
        "timing_ms": {
            "read": ms_read,
            "upsert": ms_upsert,
            "derive": ms_derive,
            "total": int((time.perf_counter() - t_total) * 1000),
        },
    })
    return rows_inserted


def ingest_dataframe(df: pd.DataFrame, instrument: str, timeframe: str, derive: bool = True) -> int:
    """
    Ingest a DataFrame directly into DuckDB.
    Must have a 'timestamp' column plus open/high/low/close.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)
    init_duckdb()

    if "timestamp" not in df.columns:
        raise ValueError("DataFrame must have a 'timestamp' column")

    standardized = _standardize(df.copy())

    with write_transaction():
        rows = upsert_ohlc_data(standardized, instrument, timeframe)
        if derive and not standardized.empty:
            derive_ohlc_timeframes(instrument, timeframe, standardized["timestamp"].min(), standardized["timestamp"].max())

    return rows


# --- Tick data pipeline ---

TICK_REQUIRED_COLS = ["timestamp", "price"]


def _read_raw_tick(path: Path) -> pd.DataFrame:
    """Read a raw tick CSV file, auto-detecting MetaTrader and Dukascopy formats."""
    with open(path, "rb") as f:
        first_line = f.readline().decode(errors="ignore")

    # MetaTrader tick export: <DATE> <TIME> <BID> <ASK> <LAST> <VOLUME>
    if "<DATE>" in first_line and ("<BID>" in first_line or "<LAST>" in first_line):
        df = pd.read_csv(path, sep=r"\s+", engine="python")
        if "<TIME>" in df.columns:
            df["timestamp"] = pd.to_datetime(df["<DATE>"] + " " + df["<TIME>"], utc=True)
        else:
            df["timestamp"] = pd.to_datetime(df["<DATE>"], utc=True)

        rename = {}
        if "<BID>" in df.columns:
            rename["<BID>"] = "bid"
        if "<ASK>" in df.columns:
            rename["<ASK>"] = "ask"
        if "<LAST>" in df.columns:
            rename["<LAST>"] = "price"
        if "<VOLUME>" in df.columns:
            rename["<VOLUME>"] = "volume"
        df = df.rename(columns=rename)

        # Compute mid price from bid/ask if no explicit price
        if "price" not in df.columns and "bid" in df.columns and "ask" in df.columns:
            df["price"] = (df["bid"] + df["ask"]) / 2

        keep = [c for c in df.columns if c in ["timestamp", "price", "volume", "bid", "ask"]]
        return df[keep]

    # Dukascopy format: Gmt time,Bid,Ask,Volume
    lower_first = first_line.lower()
    if "gmt time" in lower_first or ("bid" in lower_first and "ask" in lower_first and "open" not in lower_first):
        df = pd.read_csv(path)
        df.columns = [c.strip().lower() for c in df.columns]
        rename = {}
        for col in df.columns:
            if "gmt" in col or "time" in col:
                rename[col] = "timestamp"
        df = df.rename(columns=rename)

        if "price" not in df.columns and "bid" in df.columns and "ask" in df.columns:
            df["price"] = (df["bid"] + df["ask"]) / 2

        keep = [c for c in df.columns if c in ["timestamp", "price", "volume", "bid", "ask"]]
        return df[keep]

    # Generic tick CSV: timestamp, price, volume (optional bid, ask)
    try:
        df = pd.read_csv(path)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="utf-16")
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def standardize_tick_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize tick data: validate columns, compute mid price if needed, clean up."""
    df.columns = [c.strip().lower() for c in df.columns]

    # Compute mid price from bid/ask if price column is missing
    if "price" not in df.columns:
        if "bid" in df.columns and "ask" in df.columns:
            df["price"] = (df["bid"] + df["ask"]) / 2
        else:
            raise ValueError("Missing 'price' column and cannot compute from bid/ask")

    missing = [c for c in TICK_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after mapping: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if df["timestamp"].isna().any():
        bad = df[df["timestamp"].isna()].shape[0]
        raise ValueError(f"Found {bad} rows with invalid timestamps")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if df["price"].isna().any() or (df["price"] <= 0).any():
        raise ValueError("Tick data contains NaN or non-positive prices")

    for col in ["volume", "bid", "ask"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates().sort_values("timestamp")
    return df


def ingest_tick_file(file: Path, instrument: str, derive: bool = True) -> int:
    """Ingest a single tick CSV file into DuckDB. Returns the number of rows inserted."""
    instrument = validate_instrument(instrument)

    logger.info("Starting tick file ingestion", extra={"file": str(file), "instrument": instrument})

    t_total = time.perf_counter()
    init_duckdb()

    t_read = time.perf_counter()
    raw = _read_raw_tick(file)
    df = standardize_tick_csv(raw)
    ms_read = int((time.perf_counter() - t_read) * 1000)

    rows_in_file = len(df)
    rows_matched = 0
    derive_result: Dict = {}
    ms_upsert = ms_derive = 0

    if rows_in_file > 0:
        win_start, win_end = df["timestamp"].min(), df["timestamp"].max()
        rows_matched = _count_existing_ticks(instrument, win_start, win_end)

    with write_transaction():
        t_upsert = time.perf_counter()
        rows_inserted = upsert_tick_data(df, instrument)
        ms_upsert = int((time.perf_counter() - t_upsert) * 1000)

        if derive and not df.empty:
            t_derive = time.perf_counter()
            derive_result = derive_ohlc_from_ticks(instrument, df["timestamp"].min(), df["timestamp"].max())
            ms_derive = int((time.perf_counter() - t_derive) * 1000)

    after = _count_existing_ticks(instrument, df["timestamp"].min(), df["timestamp"].max()) if rows_in_file > 0 else 0
    rows_new = max(0, after - rows_matched)

    logger.info("Tick file ingestion completed", extra={
        "file": str(file),
        "instrument": instrument,
        "rows_in_file": rows_in_file,
        "rows_new": rows_new,
        "rows_matched": max(0, rows_inserted - rows_new),
        "derived_targets": derive_result,
        "timing_ms": {
            "read": ms_read,
            "upsert": ms_upsert,
            "derive": ms_derive,
            "total": int((time.perf_counter() - t_total) * 1000),
        },
    })
    return rows_inserted


def ingest_tick_dataframe(df: pd.DataFrame, instrument: str, derive: bool = True) -> int:
    """Ingest a tick DataFrame directly into DuckDB."""
    instrument = validate_instrument(instrument)
    init_duckdb()

    if "timestamp" not in df.columns:
        raise ValueError("DataFrame must have a 'timestamp' column")

    standardized = standardize_tick_csv(df.copy())

    with write_transaction():
        rows = upsert_tick_data(standardized, instrument)
        if derive and not standardized.empty:
            derive_ohlc_from_ticks(instrument, standardized["timestamp"].min(), standardized["timestamp"].max())

    return rows
