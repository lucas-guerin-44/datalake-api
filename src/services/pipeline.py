"""
Data ingestion pipeline for OHLC and tick data.
Reads CSV/Excel files and inserts data into DuckDB.
"""
import os
import time
from pathlib import Path
from typing import Optional, Dict

import pandas as pd

from src.config import PROJECT_ROOT
from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.core.datalake import (
    init_duckdb,
    write_transaction,
)
from src.core.writes import (
    upsert_ohlc_data,
    upsert_tick_data,
)
from src.core.derive import (
    derive_ohlc_timeframes,
    derive_ohlc_from_ticks,
)
from src.core.derivation_queue import enqueue as enqueue_derivation, notify as notify_worker

logger = get_logger(__name__)

DEFAULT_STAGING = PROJECT_ROOT / "staging"

REQUIRED_COLS = ["timestamp", "open", "high", "low", "close"]


def _sniff_first_line(path: Path) -> str:
    """Read the first line of a file for format detection."""
    with open(path, "rb") as f:
        return f.readline().decode(errors="ignore")


def _read_csv_auto(path: Path) -> pd.DataFrame:
    """Read CSV with encoding fallback."""
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-16")


def _read_raw(path: Path) -> pd.DataFrame:
    """Read a raw CSV/Excel file, auto-detecting MetaTrader export format."""
    first_line = _sniff_first_line(path)

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
        return _read_csv_auto(path)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def _lowercase_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip and lowercase all column names."""
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _compute_mid_price(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mid price from bid/ask if price column is missing."""
    if "price" not in df.columns and "bid" in df.columns and "ask" in df.columns:
        df["price"] = (df["bid"] + df["ask"]) / 2
    return df


def _validate_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce timestamp column to UTC and raise on invalid values."""
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if df["timestamp"].isna().any():
        bad = df["timestamp"].isna().sum()
        raise ValueError(f"Found {bad} rows with invalid timestamps")
    return df


def _standardize(df: pd.DataFrame, column_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Normalize column names, validate required columns, and coerce types."""
    _lowercase_columns(df)

    if column_map:
        df = df.rename(columns=column_map)

    # Drop common non-OHLC columns from broker exports
    for c in ["tickvol", "vol", "spread", "volume"]:
        if c in df.columns:
            df = df.drop(columns=c)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after mapping: {missing}")

    _validate_timestamps(df)

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


# --- Generic ingest helpers ---------------------------------------------------


def _ingest_file(
    file: Path, instrument: str, timeframe: Optional[str],
    read_fn, standardize_fn, upsert_fn, derive_fn,
    derive: bool = True, log_prefix: str = "File",
) -> int:
    """Generic ingest: read -> standardize -> write_transaction(upsert + derive) -> log."""
    instrument = validate_instrument(instrument)
    if timeframe is not None:
        timeframe = validate_timeframe(timeframe)

    logger.info("Starting %s ingestion", log_prefix.lower(), extra={"file": str(file), "instrument": instrument, "timeframe": timeframe})

    t_total = time.perf_counter()
    init_duckdb()

    t_read = time.perf_counter()
    df = standardize_fn(read_fn(file))
    ms_read = int((time.perf_counter() - t_read) * 1000)

    rows_in_file = len(df)
    derive_result: Dict = {}
    ms_upsert = ms_derive = 0

    with write_transaction():
        t_upsert = time.perf_counter()
        rows_inserted = upsert_fn(df, instrument, timeframe) if timeframe is not None else upsert_fn(df, instrument)
        ms_upsert = int((time.perf_counter() - t_upsert) * 1000)

        if derive and not df.empty:
            t_derive = time.perf_counter()
            derive_result = derive_fn(instrument, timeframe, df["timestamp"].min(), df["timestamp"].max()) if timeframe is not None else derive_fn(instrument, df["timestamp"].min(), df["timestamp"].max())
            ms_derive = int((time.perf_counter() - t_derive) * 1000)

    logger.info("%s ingestion completed", log_prefix, extra={
        "file": str(file),
        "instrument": instrument,
        "timeframe": timeframe,
        "rows_in_file": rows_in_file,
        "rows_inserted": rows_inserted,
        "derived_targets": derive_result,
        "timing_ms": {
            "read": ms_read,
            "upsert": ms_upsert,
            "derive": ms_derive,
            "total": int((time.perf_counter() - t_total) * 1000),
        },
    })
    return rows_inserted


def _ingest_file_queued(
    file: Path, instrument: str, timeframe: Optional[str],
    read_fn, standardize_fn, upsert_fn, source_kind: str,
    log_prefix: str = "File",
) -> Dict:
    """Generic queued ingest: read -> write_transaction(upsert + enqueue) -> log."""
    instrument = validate_instrument(instrument)
    if timeframe is not None:
        timeframe = validate_timeframe(timeframe)

    t_total = time.perf_counter()
    init_duckdb()
    df = standardize_fn(read_fn(file))

    queue_id = None
    window = None
    with write_transaction() as con:
        rows_inserted = upsert_fn(df, instrument, timeframe) if timeframe is not None else upsert_fn(df, instrument)
        if not df.empty:
            window = (df["timestamp"].min(), df["timestamp"].max())
            queue_id = enqueue_derivation(con, instrument, source_kind, timeframe, window[0], window[1])

    if queue_id is not None:
        notify_worker()

    logger.info("%s ingested (derivation queued)", log_prefix, extra={
        "file": str(file), "instrument": instrument, "timeframe": timeframe,
        "rows_in_file": len(df), "queue_id": queue_id,
        "timing_ms": {"total": int((time.perf_counter() - t_total) * 1000)},
    })
    return {"rows_inserted": rows_inserted, "queue_id": queue_id, "window": window}


# --- OHLC ingest functions ----------------------------------------------------


def ingest_single_file(file: Path, instrument: str, timeframe: str, derive: bool = True) -> int:
    """Ingest a single CSV/Excel file into DuckDB. Returns the number of rows inserted."""
    return _ingest_file(
        file, instrument, timeframe,
        read_fn=_read_raw, standardize_fn=_standardize,
        upsert_fn=upsert_ohlc_data,
        derive_fn=derive_ohlc_timeframes, derive=derive, log_prefix="OHLC file",
    )


def ingest_single_file_queued(file: Path, instrument: str, timeframe: str) -> Dict:
    """Ingest a CSV/Excel file with derivation queued off the request path."""
    return _ingest_file_queued(
        file, instrument, timeframe,
        read_fn=_read_raw, standardize_fn=_standardize,
        upsert_fn=upsert_ohlc_data, source_kind="ohlc", log_prefix="OHLC file",
    )


def ingest_tick_file_queued(file: Path, instrument: str) -> Dict:
    """Tick counterpart of ingest_single_file_queued."""
    return _ingest_file_queued(
        file, instrument, None,
        read_fn=_read_raw_tick, standardize_fn=standardize_tick_csv,
        upsert_fn=upsert_tick_data, source_kind="ticks", log_prefix="Tick file",
    )


# --- DataFrame ingest (no file I/O) -------------------------------------------


def _ingest_dataframe(
    df: pd.DataFrame, instrument: str, timeframe: Optional[str],
    standardize_fn, upsert_fn, derive_fn, source_kind: str,
    derive: bool = True,
) -> int:
    """Generic DataFrame ingest: validate -> standardize -> write_transaction."""
    instrument = validate_instrument(instrument)
    if timeframe is not None:
        timeframe = validate_timeframe(timeframe)
    init_duckdb()

    if "timestamp" not in df.columns:
        raise ValueError("DataFrame must have a 'timestamp' column")

    standardized = standardize_fn(df.copy())

    with write_transaction():
        rows = upsert_fn(standardized, instrument, timeframe) if timeframe is not None else upsert_fn(standardized, instrument)
        if derive and not standardized.empty:
            if timeframe is not None:
                derive_fn(instrument, timeframe, standardized["timestamp"].min(), standardized["timestamp"].max())
            else:
                derive_fn(instrument, standardized["timestamp"].min(), standardized["timestamp"].max())

    return rows


def ingest_dataframe(df: pd.DataFrame, instrument: str, timeframe: str, derive: bool = True) -> int:
    """Ingest an OHLC DataFrame directly into DuckDB."""
    return _ingest_dataframe(
        df, instrument, timeframe,
        standardize_fn=_standardize, upsert_fn=upsert_ohlc_data,
        derive_fn=derive_ohlc_timeframes, source_kind="ohlc", derive=derive,
    )


def ingest_tick_dataframe(df: pd.DataFrame, instrument: str, derive: bool = True) -> int:
    """Ingest a tick DataFrame directly into DuckDB."""
    return _ingest_dataframe(
        df, instrument, None,
        standardize_fn=standardize_tick_csv, upsert_fn=upsert_tick_data,
        derive_fn=derive_ohlc_from_ticks, source_kind="ticks", derive=derive,
    )


TICK_REQUIRED_COLS = ["timestamp", "price"]


def _read_raw_tick(path: Path) -> pd.DataFrame:
    """Read a raw tick CSV file, auto-detecting MetaTrader and Dukascopy formats."""
    first_line = _sniff_first_line(path)

    # MetaTrader tick export: <DATE> <TIME> <BID> <ASK> <LAST> <VOLUME>
    if "<DATE>" in first_line and ("<BID>" in first_line or "<LAST>" in first_line):
        df = pd.read_csv(path, sep=r"\s+", engine="python")
        # MetaTrader <DATE>/<TIME> columns are in BROKER-LOCAL time, not UTC.
        # Localize as the broker tz (default Europe/Athens for Eightcap-style
        # GMT+3/+2 servers) then convert to real UTC. Override via env var.
        # See quant-strategies-research/docs/RESEARCH_NOTES.md lesson #80.
        _broker_tz = os.getenv("BROKER_TZ", os.getenv("MT5_BROKER_TZ", "Europe/Athens"))
        if "<TIME>" in df.columns:
            _naive = pd.to_datetime(df["<DATE>"] + " " + df["<TIME>"])
        else:
            _naive = pd.to_datetime(df["<DATE>"])
        try:
            _localized = _naive.dt.tz_localize(
                _broker_tz, ambiguous="infer", nonexistent="shift_forward",
            )
        except Exception:
            _localized = _naive.dt.tz_localize(
                _broker_tz, ambiguous="NaT", nonexistent="NaT",
            )
        df["timestamp"] = _localized.dt.tz_convert("UTC")
        # Drop DST-ambiguous / non-existent rows (typically 1-2 per year).
        df = df.dropna(subset=["timestamp"]).reset_index(drop=True)

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

        _compute_mid_price(df)

        keep = [c for c in df.columns if c in ["timestamp", "price", "volume", "bid", "ask"]]
        return df[keep]

    # Dukascopy format: Gmt time,Bid,Ask,Volume
    lower_first = first_line.lower()
    if "gmt time" in lower_first or ("bid" in lower_first and "ask" in lower_first and "open" not in lower_first):
        df = _read_csv_auto(path)
        _lowercase_columns(df)
        rename = {}
        for col in df.columns:
            if "gmt" in col or "time" in col:
                rename[col] = "timestamp"
        df = df.rename(columns=rename)

        _compute_mid_price(df)

        keep = [c for c in df.columns if c in ["timestamp", "price", "volume", "bid", "ask"]]
        return df[keep]

    # Generic tick CSV: timestamp, price, volume (optional bid, ask)
    df = _read_csv_auto(path)
    _lowercase_columns(df)
    return df


def standardize_tick_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize tick data: validate columns, compute mid price if needed, clean up."""
    _lowercase_columns(df)

    _compute_mid_price(df)
    if "price" not in df.columns:
        raise ValueError("Missing 'price' column and cannot compute from bid/ask")

    missing = [c for c in TICK_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after mapping: {missing}")

    _validate_timestamps(df)

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
    return _ingest_file(
        file, instrument, None,
        read_fn=_read_raw_tick, standardize_fn=standardize_tick_csv,
        upsert_fn=upsert_tick_data,
        derive_fn=derive_ohlc_from_ticks, derive=derive, log_prefix="Tick file",
    )


