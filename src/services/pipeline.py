"""
Data ingestion pipeline for OHLC data.
Reads CSV/Excel files and inserts data into DuckDB.
"""
from pathlib import Path
from typing import Optional, Dict

import pandas as pd

from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.core.datalake import upsert_ohlc_data, init_duckdb

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


def ingest_single_file(file: Path, instrument: str, timeframe: str) -> int:
    """
    Ingest a single CSV/Excel file into DuckDB.

    Returns the number of rows inserted.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    logger.info("Starting file ingestion", extra={"file": str(file), "instrument": instrument, "timeframe": timeframe})

    init_duckdb()
    raw = _read_raw(file)
    df = _standardize(raw)
    rows_inserted = upsert_ohlc_data(df, instrument, timeframe)

    logger.info("File ingestion completed", extra={"file": str(file), "instrument": instrument, "timeframe": timeframe, "rows_inserted": rows_inserted})
    return rows_inserted


def ingest_dataframe(df: pd.DataFrame, instrument: str, timeframe: str) -> int:
    """
    Ingest a DataFrame directly into DuckDB.
    Must have a 'timestamp' column plus open/high/low/close.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)
    init_duckdb()

    if "timestamp" not in df.columns:
        raise ValueError("DataFrame must have a 'timestamp' column")

    return upsert_ohlc_data(_standardize(df.copy()), instrument, timeframe)
