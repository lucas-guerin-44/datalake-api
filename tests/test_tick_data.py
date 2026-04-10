"""
Tests for tick data ingestion, storage, querying, and pipeline functions.
"""
import os
import tempfile

os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing")

import sys
from pathlib import Path

import pandas as pd
import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.pipeline import standardize_tick_csv, _read_raw_tick
from src.services.validators import validate_timeframe
from src.core import datalake


@pytest.fixture(autouse=True)
def use_temp_duckdb(tmp_path, monkeypatch):
    """Use a temporary DuckDB file for each test, resetting the shared connection."""
    db_path = tmp_path / "test.duckdb"
    # Close and reset the shared connection so it reconnects to the temp path
    if datalake._db_connection is not None:
        try:
            datalake._db_connection.close()
        except Exception:
            pass
    monkeypatch.setattr(datalake, "_db_connection", None)
    monkeypatch.setattr(datalake, "DUCKDB_PATH", db_path)
    datalake.init_duckdb()
    yield db_path


class TestTickTimeframeValidation:

    def test_tick_is_valid_timeframe(self):
        assert validate_timeframe("TICK") == "TICK"

    def test_tick_case_insensitive(self):
        assert validate_timeframe("tick") == "TICK"
        assert validate_timeframe("Tick") == "TICK"


class TestStandardizeTickCSV:

    def test_basic_tick_data(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:00", "2024-01-01 09:00:01"],
            "price": [2645.32, 2645.50],
            "volume": [1.0, 2.0],
        })
        result = standardize_tick_csv(df)
        assert len(result) == 2
        assert list(result.columns) >= ["timestamp", "price", "volume"]
        assert result["price"].iloc[0] == 2645.32

    def test_bid_ask_computes_mid_price(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:00"],
            "bid": [2645.30],
            "ask": [2645.34],
            "volume": [1.0],
        })
        result = standardize_tick_csv(df)
        assert "price" in result.columns
        assert result["price"].iloc[0] == pytest.approx(2645.32)

    def test_missing_price_and_no_bid_ask_raises(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:00"],
            "volume": [1.0],
        })
        with pytest.raises(ValueError, match="price"):
            standardize_tick_csv(df)

    def test_missing_timestamp_raises(self):
        df = pd.DataFrame({
            "price": [2645.32],
        })
        with pytest.raises(ValueError, match="timestamp"):
            standardize_tick_csv(df)

    def test_invalid_timestamp_raises(self):
        df = pd.DataFrame({
            "timestamp": ["not-a-date"],
            "price": [2645.32],
        })
        with pytest.raises(ValueError, match="invalid timestamps"):
            standardize_tick_csv(df)

    def test_nan_price_raises(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:00"],
            "price": [float("nan")],
        })
        with pytest.raises(ValueError, match="NaN or non-positive"):
            standardize_tick_csv(df)

    def test_negative_price_raises(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:00"],
            "price": [-100.0],
        })
        with pytest.raises(ValueError, match="NaN or non-positive"):
            standardize_tick_csv(df)

    def test_zero_price_raises(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:00"],
            "price": [0.0],
        })
        with pytest.raises(ValueError, match="NaN or non-positive"):
            standardize_tick_csv(df)

    def test_drops_duplicates_and_sorts(self):
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:01", "2024-01-01 09:00:00", "2024-01-01 09:00:01"],
            "price": [100.0, 99.0, 100.0],
        })
        result = standardize_tick_csv(df)
        assert len(result) == 2
        assert result["price"].iloc[0] == 99.0  # sorted by timestamp

    def test_column_names_normalized_to_lowercase(self):
        df = pd.DataFrame({
            "Timestamp": ["2024-01-01 09:00:00"],
            "Price": [2645.32],
            "Volume": [1.0],
        })
        result = standardize_tick_csv(df)
        assert "price" in result.columns
        assert "timestamp" in result.columns


class TestReadRawTick:

    def test_generic_csv(self, tmp_path):
        csv_path = tmp_path / "ticks.csv"
        csv_path.write_text("timestamp,price,volume\n2024-01-01 09:00:00,2645.32,1.0\n")
        df = _read_raw_tick(csv_path)
        assert "timestamp" in df.columns
        assert "price" in df.columns

    def test_dukascopy_format(self, tmp_path):
        csv_path = tmp_path / "ticks.csv"
        csv_path.write_text("Gmt time,Bid,Ask,Volume\n2024-01-01 09:00:00,2645.30,2645.34,1.0\n")
        df = _read_raw_tick(csv_path)
        assert "timestamp" in df.columns
        assert "price" in df.columns
        assert "bid" in df.columns
        assert "ask" in df.columns

    def test_metatrader_tick_format(self, tmp_path):
        csv_path = tmp_path / "ticks.csv"
        csv_path.write_text("<DATE> <TIME> <BID> <ASK> <LAST> <VOLUME>\n2024.01.01 09:00:00 2645.30 2645.34 2645.32 1.0\n")
        df = _read_raw_tick(csv_path)
        assert "timestamp" in df.columns
        assert "price" in df.columns


class TestUpsertTickData:

    def test_basic_upsert(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00", "2024-01-01 09:00:01"]),
            "price": [2645.32, 2645.50],
            "volume": [1.0, 2.0],
        })
        rows = datalake.upsert_tick_data(df, "XAUUSD")
        assert rows == 2

    def test_upsert_with_bid_ask(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00"]),
            "price": [2645.32],
            "volume": [1.0],
            "bid": [2645.30],
            "ask": [2645.34],
        })
        rows = datalake.upsert_tick_data(df, "XAUUSD")
        assert rows == 1

    def test_upsert_empty_df_returns_zero(self):
        df = pd.DataFrame(columns=["timestamp", "price"])
        assert datalake.upsert_tick_data(df, "XAUUSD") == 0

    def test_upsert_missing_price_raises(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00"]),
            "volume": [1.0],
        })
        with pytest.raises(ValueError, match="Missing required columns"):
            datalake.upsert_tick_data(df, "XAUUSD")

    def test_upsert_replaces_on_duplicate(self):
        df1 = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00"]),
            "price": [100.0],
        })
        df2 = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00"]),
            "price": [200.0],
        })
        datalake.upsert_tick_data(df1, "XAUUSD")
        datalake.upsert_tick_data(df2, "XAUUSD")

        with datalake.get_db_connection() as con:
            result = con.execute("SELECT price FROM tick_data WHERE instrument = 'XAUUSD'").fetchall()
        assert len(result) == 1
        assert result[0][0] == 200.0


class TestListTickInstruments:

    def test_empty_returns_empty(self):
        assert datalake.list_tick_instruments() == []

    def test_returns_instruments_with_data(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00"]),
            "price": [2645.32],
        })
        datalake.upsert_tick_data(df, "XAUUSD")
        datalake.upsert_tick_data(df, "EURUSD")
        instruments = datalake.list_tick_instruments()
        assert "XAUUSD" in instruments
        assert "EURUSD" in instruments


class TestGetTickCoverage:

    def test_no_data_returns_none(self):
        assert datalake.get_tick_coverage("XAUUSD") is None

    def test_returns_coverage(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00", "2024-06-15 12:00:00"]),
            "price": [2645.32, 2700.00],
        })
        datalake.upsert_tick_data(df, "XAUUSD")
        cov = datalake.get_tick_coverage("XAUUSD")
        assert cov is not None
        assert cov["count"] == 2
        assert cov["min_date"] is not None
        assert cov["max_date"] is not None


class TestGetTickDatabaseStats:

    def test_empty_stats(self):
        stats = datalake.get_tick_database_stats()
        assert stats["total_ticks"] == 0
        assert stats["instruments"] == []

    def test_stats_with_data(self):
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 09:00:00", "2024-01-01 09:00:01"]),
            "price": [2645.32, 2645.50],
        })
        datalake.upsert_tick_data(df, "XAUUSD")
        stats = datalake.get_tick_database_stats()
        assert stats["total_ticks"] == 2
        assert len(stats["instruments"]) == 1
        assert stats["instruments"][0]["instrument"] == "XAUUSD"


class TestTickPipeline:

    def test_ingest_tick_file(self, tmp_path):
        from src.services.pipeline import ingest_tick_file

        csv_path = tmp_path / "XAUUSD_TICK_test.csv"
        csv_path.write_text(
            "timestamp,price,volume\n"
            "2024-01-01 09:00:00,2645.32,1.0\n"
            "2024-01-01 09:00:01,2645.50,2.0\n"
        )
        rows = ingest_tick_file(csv_path, "XAUUSD")
        assert rows == 2

        # Verify data was stored
        cov = datalake.get_tick_coverage("XAUUSD")
        assert cov["count"] == 2

    def test_ingest_tick_dataframe(self):
        from src.services.pipeline import ingest_tick_dataframe

        df = pd.DataFrame({
            "timestamp": ["2024-01-01 09:00:00", "2024-01-01 09:00:01"],
            "price": [2645.32, 2645.50],
            "volume": [1.0, 2.0],
        })
        rows = ingest_tick_dataframe(df, "XAUUSD")
        assert rows == 2
