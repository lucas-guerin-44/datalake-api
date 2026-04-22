"""
Tests for `find_gaps` / `/catalog/gaps` — locates holes in OHLC time series.
"""
import os


import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import datalake
from src.services.pipeline import ingest_dataframe


@pytest.fixture(autouse=True)
def use_temp_duckdb(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    if datalake._db_connection is not None:
        try:
            datalake._db_connection.close()
        except Exception:
            pass
    monkeypatch.setattr(datalake, "_db_connection", None)
    monkeypatch.setattr(datalake, "DUCKDB_PATH", db_path)
    datalake.init_duckdb()
    yield db_path


def _bars_with_gap(gap_minutes: int = 30) -> pd.DataFrame:
    """10 M1 bars, a gap of `gap_minutes`, then 10 more M1 bars."""
    first = pd.date_range("2024-01-02 09:00:00", periods=10, freq="1min", tz="UTC")
    second_start = first[-1] + pd.Timedelta(minutes=gap_minutes + 1)
    second = pd.date_range(second_start, periods=10, freq="1min", tz="UTC")
    ts = first.append(second)
    return pd.DataFrame({
        "timestamp": ts,
        "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.1,
    })


class TestFindGaps:

    def test_detects_a_gap(self):
        ingest_dataframe(_bars_with_gap(30), "XAUUSD", "M1", derive=False)

        gaps = datalake.find_gaps("XAUUSD", "M1")
        assert len(gaps) == 1
        assert gaps[0]["duration_seconds"] == 30 * 60 + 60  # 31 minutes
        assert gaps[0]["missing_bars"] == 30

    def test_no_gaps_when_series_contiguous(self):
        ts = pd.date_range("2024-01-02 09:00:00", periods=60, freq="1min", tz="UTC")
        df = pd.DataFrame({
            "timestamp": ts, "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.1,
        })
        ingest_dataframe(df, "XAUUSD", "M1", derive=False)

        assert datalake.find_gaps("XAUUSD", "M1") == []

    def test_threshold_filters_small_gaps(self):
        # A 3-minute gap (180s). Default threshold is 2*60=120s, so it reports.
        ingest_dataframe(_bars_with_gap(2), "XAUUSD", "M1", derive=False)
        assert len(datalake.find_gaps("XAUUSD", "M1")) == 1

        # Raise the threshold above 3 minutes — nothing should report.
        assert datalake.find_gaps("XAUUSD", "M1", min_gap_seconds=10 * 60) == []

    def test_unknown_timeframe_returns_empty(self):
        # W1 doesn't have a derivation second-count; gap semantics aren't defined.
        assert datalake.find_gaps("XAUUSD", "W1") == []

    def test_flags_weekend_like_gap(self):
        # Friday 21:00 UTC → Sunday 22:00 UTC ≈ typical FX weekend closure (~49 hours).
        first = pd.date_range("2024-01-05 20:58:00", periods=2, freq="1min", tz="UTC")  # Fri
        second = pd.date_range("2024-01-07 22:00:00", periods=2, freq="1min", tz="UTC")  # Sun
        ts = first.append(second)
        df = pd.DataFrame({
            "timestamp": ts, "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.1,
        })
        ingest_dataframe(df, "XAUUSD", "M1", derive=False)

        gaps = datalake.find_gaps("XAUUSD", "M1")
        assert len(gaps) == 1
        assert gaps[0]["is_weekend"] is True
