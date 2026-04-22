"""
Tests for automatic timeframe derivation on ingest.
"""
import os


import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import datalake
from src.services.pipeline import ingest_dataframe, ingest_tick_dataframe


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


def _m1_bars(start: str, count: int, base: float = 100.0) -> pd.DataFrame:
    """Produce `count` consecutive M1 bars starting at `start` (UTC)."""
    ts = pd.date_range(start=start, periods=count, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "open": base + 0.0,
        "high": base + 0.5,
        "low": base - 0.5,
        "close": base + 0.1,
    })
    # Vary values slightly so aggregates differ bar to bar
    df["open"] += df.index * 0.01
    df["high"] += df.index * 0.01
    df["low"] += df.index * 0.01
    df["close"] += df.index * 0.01
    return df


class TestDeriveOhlcTimeframes:

    def test_deriving_m1_produces_higher_tfs(self):
        df = _m1_bars("2024-01-02 00:00:00", count=120)  # 2 hours of M1
        ingest_dataframe(df, "XAUUSD", "M1", derive=True)

        with datalake.get_db_connection() as con:
            tfs = {r[0] for r in con.execute(
                "SELECT DISTINCT timeframe FROM ohlc_data WHERE instrument='XAUUSD'"
            ).fetchall()}

        # M1 is raw, M5/M15/M30/H1 should be derived. H4/D1 exist but are partial.
        assert "M1" in tfs
        assert {"M5", "M15", "M30", "H1"}.issubset(tfs)

    def test_derived_bars_aggregate_correctly(self):
        df = _m1_bars("2024-01-02 00:00:00", count=5)  # exactly one M5 bar
        ingest_dataframe(df, "XAUUSD", "M1", derive=True)

        with datalake.get_db_connection() as con:
            row = con.execute(
                """SELECT open, high, low, close FROM ohlc_data
                   WHERE instrument='XAUUSD' AND timeframe='M5'
                   ORDER BY timestamp LIMIT 1"""
            ).fetchone()

        # First M5 bar: open = first M1's open, close = last M1's close
        assert row[0] == pytest.approx(df["open"].iloc[0])
        assert row[1] == pytest.approx(df["high"].max())
        assert row[2] == pytest.approx(df["low"].min())
        assert row[3] == pytest.approx(df["close"].iloc[-1])

    def test_derived_bars_marked_derived(self):
        df = _m1_bars("2024-01-02 00:00:00", count=60)
        ingest_dataframe(df, "XAUUSD", "M1", derive=True)

        with datalake.get_db_connection() as con:
            sources = {r[0] for r in con.execute(
                "SELECT DISTINCT source FROM ohlc_data WHERE instrument='XAUUSD' AND timeframe='M5'"
            ).fetchall()}
        assert sources == {"derived"}

    def test_raw_bars_protected_from_derivation_clobber(self):
        # Pre-populate raw H1 with a marker value on a canonical hour boundary.
        custom = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-02 00:00:00"], utc=True),
            "open": [999.0], "high": [999.0], "low": [999.0], "close": [999.0],
        })
        ingest_dataframe(custom, "XAUUSD", "H1", derive=False)

        # Now ingest M1 covering the same hour — derivation would normally overwrite H1.
        df = _m1_bars("2024-01-02 00:00:00", count=60)
        ingest_dataframe(df, "XAUUSD", "M1", derive=True)

        # Query by value — DuckDB's TIMESTAMP column drops tz info so specific-ts
        # comparisons depend on the host tz; asserting on values avoids that quirk.
        with datalake.get_db_connection() as con:
            rows = con.execute(
                """SELECT open, source FROM ohlc_data
                   WHERE instrument='XAUUSD' AND timeframe='H1'"""
            ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == 999.0  # raw untouched
        assert rows[0][1] == "raw"

    def test_derive_disabled_produces_only_raw(self):
        df = _m1_bars("2024-01-02 00:00:00", count=10)
        ingest_dataframe(df, "XAUUSD", "M1", derive=False)

        with datalake.get_db_connection() as con:
            tfs = {r[0] for r in con.execute(
                "SELECT DISTINCT timeframe FROM ohlc_data WHERE instrument='XAUUSD'"
            ).fetchall()}

        assert tfs == {"M1"}


class TestDeriveFromTicks:

    def test_ticks_produce_m1_bars(self):
        ts = pd.date_range("2024-01-02 00:00:00", periods=120, freq="1s", tz="UTC")
        df = pd.DataFrame({"timestamp": ts, "price": [100.0 + i * 0.01 for i in range(120)]})
        ingest_tick_dataframe(df, "XAUUSD", derive=True)

        with datalake.get_db_connection() as con:
            tfs = {r[0] for r in con.execute(
                "SELECT DISTINCT timeframe FROM ohlc_data WHERE instrument='XAUUSD'"
            ).fetchall()}

        assert {"M1", "M5"}.issubset(tfs)  # 2 min of ticks → 2× M1 + one partial M5

    def test_tick_derived_values_match_tick_range(self):
        ts = pd.date_range("2024-01-02 00:00:00", periods=60, freq="1s", tz="UTC")
        prices = [100.0 + i * 0.01 for i in range(60)]
        df = pd.DataFrame({"timestamp": ts, "price": prices})
        ingest_tick_dataframe(df, "XAUUSD", derive=True)

        # 60s of ticks in a single minute → exactly one M1 bar
        with datalake.get_db_connection() as con:
            row = con.execute(
                """SELECT open, high, low, close FROM ohlc_data
                   WHERE instrument='XAUUSD' AND timeframe='M1'
                   ORDER BY timestamp LIMIT 1"""
            ).fetchone()

        assert row[0] == pytest.approx(prices[0])
        assert row[1] == pytest.approx(max(prices))
        assert row[2] == pytest.approx(min(prices))
        assert row[3] == pytest.approx(prices[-1])
