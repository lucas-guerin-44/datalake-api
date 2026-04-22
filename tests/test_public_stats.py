"""
Tests for /public/stats — the aggregate-only landing-page endpoint.

Exercises the compute + cache helpers directly so we don't depend on the full
FastAPI app (which drags in a Postgres engine that's awkward to stub
consistently across the full test suite — see test_stream.py for the dance
required when you do need the app).
"""
import os
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ALLOW_PUBLIC_READS", "true")

# src.routes.__init__ pulls in every sibling router, some of which load
# src.core.database → SQLAlchemy Postgres driver. Stub the driver so collection
# works in envs without psycopg2 installed. Matches tests/test_stream.py.
sys.modules.setdefault("psycopg2", MagicMock())

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import datalake
from src.routes import public as public_route
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
    public_route._cache["value"] = None
    public_route._cache["expires_at"] = 0.0
    yield db_path


def _sample_bars(n: int = 5) -> pd.DataFrame:
    ts = pd.date_range("2024-01-02 09:00:00", periods=n, freq="1min", tz="UTC")
    return pd.DataFrame({
        "timestamp": ts,
        "open": [1.0] * n,
        "high": [1.1] * n,
        "low": [0.9] * n,
        "close": [1.05] * n,
        "volume": [100] * n,
    })


def _call(monkeypatch=None):
    """
    Drive the cache logic the same way the endpoint does, without going through
    the ASGI stack. Mirrors public_route.public_stats but skips slowapi.
    """
    now = time.monotonic()
    if (
        public_route._cache["value"] is not None
        and now < public_route._cache["expires_at"]
    ):
        return public_route._cache["value"]
    value = public_route._compute_stats()
    public_route._cache["value"] = value
    public_route._cache["expires_at"] = now + public_route._CACHE_TTL_SECONDS
    return value


def test_empty_datalake_returns_zeros():
    body = _call()
    assert body["ohlc_rows"] == 0
    assert body["tick_rows"] == 0
    assert body["total_rows"] == 0
    assert body["instruments"] == 0
    assert body["timeframes"] == []
    assert body["date_range"] == {"start": None, "end": None}
    assert "last_refresh" in body
    assert body["cache_ttl_seconds"] == 60


def test_counts_ohlc_rows_and_instruments():
    ingest_dataframe(_sample_bars(5), "XAUUSD", "M1", derive=False)
    ingest_dataframe(_sample_bars(3), "EURUSD", "M1", derive=False)

    body = _call()
    assert body["ohlc_rows"] == 8
    assert body["instruments"] == 2
    assert "M1" in body["timeframes"]
    assert body["total_rows"] == 8
    assert body["date_range"]["start"] is not None


def test_cache_hits_skip_recomputation(monkeypatch):
    ingest_dataframe(_sample_bars(5), "XAUUSD", "M1", derive=False)
    calls = {"n": 0}
    original = public_route._compute_stats

    def counting():
        calls["n"] += 1
        return original()

    monkeypatch.setattr(public_route, "_compute_stats", counting)

    _call()
    _call()
    _call()

    assert calls["n"] == 1


def test_cache_expiry_forces_recomputation(monkeypatch):
    ingest_dataframe(_sample_bars(5), "XAUUSD", "M1", derive=False)
    calls = {"n": 0}
    original = public_route._compute_stats

    def counting():
        calls["n"] += 1
        return original()

    monkeypatch.setattr(public_route, "_compute_stats", counting)

    _call()
    public_route._cache["expires_at"] = 0.0  # force expiry
    _call()

    assert calls["n"] == 2


def test_tick_timeframe_appears_when_ticks_present():
    # Seed a tick row directly — pipeline's tick ingest would also trigger
    # OHLC derivation, which we don't need here.
    with datalake.get_db_connection() as con:
        con.execute("""
            INSERT INTO tick_data VALUES
            ('XAUUSD', '2024-01-01 09:00:00', 2645.32, 1.0, 2645.30, 2645.34)
        """)

    body = _call()
    assert body["tick_rows"] == 1
    assert "TICK" in body["timeframes"]
