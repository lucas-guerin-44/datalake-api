"""
Tests for the read-concurrency / backpressure work (handoff: stop false 502s and
serve more than one request at a time).

Covers:
  * read_connection() — independent cursors, MVCC snapshot, reads don't block on writes
  * duckdb_ready() / readiness probe decoupled from the query path
  * data-version counter — bumps on commit/delete, not on reads or rollback
  * versioned cache — caches by data version, self-invalidates on write
  * query_slot() backpressure — 503 + Retry-After when saturated
"""
import os

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ALLOW_PUBLIC_READS", "true")

import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

# src.routes.__init__ pulls in routers that import the Postgres driver; stub it so
# collection works without psycopg2 (matches the other route tests).
sys.modules.setdefault("psycopg2", MagicMock())

import pandas as pd
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import datalake, cache, concurrency
from src.services.pipeline import ingest_dataframe


@pytest.fixture
def client():
    from src.api import app
    with patch("src.api.init_db"):
        # raise_server_exceptions=False so we can observe the JSON 500 envelope
        # instead of the exception propagating out of the test client.
        return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def use_temp_duckdb(tmp_path, monkeypatch):
    """Fresh temp DuckDB + clean read cache per test."""
    if datalake._db_connection is not None:
        try:
            datalake._db_connection.close()
        except Exception:
            pass
    monkeypatch.setattr(datalake, "_db_connection", None)
    monkeypatch.setattr(datalake, "DUCKDB_PATH", db_path := tmp_path / "test.duckdb")
    datalake.init_duckdb()
    cache.clear()
    yield db_path


def _sample_bars(n: int = 5) -> pd.DataFrame:
    ts = pd.date_range("2024-01-02 09:00:00", periods=n, freq="1min", tz="UTC")
    return pd.DataFrame({
        "timestamp": ts,
        "open": [1.0] * n, "high": [1.1] * n, "low": [0.9] * n, "close": [1.05] * n,
    })


# --- read_connection ---------------------------------------------------------

class TestReadConnection:
    def test_yields_independent_cursor_that_sees_committed_data(self):
        ingest_dataframe(_sample_bars(3), "XAUUSD", "M1", derive=False)
        with datalake.read_connection() as con:
            n = con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0]
        assert n == 3

    def test_two_read_connections_are_distinct_objects(self):
        with datalake.read_connection() as a, datalake.read_connection() as b:
            assert a is not b  # separate cursors -> no head-of-line blocking

    def test_read_does_not_block_on_in_flight_write(self):
        """A read must not wait on the write lock — that serialization was the 502."""
        ingest_dataframe(_sample_bars(2), "XAUUSD", "M1", derive=False)

        write_holding = threading.Event()
        release_write = threading.Event()

        def slow_writer():
            with datalake.write_transaction() as con:
                con.execute(
                    "INSERT INTO ohlc_data VALUES "
                    "('XAUUSD','M1','2024-02-01 00:00:00',1,1,1,1,'raw')"
                )
                write_holding.set()
                release_write.wait(timeout=5)  # hold the open transaction

        t = threading.Thread(target=slow_writer)
        t.start()
        assert write_holding.wait(timeout=5)

        # Read while the write transaction is open and uncommitted.
        start = time.perf_counter()
        with datalake.read_connection() as con:
            count = con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0]
        elapsed = time.perf_counter() - start

        assert elapsed < 1.0, "read blocked behind the open write transaction"
        assert count == 2, "read should see the pre-write MVCC snapshot, not the uncommitted row"

        release_write.set()
        t.join(timeout=5)

        with datalake.read_connection() as con:
            assert con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0] == 3


# --- readiness ----------------------------------------------------------------

class TestReadiness:
    def test_duckdb_ready_reflects_connection_state(self, monkeypatch):
        assert datalake.duckdb_ready() is True
        monkeypatch.setattr(datalake, "_db_connection", None)
        assert datalake.duckdb_ready() is False

    def test_ready_probe_does_not_query_duckdb(self):
        """Readiness must not touch the query path — guard against a regression."""
        with patch.object(datalake, "read_connection") as rc, \
             patch.object(datalake, "get_db_connection") as gc:
            assert datalake.duckdb_ready() is True
            rc.assert_not_called()
            gc.assert_not_called()


# --- data version + cache -----------------------------------------------------

class TestDataVersion:
    def test_commit_bumps_version(self):
        before = datalake.get_data_version()
        ingest_dataframe(_sample_bars(2), "XAUUSD", "M1", derive=False)
        assert datalake.get_data_version() > before

    def test_reads_do_not_bump_version(self):
        ingest_dataframe(_sample_bars(2), "XAUUSD", "M1", derive=False)
        v = datalake.get_data_version()
        datalake.list_instruments()
        datalake.get_database_stats()
        assert datalake.get_data_version() == v

    def test_rollback_does_not_bump_version(self):
        before = datalake.get_data_version()
        with pytest.raises(RuntimeError):
            with datalake.write_transaction() as con:
                con.execute(
                    "INSERT INTO ohlc_data VALUES "
                    "('XAUUSD','M1','2024-03-01 00:00:00',1,1,1,1,'raw')"
                )
                raise RuntimeError("boom")
        assert datalake.get_data_version() == before

    def test_delete_bumps_version(self):
        ingest_dataframe(_sample_bars(2), "XAUUSD", "M1", derive=False)
        v = datalake.get_data_version()
        datalake.delete_ohlc_data("XAUUSD")
        assert datalake.get_data_version() > v


class TestVersionedCache:
    def test_caches_until_version_changes(self):
        calls = {"n": 0}

        def producer():
            calls["n"] += 1
            return {"n": calls["n"]}

        assert cache.get_or_compute("k", producer) == {"n": 1}
        assert cache.get_or_compute("k", producer) == {"n": 1}  # cache hit
        assert calls["n"] == 1

        datalake.bump_data_version()
        assert cache.get_or_compute("k", producer) == {"n": 2}  # recomputed
        assert calls["n"] == 2

    def test_write_invalidates_cached_value(self):
        ingest_dataframe(_sample_bars(2), "XAUUSD", "M1", derive=False)
        first = cache.get_or_compute("instruments", lambda: list(datalake.list_instruments()))
        assert first == ["XAUUSD"]

        ingest_dataframe(_sample_bars(2), "EURUSD", "M1", derive=False)
        second = cache.get_or_compute("instruments", lambda: list(datalake.list_instruments()))
        assert second == ["EURUSD", "XAUUSD"]


# --- backpressure -------------------------------------------------------------

class TestQuerySlot:
    def test_acquires_and_releases_slot(self):
        gen = concurrency.query_slot()
        next(gen)  # acquire
        with pytest.raises(StopIteration):
            next(gen)  # release on teardown, no error

    def test_returns_503_with_retry_after_when_saturated(self, monkeypatch):
        sem = threading.BoundedSemaphore(1)
        monkeypatch.setattr(concurrency, "_query_semaphore", sem)

        held = concurrency.query_slot()
        next(held)  # take the only slot

        blocked = concurrency.query_slot()
        with pytest.raises(HTTPException) as exc:
            next(blocked)
        assert exc.value.status_code == 503
        assert exc.value.headers["Retry-After"]

        # Releasing frees the slot again.
        with pytest.raises(StopIteration):
            next(held)
        freed = concurrency.query_slot()
        next(freed)
        with pytest.raises(StopIteration):
            next(freed)


class TestWriteSlot:
    def test_acquires_and_releases_slot(self):
        gen = concurrency.write_slot()
        next(gen)  # acquire
        with pytest.raises(StopIteration):
            next(gen)  # release on teardown, no error

    def test_returns_503_with_retry_after_when_saturated(self, monkeypatch):
        sem = threading.BoundedSemaphore(1)
        monkeypatch.setattr(concurrency, "_write_semaphore", sem)

        held = concurrency.write_slot()
        next(held)  # take the only write slot

        blocked = concurrency.write_slot()
        with pytest.raises(HTTPException) as exc:
            next(blocked)
        assert exc.value.status_code == 503
        assert exc.value.headers["Retry-After"] == str(concurrency.WRITE_RETRY_AFTER_SECONDS)

        # Releasing frees the slot again — a serial client's next ingest gets in.
        with pytest.raises(StopIteration):
            next(held)
        freed = concurrency.write_slot()
        next(freed)
        with pytest.raises(StopIteration):
            next(freed)


# --- route-level integration through the ASGI stack --------------------------

class TestRoutes:
    def test_readiness_returns_ready(self, client):
        # The Postgres leg uses its own pooled connection and is orthogonal to the
        # DuckDB decoupling under test; pin it to OK so the assertion is about the
        # DuckDB leg regardless of how the shared SQLAlchemy engine got built across
        # the suite (a MagicMock-stubbed psycopg2 otherwise trips it).
        import src.routes.health as health_route
        with patch.object(health_route, "SessionLocal") as SL:
            SL.return_value.execute.return_value = None
            r = client.get("/healthcheck/ready")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ready"
        assert body["checks"]["duckdb"] == "ok"

    def test_query_returns_envelope_when_empty(self, client):
        r = client.get("/query?instrument=XAUUSD&timeframe=M1")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["pagination"]["count"] == 0

    def test_query_returns_503_with_retry_after_when_saturated(self, client, monkeypatch):
        sem = threading.BoundedSemaphore(1)
        monkeypatch.setattr(concurrency, "_query_semaphore", sem)
        assert sem.acquire(blocking=False)  # drain the only slot

        r = client.get("/query?instrument=XAUUSD&timeframe=M1")
        assert r.status_code == 503
        assert r.headers["retry-after"] == str(concurrency.RETRY_AFTER_SECONDS)

    def test_catalog_is_cached_between_calls(self, client, monkeypatch):
        ingest_dataframe(_sample_bars(3), "XAUUSD", "M1", derive=False)
        calls = {"n": 0}
        import src.routes.catalog as catalog_route
        original = catalog_route._build_catalog

        def counting():
            calls["n"] += 1
            return original()

        monkeypatch.setattr(catalog_route, "_build_catalog", counting)

        assert client.get("/catalog").status_code == 200
        assert client.get("/catalog").status_code == 200
        assert calls["n"] == 1  # second call served from cache

        ingest_dataframe(_sample_bars(2), "EURUSD", "M1", derive=False)  # bumps version
        assert client.get("/catalog").status_code == 200
        assert calls["n"] == 2  # recomputed after the write

    def test_ingest_routes_guarded_by_write_slot(self):
        """The write path must carry write_slot backpressure, not just exist."""
        from src.api import app
        from src.core.concurrency import write_slot

        def _flatten_calls(dependant):
            calls = []
            for sub in dependant.dependencies:
                if sub.call is not None:
                    calls.append(sub.call)
                calls.extend(_flatten_calls(sub))
            return calls

        def deps_of(path):
            for route in app.routes:
                if getattr(route, "path", None) == path:
                    return _flatten_calls(route.dependant)
            raise AssertionError(f"route {path} not found")

        for path in ("/ingest", "/ingest/ticks"):
            assert write_slot in deps_of(path), f"{path} missing write_slot backpressure"

    def test_unhandled_error_returns_json_envelope(self, client, monkeypatch):
        import src.routes.catalog as catalog_route

        def boom():
            raise RuntimeError("kaboom")

        monkeypatch.setattr(catalog_route, "_build_catalog", boom)

        r = client.get("/catalog")
        assert r.status_code == 500
        body = r.json()
        assert body["error"] == "internal_error"
        assert "kaboom" in body["detail"]
