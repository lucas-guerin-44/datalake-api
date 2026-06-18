"""
Tests for the durable, single-writer derivation queue (datalake-api-k0s).

Covers:
  * queued ingest commits raw bars AND a pending task in one transaction
  * derived timeframes are NOT present until the queue is drained (async)
  * drain()/process_one() derive the higher TFs and mark the task done
  * wait_for() processes a specific task and returns its final status
  * pending tasks survive a DB reopen (crash recovery) and re-derive
  * a persistently failing derive parks as 'error' after MAX_ATTEMPTS
  * ticks enqueue and derive OHLC bars
"""
import os

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ALLOW_PUBLIC_READS", "true")
os.environ.setdefault("DERIVATION_WORKER_AUTOSTART", "false")

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import datalake, derivation_queue
from src.core.derive import derive_ohlc_timeframes
from src.services.pipeline import ingest_single_file_queued, ingest_tick_file_queued

datalake.derive_ohlc_timeframes = derive_ohlc_timeframes


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
    # A prior route test's shutdown may have set the worker stop flag; clear it so
    # drain() actually runs here.
    derivation_queue._stop_event.clear()
    datalake.init_duckdb()
    yield db_path


def _write_m1_csv(path: Path, start: str, count: int, base: float = 100.0) -> Path:
    ts = pd.date_range(start=start, periods=count, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "open": base, "high": base + 1.0, "low": base - 1.0, "close": base,
    })
    df["close"] = base + df.index * 0.01  # vary so aggregates differ
    df.to_csv(path, index=False)
    return path


def _write_tick_csv(path: Path, start: str, count: int) -> Path:
    ts = pd.date_range(start=start, periods=count, freq="1s", tz="UTC")
    df = pd.DataFrame({"timestamp": ts, "price": [100.0 + i * 0.001 for i in range(count)]})
    df.to_csv(path, index=False)
    return path


def _timeframes(instrument: str) -> set:
    with datalake.read_connection() as con:
        return {r[0] for r in con.execute(
            "SELECT DISTINCT timeframe FROM ohlc_data WHERE instrument = ?", [instrument]
        ).fetchall()}


def test_queued_ingest_commits_raw_and_enqueues_without_deriving(tmp_path):
    csv = _write_m1_csv(tmp_path / "XAUUSD_M1.csv", "2024-01-02 00:00:00", 120)
    res = ingest_single_file_queued(csv, "XAUUSD", "M1")

    assert res["rows_inserted"] == 120
    assert res["queue_id"] is not None

    # Raw bars are immediately visible...
    with datalake.read_connection() as con:
        raw = con.execute(
            "SELECT COUNT(*) FROM ohlc_data WHERE instrument='XAUUSD' AND timeframe='M1'"
        ).fetchone()[0]
    assert raw == 120

    # ...but derived TFs are not — derivation is queued, not inline.
    assert _timeframes("XAUUSD") == {"M1"}
    stats = derivation_queue.queue_stats()
    assert stats["pending"] == 1
    assert stats["done"] == 0


def test_drain_derives_higher_timeframes_and_marks_done(tmp_path):
    csv = _write_m1_csv(tmp_path / "XAUUSD_M1.csv", "2024-01-02 00:00:00", 120)
    ingest_single_file_queued(csv, "XAUUSD", "M1")

    processed = derivation_queue.drain()
    assert processed == 1

    assert {"M5", "M15", "M30", "H1"}.issubset(_timeframes("XAUUSD"))
    stats = derivation_queue.queue_stats()
    assert stats["pending"] == 0
    assert stats["done"] == 1
    # Nothing left to do.
    assert derivation_queue.process_one() is False


def test_wait_for_returns_done(tmp_path):
    csv = _write_m1_csv(tmp_path / "XAUUSD_M1.csv", "2024-01-02 00:00:00", 60)
    res = ingest_single_file_queued(csv, "XAUUSD", "M1")

    final = derivation_queue.wait_for(res["queue_id"])
    assert final == "done"
    assert "H1" in _timeframes("XAUUSD")


def test_pending_task_survives_db_reopen(tmp_path, monkeypatch):
    """Crash recovery: a pending task persists across a process restart."""
    csv = _write_m1_csv(tmp_path / "XAUUSD_M1.csv", "2024-01-02 00:00:00", 120)
    ingest_single_file_queued(csv, "XAUUSD", "M1")
    assert derivation_queue.queue_stats()["pending"] == 1

    # Simulate a restart: drop and reopen the DuckDB instance on the same file.
    datalake._db_connection.close()
    monkeypatch.setattr(datalake, "_db_connection", None)
    datalake.init_duckdb()

    assert derivation_queue.queue_stats()["pending"] == 1  # survived the reopen
    derivation_queue.drain()
    assert "H1" in _timeframes("XAUUSD")
    assert derivation_queue.queue_stats()["done"] == 1


def test_failed_derivation_parks_as_error_after_max_attempts(tmp_path, monkeypatch):
    csv = _write_m1_csv(tmp_path / "XAUUSD_M1.csv", "2024-01-02 00:00:00", 120)
    ingest_single_file_queued(csv, "XAUUSD", "M1")

    monkeypatch.setattr(derivation_queue, "MAX_ATTEMPTS", 3)

    def boom(*a, **k):
        raise RuntimeError("derive boom")

    # process_one imports derive_ohlc_timeframes from src.core.derive, so patch there.
    import src.core.derive as _derive_mod
    monkeypatch.setattr(_derive_mod, "derive_ohlc_timeframes", boom)

    derivation_queue.drain()

    stats = derivation_queue.queue_stats()
    assert stats["error"] == 1
    assert stats["pending"] == 0
    # The raw bars are untouched by the failed derive.
    assert _timeframes("XAUUSD") == {"M1"}


def test_ticks_enqueue_and_derive(tmp_path):
    csv = _write_tick_csv(tmp_path / "XAUUSD_TICK.csv", "2024-01-02 00:00:00", 600)  # 10 min
    res = ingest_tick_file_queued(csv, "XAUUSD")
    assert res["queue_id"] is not None

    # Ticks landed; OHLC not yet.
    assert _timeframes("XAUUSD") == set()
    derivation_queue.drain()

    tfs = _timeframes("XAUUSD")
    assert "M1" in tfs and "M5" in tfs
    assert derivation_queue.queue_stats()["done"] == 1
