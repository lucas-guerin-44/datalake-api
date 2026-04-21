"""
Tests for catalog export / restore round trip.
"""
import json
import os

os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing")

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import datalake
from src.services.pipeline import ingest_dataframe, ingest_tick_dataframe
from src.services.backup import export_catalog, restore_catalog


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


def _seed_sample(instrument="XAUUSD"):
    ts = pd.date_range("2024-01-02 00:00:00", periods=30, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.1,
    })
    ingest_dataframe(df, instrument, "M1", derive=False)

    tick_ts = pd.date_range("2024-01-02 00:00:00", periods=30, freq="1s", tz="UTC")
    tick_df = pd.DataFrame({"timestamp": tick_ts, "price": [100.0 + i * 0.01 for i in range(30)]})
    ingest_tick_dataframe(tick_df, instrument, derive=False)


class TestCatalogExport:

    def test_export_writes_manifest(self, tmp_path):
        _seed_sample()
        out = tmp_path / "backup"
        manifest = export_catalog(out)

        assert (out / "manifest.json").exists()
        assert manifest["ohlc"]["row_count"] == 30
        assert manifest["ticks"]["row_count"] == 30
        assert manifest["schema_version"] == 1

    def test_export_writes_partitioned_parquet(self, tmp_path):
        _seed_sample()
        out = tmp_path / "backup"
        export_catalog(out)

        # Hive partition structure: ohlc/instrument=.../timeframe=.../data.parquet
        parts = list((out / "ohlc").rglob("*.parquet"))
        assert len(parts) >= 1
        path_str = str(parts[0]).replace("\\", "/")
        assert "instrument=XAUUSD" in path_str
        assert "timeframe=M1" in path_str

    def test_export_empty_db_produces_manifest(self, tmp_path):
        out = tmp_path / "backup"
        manifest = export_catalog(out)

        assert manifest["ohlc"]["row_count"] == 0
        assert manifest["ticks"]["row_count"] == 0
        assert (out / "manifest.json").exists()


class TestCatalogRoundTrip:

    def test_export_then_restore_into_empty_db(self, tmp_path, monkeypatch):
        _seed_sample()
        out = tmp_path / "backup"
        export_catalog(out)

        # Wipe the DB: reset the shared connection to a fresh file
        db2 = tmp_path / "restored.duckdb"
        datalake._db_connection.close()
        monkeypatch.setattr(datalake, "_db_connection", None)
        monkeypatch.setattr(datalake, "DUCKDB_PATH", db2)
        datalake.init_duckdb()

        with datalake.get_db_connection() as con:
            assert con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0] == 0
            assert con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0] == 0

        result = restore_catalog(out / "manifest.json")

        assert result["ohlc_rows_restored"] == 30
        assert result["tick_rows_restored"] == 30
        with datalake.get_db_connection() as con:
            assert con.execute("SELECT COUNT(*) FROM ohlc_data WHERE instrument='XAUUSD'").fetchone()[0] == 30
            assert con.execute("SELECT COUNT(*) FROM tick_data WHERE instrument='XAUUSD'").fetchone()[0] == 30

    def test_restore_is_idempotent(self, tmp_path):
        _seed_sample()
        out = tmp_path / "backup"
        export_catalog(out)

        # Restore twice — row counts should not double
        restore_catalog(out / "manifest.json")
        restore_catalog(out / "manifest.json")
        with datalake.get_db_connection() as con:
            assert con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0] == 30
            assert con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0] == 30

    def test_restore_rejects_future_schema_version(self, tmp_path):
        _seed_sample()
        out = tmp_path / "backup"
        export_catalog(out)

        manifest_path = out / "manifest.json"
        m = json.loads(manifest_path.read_text())
        m["schema_version"] = 999
        manifest_path.write_text(json.dumps(m))

        with pytest.raises(ValueError, match="schema_version"):
            restore_catalog(manifest_path)

    def test_restore_missing_manifest_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            restore_catalog(tmp_path / "nope.json")
