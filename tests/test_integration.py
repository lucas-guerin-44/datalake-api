"""
End-to-end integration test: ingest -> query -> download -> instruments -> catalog.

Boots the real FastAPI app against temp DuckDB + SQLite, exercises the main
data-flow path through HTTP. ALLOW_PUBLIC_READS defaults to false in conftest.
"""
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

os.environ.setdefault("RATE_LIMIT_ENABLED", "false")
os.environ.setdefault("DERIVATION_WORKER_AUTOSTART", "false")

sys.modules.setdefault("psycopg2", MagicMock())
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient

from src.core import datalake, cache
from src.core.database import Base, User, APIKey
from src.auth.auth import generate_api_key, hash_api_key


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def setup_sqlite(monkeypatch):
    """Patch both DuckDB (temp file) and Postgres (in-memory SQLite)."""
    # DuckDB: temp file
    if datalake._db_connection is not None:
        try:
            datalake._db_connection.close()
        except Exception:
            pass
    monkeypatch.setattr(datalake, "_db_connection", None)

    # Postgres: in-memory SQLite
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    import src.core.database as db_mod
    monkeypatch.setattr(db_mod, "engine", engine)
    monkeypatch.setattr(db_mod, "SessionLocal", TestSession)

    yield engine

    Base.metadata.drop_all(bind=engine)
    cache.clear()


@pytest.fixture()
def seeded_app(setup_sqlite, tmp_path, monkeypatch):
    """Create an admin user + key, init DuckDB, return (client, api_key)."""
    engine = setup_sqlite
    TestSession = sessionmaker(bind=engine)()

    user = User(username="admin", email="admin@test.com", hashed_password="x", is_active=True)
    TestSession.add(user)
    TestSession.commit()
    TestSession.refresh(user)

    full_key, prefix = generate_api_key()
    TestSession.add(APIKey(
        user_id=user.id, key_hash=hash_api_key(full_key), prefix=prefix,
        name="integration-key", scopes=["admin"], is_active=True,
    ))
    TestSession.commit()
    TestSession.close()

    # Init DuckDB in a temp dir
    db_path = tmp_path / "integration.duckdb"
    monkeypatch.setattr(datalake, "DUCKDB_PATH", db_path)
    datalake.init_duckdb()

    from src.api import app
    with patch("src.api.init_db"):
        client = TestClient(app, raise_server_exceptions=False)

    return client, full_key


@pytest.fixture()
def sample_csv(tmp_path):
    """Write a small OHLC CSV file and return its path."""
    ts = pd.date_range("2024-06-01 00:00", periods=60, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.1,
    })
    path = tmp_path / "XAUUSD_M1_202406010000_202406010100.csv"
    df.to_csv(path, index=False)
    return path


# ── Tests ────────────────────────────────────────────────────────────────────


class TestFullIngestQueryDownloadFlow:
    def test_ingest_then_query(self, seeded_app, sample_csv):
        client, api_key = seeded_app
        headers = {"X-API-Key": api_key}

        # 1. Ingest the CSV file
        with open(sample_csv, "rb") as f:
            resp = client.post(
                "/ingest",
                files={"file": ("XAUUSD_M1_202406010000_202406010100.csv", f, "text/csv")},
                data={"instrument": "XAUUSD", "timeframe": "M1", "derive": "false"},
                headers=headers,
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["instrument"] == "XAUUSD"
        assert body["timeframe"] == "M1"

        # 2. Query the ingested data
        resp = client.get("/query?instrument=XAUUSD&timeframe=M1&limit=10", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["data"]) == 10
        assert data["pagination"]["count"] == 10
        assert data["pagination"]["has_more"] is True

        # 3. Verify the first row has valid OHLC values
        first = data["data"][0]
        assert first["instrument"] == "XAUUSD"
        assert first["timeframe"] == "M1"
        assert "timestamp" in first
        assert first["open"] is not None

    def test_ingest_then_download_csv(self, seeded_app, sample_csv):
        client, api_key = seeded_app
        headers = {"X-API-Key": api_key}

        # Ingest
        with open(sample_csv, "rb") as f:
            client.post(
                "/ingest",
                files={"file": ("XAUUSD_M1_202406010000_202406010100.csv", f, "text/csv")},
                data={"instrument": "XAUUSD", "timeframe": "M1", "derive": "false"},
                headers=headers,
            )

        # Download as CSV
        resp = client.get("/download?instrument=XAUUSD&timeframe=M1", headers=headers)
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "text/csv; charset=utf-8"
        assert "attachment" in resp.headers.get("content-disposition", "")

        # Parse the CSV and verify row count
        csv_text = resp.text
        lines = [l for l in csv_text.strip().split("\n") if l]
        # Header + 60 data rows
        assert len(lines) == 61

    def test_instruments_endpoint(self, seeded_app, sample_csv):
        client, api_key = seeded_app
        headers = {"X-API-Key": api_key}

        # Before ingest: empty
        resp = client.get("/instruments", headers=headers)
        assert resp.status_code == 200
        assert resp.json()["instruments"] == []

        # Ingest
        with open(sample_csv, "rb") as f:
            client.post(
                "/ingest",
                files={"file": ("XAUUSD_M1_202406010000_202406010100.csv", f, "text/csv")},
                data={"instrument": "XAUUSD", "timeframe": "M1", "derive": "false"},
                headers=headers,
            )

        # After ingest: XAUUSD appears
        resp = client.get("/instruments", headers=headers)
        assert resp.status_code == 200
        assert "XAUUSD" in resp.json()["instruments"]

    def test_catalog_stats(self, seeded_app, sample_csv):
        client, api_key = seeded_app
        headers = {"X-API-Key": api_key}

        # Ingest
        with open(sample_csv, "rb") as f:
            client.post(
                "/ingest",
                files={"file": ("XAUUSD_M1_202406010000_202406010100.csv", f, "text/csv")},
                data={"instrument": "XAUUSD", "timeframe": "M1", "derive": "false"},
                headers=headers,
            )

        # Catalog stats
        resp = client.get("/catalog/stats", headers=headers)
        assert resp.status_code == 200
        stats = resp.json()
        assert stats["total_rows"] >= 60
        instruments = [i["instrument"] for i in stats.get("instruments", [])]
        assert "XAUUSD" in instruments

    def test_auth_required_for_write(self, seeded_app, sample_csv):
        client, _ = seeded_app
        # Ingest requires write scope — no key -> 401
        with open(sample_csv, "rb") as f:
            resp = client.post(
                "/ingest",
                files={"file": ("test.csv", f, "text/csv")},
                data={"instrument": "XAUUSD", "timeframe": "M1"},
            )
        assert resp.status_code == 401

    def test_wrong_scope_rejected(self, seeded_app, setup_sqlite):
        client, _ = seeded_app

        # Create a read-only key
        engine = setup_sqlite
        session = sessionmaker(bind=engine)()
        user = session.query(User).first()
        full_key, prefix = generate_api_key()
        session.add(APIKey(
            user_id=user.id, key_hash=hash_api_key(full_key), prefix=prefix,
            name="read-only", scopes=["read"], is_active=True,
        ))
        session.commit()
        session.close()

        # Read-only key can query but not ingest
        resp = client.get("/query?instrument=XAUUSD&timeframe=M1", headers={"X-API-Key": full_key})
        assert resp.status_code == 200  # read is allowed

        resp = client.post(
            "/ingest",
            files={"file": ("test.csv", b"dummy", "text/csv")},
            data={"instrument": "XAUUSD", "timeframe": "M1"},
            headers={"X-API-Key": full_key},
        )
        assert resp.status_code == 403  # write not allowed
