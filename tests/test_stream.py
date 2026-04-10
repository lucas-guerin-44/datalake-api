"""
Tests for WebSocket streaming endpoints (ticks and bars).
"""
import os

os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

import json
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch, MagicMock

# Stub psycopg2 before any SQLAlchemy PostgreSQL import
sys.modules.setdefault("psycopg2", MagicMock())

import pandas as pd
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import datalake


@pytest.fixture(autouse=True)
def use_temp_duckdb(tmp_path, monkeypatch):
    """Use a temporary DuckDB file for each test."""
    db_path = tmp_path / "test.duckdb"
    monkeypatch.setattr(datalake, "DUCKDB_PATH", db_path)
    datalake.init_duckdb()
    yield db_path


@pytest.fixture
def client():
    from src.api import app
    with patch("src.api.init_db"):
        return TestClient(app)


@pytest.fixture
def seed_ticks():
    """Insert sample tick data. Uses naive timestamps to avoid timezone shift issues."""
    with datalake.get_db_connection() as con:
        con.execute("""
            INSERT INTO tick_data VALUES
            ('XAUUSD', '2024-01-01 09:00:00', 2645.32, 1.0, 2645.30, 2645.34),
            ('XAUUSD', '2024-01-01 09:00:01', 2645.50, 2.0, 2645.48, 2645.52),
            ('XAUUSD', '2024-01-01 09:00:02', 2645.45, 1.5, 2645.43, 2645.47)
        """)


@pytest.fixture
def seed_bars():
    """Insert sample OHLC bar data. Uses naive timestamps to avoid timezone shift issues."""
    with datalake.get_db_connection() as con:
        con.execute("""
            INSERT INTO ohlc_data VALUES
            ('XAUUSD', 'M5', '2024-01-01 09:00:00', 2645.00, 2646.50, 2644.50, 2646.00),
            ('XAUUSD', 'M5', '2024-01-01 09:05:00', 2646.00, 2647.00, 2645.50, 2645.50),
            ('XAUUSD', 'M5', '2024-01-01 09:10:00', 2645.50, 2646.00, 2645.00, 2645.80)
        """)


class TestTickStreaming:

    def test_streams_all_ticks(self, client, seed_ticks):
        with client.websocket_connect("/ws/ticks?instrument=XAUUSD&speed=1000") as ws:
            messages = []
            while True:
                data = json.loads(ws.receive_text())
                messages.append(data)
                if data.get("done"):
                    break

        # 3 ticks + 1 done message
        assert len(messages) == 4
        assert messages[0]["price"] == 2645.32
        assert messages[1]["price"] == 2645.50
        assert messages[2]["price"] == 2645.45
        assert messages[3]["done"] is True

    def test_streams_with_time_range(self, client, seed_ticks):
        with client.websocket_connect(
            "/ws/ticks?instrument=XAUUSD&start=2024-01-01T09:00:01&end=2024-01-01T09:00:02&speed=1000"
        ) as ws:
            messages = []
            while True:
                data = json.loads(ws.receive_text())
                messages.append(data)
                if data.get("done"):
                    break

        # 2 ticks in range + done
        assert len(messages) == 3
        assert messages[0]["price"] == 2645.50

    def test_streams_includes_bid_ask(self, client, seed_ticks):
        with client.websocket_connect("/ws/ticks?instrument=XAUUSD&speed=1000") as ws:
            data = json.loads(ws.receive_text())
            assert "bid" in data
            assert "ask" in data
            assert data["bid"] == 2645.30

    def test_burst_mode_no_pacing(self, client, seed_ticks):
        """max_delay=0 delivers all messages with no sleep between them."""
        with client.websocket_connect("/ws/ticks?instrument=XAUUSD&speed=1&max_delay=0") as ws:
            messages = []
            while True:
                data = json.loads(ws.receive_text())
                messages.append(data)
                if data.get("done"):
                    break
        assert len(messages) == 4

    def test_max_delay_caps_sleep(self, client, seed_ticks):
        """max_delay=0.1 with speed=1 should still deliver quickly despite 1s timestamp gaps."""
        with client.websocket_connect("/ws/ticks?instrument=XAUUSD&speed=1&max_delay=0.01") as ws:
            messages = []
            while True:
                data = json.loads(ws.receive_text())
                messages.append(data)
                if data.get("done"):
                    break
        assert len(messages) == 4

    def test_empty_instrument_sends_done(self, client):
        with client.websocket_connect("/ws/ticks?instrument=EURUSD&speed=1000") as ws:
            data = json.loads(ws.receive_text())
            assert data["done"] is True


class TestBarStreaming:

    def test_streams_all_bars(self, client, seed_bars):
        with client.websocket_connect("/ws/bars?instrument=XAUUSD&timeframe=M5&speed=1000") as ws:
            messages = []
            while True:
                data = json.loads(ws.receive_text())
                messages.append(data)
                if data.get("done"):
                    break

        # 3 bars + done
        assert len(messages) == 4
        assert messages[0]["open"] == 2645.00
        assert messages[0]["high"] == 2646.50
        assert messages[1]["open"] == 2646.00
        assert messages[3]["done"] is True

    def test_streams_with_time_range(self, client, seed_bars):
        with client.websocket_connect(
            "/ws/bars?instrument=XAUUSD&timeframe=M5&start=2024-01-01T09:05:00&speed=1000"
        ) as ws:
            messages = []
            while True:
                data = json.loads(ws.receive_text())
                messages.append(data)
                if data.get("done"):
                    break

        # 2 bars from 09:05 onward + done
        assert len(messages) == 3
        assert messages[0]["open"] == 2646.00

    def test_burst_mode_bars(self, client, seed_bars):
        """max_delay=0 delivers all bars instantly."""
        with client.websocket_connect("/ws/bars?instrument=XAUUSD&timeframe=M5&speed=1&max_delay=0") as ws:
            messages = []
            while True:
                data = json.loads(ws.receive_text())
                messages.append(data)
                if data.get("done"):
                    break
        assert len(messages) == 4

    def test_empty_result_sends_done(self, client):
        with client.websocket_connect("/ws/bars?instrument=EURUSD&timeframe=H1&speed=1000") as ws:
            data = json.loads(ws.receive_text())
            assert data["done"] is True
