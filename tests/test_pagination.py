"""
Tests for cursor-based pagination in the query endpoint.

Tests cover cursor encoding/decoding, pagination flow, and edge cases.
"""
import base64
import json
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import cursor functions from the pagination module (no DB dependencies)
from src.core.pagination import encode_cursor, decode_cursor


class TestCursorEncoding:
    """Tests for cursor encoding functions."""

    def test_encode_cursor_basic(self):
        """Test basic cursor encoding with just timestamp."""
        cursor = encode_cursor("2024-01-15T10:30:00")
        decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        assert decoded["ts"] == "2024-01-15T10:30:00"

    def test_encode_cursor_with_instrument(self):
        """Test cursor encoding with instrument."""
        cursor = encode_cursor("2024-01-15T10:30:00", instrument="EURUSD")
        decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        assert decoded["ts"] == "2024-01-15T10:30:00"
        assert decoded["i"] == "EURUSD"

    def test_encode_cursor_with_timeframe(self):
        """Test cursor encoding with timeframe."""
        cursor = encode_cursor("2024-01-15T10:30:00", timeframe="H1")
        decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        assert decoded["ts"] == "2024-01-15T10:30:00"
        assert decoded["tf"] == "H1"

    def test_encode_cursor_full(self):
        """Test cursor encoding with all parameters."""
        cursor = encode_cursor("2024-01-15T10:30:00", instrument="EURUSD", timeframe="H1")
        decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        assert decoded["ts"] == "2024-01-15T10:30:00"
        assert decoded["i"] == "EURUSD"
        assert decoded["tf"] == "H1"

    def test_cursor_is_url_safe(self):
        """Test that cursor is URL-safe (no special characters)."""
        cursor = encode_cursor("2024-01-15T10:30:00+00:00", instrument="EUR/USD", timeframe="H1")
        # URL-safe base64 should not contain +, /, or =
        assert "+" not in cursor or cursor.endswith("=")  # Padding = is allowed
        assert "/" not in cursor


class TestCursorDecoding:
    """Tests for cursor decoding functions."""

    def test_decode_cursor_basic(self):
        """Test basic cursor decoding."""
        cursor = encode_cursor("2024-01-15T10:30:00")
        timestamp = decode_cursor(cursor)
        assert timestamp == "2024-01-15T10:30:00"

    def test_decode_cursor_with_matching_context(self):
        """Test cursor decoding with matching instrument/timeframe."""
        cursor = encode_cursor("2024-01-15T10:30:00", instrument="EURUSD", timeframe="H1")
        timestamp = decode_cursor(cursor, instrument="EURUSD", timeframe="H1")
        assert timestamp == "2024-01-15T10:30:00"

    def test_decode_cursor_mismatched_instrument(self):
        """Test that mismatched instrument raises error."""
        cursor = encode_cursor("2024-01-15T10:30:00", instrument="EURUSD", timeframe="H1")
        with pytest.raises(HTTPException) as exc_info:
            decode_cursor(cursor, instrument="GBPUSD", timeframe="H1")
        assert exc_info.value.status_code == 400
        assert "Cursor does not match query parameters" in exc_info.value.detail

    def test_decode_cursor_mismatched_timeframe(self):
        """Test that mismatched timeframe raises error."""
        cursor = encode_cursor("2024-01-15T10:30:00", instrument="EURUSD", timeframe="H1")
        with pytest.raises(HTTPException) as exc_info:
            decode_cursor(cursor, instrument="EURUSD", timeframe="M15")
        assert exc_info.value.status_code == 400
        assert "Cursor does not match query parameters" in exc_info.value.detail

    def test_decode_cursor_invalid_base64(self):
        """Test that invalid base64 raises error."""
        with pytest.raises(HTTPException) as exc_info:
            decode_cursor("not-valid-base64!!!")
        assert exc_info.value.status_code == 400
        assert "Invalid cursor" in exc_info.value.detail

    def test_decode_cursor_invalid_json(self):
        """Test that invalid JSON raises error."""
        invalid_cursor = base64.urlsafe_b64encode(b"not json").decode()
        with pytest.raises(HTTPException) as exc_info:
            decode_cursor(invalid_cursor)
        assert exc_info.value.status_code == 400
        assert "Invalid cursor" in exc_info.value.detail

    def test_decode_cursor_missing_timestamp(self):
        """Test that missing timestamp raises error."""
        cursor_data = json.dumps({"i": "EURUSD"}).encode()
        invalid_cursor = base64.urlsafe_b64encode(cursor_data).decode()
        with pytest.raises(HTTPException) as exc_info:
            decode_cursor(invalid_cursor)
        assert exc_info.value.status_code == 400
        assert "Invalid cursor" in exc_info.value.detail


class TestCursorRoundtrip:
    """Tests for cursor encode/decode roundtrip."""

    def test_roundtrip_basic(self):
        """Test basic roundtrip."""
        original_ts = "2024-01-15T10:30:00"
        cursor = encode_cursor(original_ts)
        decoded_ts = decode_cursor(cursor)
        assert decoded_ts == original_ts

    def test_roundtrip_with_context(self):
        """Test roundtrip with full context."""
        original_ts = "2024-06-20T23:59:59.999999"
        cursor = encode_cursor(original_ts, instrument="XAUUSD", timeframe="M5")
        decoded_ts = decode_cursor(cursor, instrument="XAUUSD", timeframe="M5")
        assert decoded_ts == original_ts

    def test_roundtrip_none_context(self):
        """Test roundtrip with None context matches None."""
        original_ts = "2024-01-01T00:00:00"
        cursor = encode_cursor(original_ts, instrument=None, timeframe=None)
        decoded_ts = decode_cursor(cursor, instrument=None, timeframe=None)
        assert decoded_ts == original_ts

    def test_cursor_context_enforcement(self):
        """Test that context from first page must match subsequent pages."""
        # Simulate first page query
        cursor = encode_cursor("2024-01-15T10:30:00", instrument="EURUSD", timeframe="H1")

        # Second page with same context - should work
        ts = decode_cursor(cursor, instrument="EURUSD", timeframe="H1")
        assert ts == "2024-01-15T10:30:00"

        # Second page with different context - should fail
        with pytest.raises(HTTPException):
            decode_cursor(cursor, instrument="GBPUSD", timeframe="H1")
