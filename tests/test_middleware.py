"""
Tests for middleware: request logging, correlation IDs, query-param redaction.
"""
import os
import sys
import logging
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

os.environ.setdefault("RATE_LIMIT_ENABLED", "false")
os.environ.setdefault("ALLOW_PUBLIC_READS", "false")
os.environ.setdefault("DERIVATION_WORKER_AUTOSTART", "false")

sys.modules.setdefault("psycopg2", MagicMock())
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.middleware.middleware import _redact_query_params, _REDACTED_QUERY_KEYS
from src.middleware.logging_config import (
    correlation_id_var,
    set_correlation_id,
    get_correlation_id,
    clear_correlation_id,
    CustomJsonFormatter,
)


# ── _redact_query_params ─────────────────────────────────────────────────────


class TestRedactQueryParams:
    def test_redacts_known_keys(self):
        params = {"token": "secret123", "api_key": "dk_abc", "password": "hunter2", "q": "search"}
        result = _redact_query_params(params)
        assert result["token"] == "***"
        assert result["api_key"] == "***"
        assert result["password"] == "***"
        assert result["q"] == "search"

    def test_redaction_is_case_insensitive(self):
        params = {"Token": "s", "API_KEY": "s", "Password": "s"}
        result = _redact_query_params(params)
        assert result["Token"] == "***"
        assert result["API_KEY"] == "***"
        assert result["Password"] == "***"

    def test_redacts_access_token(self):
        params = {"access_token": "jwt_value"}
        result = _redact_query_params(params)
        assert result["access_token"] == "***"

    def test_redacts_apikey_variant(self):
        params = {"apikey": "key_value"}
        result = _redact_query_params(params)
        assert result["apikey"] == "***"

    def test_empty_params(self):
        assert _redact_query_params({}) == {}

    def test_no_sensitive_keys(self):
        params = {"instrument": "XAUUSD", "timeframe": "M1"}
        result = _redact_query_params(params)
        assert result == params

    def test_does_not_mutate_original(self):
        original = {"token": "secret", "q": "test"}
        _redact_query_params(original)
        assert original["token"] == "secret"  # unchanged


# ── Correlation ID lifecycle ─────────────────────────────────────────────────


class TestCorrelationId:
    def test_set_and_get(self):
        set_correlation_id("test-123")
        assert get_correlation_id() == "test-123"
        clear_correlation_id()

    def test_clear(self):
        set_correlation_id("test-456")
        clear_correlation_id()
        assert get_correlation_id() is None

    def test_default_is_none(self):
        clear_correlation_id()
        assert correlation_id_var.get() is None


# ── RequestLoggingMiddleware ──────────────────────────────────────────────────


@pytest.fixture()
def app():
    """Minimal FastAPI app with the logging middleware."""
    _app = FastAPI()

    @_app.get("/ok")
    def ok():
        return {"status": "ok"}

    @_app.get("/error")
    def error():
        raise ValueError("boom")

    from src.middleware.middleware import RequestLoggingMiddleware
    _app.add_middleware(RequestLoggingMiddleware)
    return _app


@pytest.fixture()
def client(app):
    return TestClient(app, raise_server_exceptions=False)


class TestRequestLoggingMiddleware:
    def test_correlation_id_in_response_headers(self, client):
        resp = client.get("/ok")
        assert resp.status_code == 200
        assert "X-Correlation-ID" in resp.headers
        # UUID4 format
        cid = resp.headers["X-Correlation-ID"]
        assert len(cid) == 36
        assert cid.count("-") == 4

    def test_different_requests_get_different_ids(self, client):
        r1 = client.get("/ok")
        r2 = client.get("/ok")
        assert r1.headers["X-Correlation-ID"] != r2.headers["X-Correlation-ID"]

    def test_logs_request_and_response(self, client, caplog):
        with caplog.at_level(logging.INFO, logger="src.middleware.middleware"):
            resp = client.get("/ok")
        assert resp.status_code == 200

        messages = [r.message for r in caplog.records]
        assert "Incoming request" in messages
        assert "Request completed" in messages

    def test_logs_error_on_exception(self, client, caplog):
        with caplog.at_level(logging.ERROR, logger="src.middleware.middleware"):
            resp = client.get("/error")
        assert resp.status_code == 500

        error_messages = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_messages) >= 1
        assert "Request failed" in error_messages[0].message

    def test_redacted_params_not_in_logs(self, client, caplog):
        with caplog.at_level(logging.INFO, logger="src.middleware.middleware"):
            client.get("/ok?token=SECRET&api_key=dk_abc&q=search")

        # Check only our middleware log records (not httpx or other loggers)
        middleware_records = [r for r in caplog.records if r.name == "src.middleware.middleware"]
        log_text = json.dumps([{"msg": r.message, **r.__dict__} for r in middleware_records])
        # The actual secret values should NOT appear in middleware logs
        assert "SECRET" not in log_text
        assert "dk_abc" not in log_text

    def test_middleware_cleans_up_correlation_id(self, client):
        """After request, correlation_id_var should be cleared."""
        client.get("/ok")
        # The var should be None after the request completes
        assert get_correlation_id() is None


# ── CustomJsonFormatter ──────────────────────────────────────────────────────


class TestCustomJsonFormatter:
    def test_adds_standard_fields(self):
        formatter = CustomJsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=1, msg="hello", args=(), exc_info=None,
        )
        # funcName is a property computed from code object; set it explicitly for direct calls
        record.funcName = "test_adds_standard_fields"
        log_record = {}
        formatter.add_fields(log_record, record, {})

        assert log_record["level"] == "INFO"
        assert log_record["logger"] == "test"
        assert "timestamp" in log_record

    def test_includes_correlation_id_when_set(self):
        set_correlation_id("ctx-789")
        formatter = CustomJsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=1, msg="hello", args=(), exc_info=None,
        )
        log_record = {}
        formatter.add_fields(log_record, record, {})

        assert log_record["correlation_id"] == "ctx-789"
        clear_correlation_id()

    def test_no_correlation_id_when_unset(self):
        clear_correlation_id()
        formatter = CustomJsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=1, msg="hello", args=(), exc_info=None,
        )
        log_record = {}
        formatter.add_fields(log_record, record, {})

        assert "correlation_id" not in log_record
