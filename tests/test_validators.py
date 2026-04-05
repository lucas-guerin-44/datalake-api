"""
Tests for input validation and security functions.
"""
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.validators import (
    validate_instrument,
    validate_timeframe,
    MAX_INSTRUMENT_LENGTH,
    MAX_TIMEFRAME_LENGTH,
)


class TestValidateInstrument:

    @pytest.mark.parametrize("instrument", [
        "EURUSD", "GBPUSD", "XAU_USD", "S&P-500", "BTC_USDT", "EUR123", "abc", "A",
    ])
    def test_valid_instruments(self, instrument):
        assert validate_instrument(instrument) == instrument

    def test_empty_instrument_raises(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_instrument("")
        assert exc_info.value.status_code == 400
        assert "required" in exc_info.value.detail.lower()

    def test_none_instrument_raises(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_instrument(None)
        assert exc_info.value.status_code == 400

    def test_max_length_instrument_passes(self):
        assert validate_instrument("A" * MAX_INSTRUMENT_LENGTH) == "A" * MAX_INSTRUMENT_LENGTH

    def test_exceeds_max_length_raises(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_instrument("A" * (MAX_INSTRUMENT_LENGTH + 1))
        assert exc_info.value.status_code == 400
        assert "maximum length" in exc_info.value.detail.lower()

    @pytest.mark.parametrize("malicious_input", [
        "../../../etc/passwd", "..\\..\\..\\windows\\system32",
        "EURUSD/../secret", "EURUSD/../../data", "..EURUSD",
        "EURUSD..", "EUR..USD", "EUR/USD", "EUR\\USD", "./EURUSD", ".\\EURUSD",
    ])
    def test_path_traversal_attacks_blocked(self, malicious_input):
        with pytest.raises(HTTPException) as exc_info:
            validate_instrument(malicious_input)
        assert exc_info.value.status_code == 400
        assert "path traversal" in exc_info.value.detail.lower()

    @pytest.mark.parametrize("invalid_input", [
        "EUR USD", "EUR\tUSD", "EUR\nUSD", "EUR;USD", "EUR'USD",
        'EUR"USD', "EUR<USD", "EUR>USD", "EUR|USD", "EUR*USD",
        "EUR?USD", "EUR:USD", "EUR`USD", "EUR$USD", "EUR!USD",
        "EUR@USD", "EUR#USD", "EUR%USD", "EUR^USD", "EUR(USD)",
        "EUR[USD]", "EUR{USD}", "EUR=USD", "EUR+USD",
    ])
    def test_invalid_characters_blocked(self, invalid_input):
        with pytest.raises(HTTPException) as exc_info:
            validate_instrument(invalid_input)
        assert exc_info.value.status_code == 400


class TestValidateTimeframe:

    @pytest.mark.parametrize("timeframe,expected", [
        ("M1", "M1"), ("M5", "M5"), ("M15", "M15"), ("M30", "M30"),
        ("H1", "H1"), ("H4", "H4"), ("H12", "H12"),
        ("D1", "D1"), ("W1", "W1"), ("MN1", "MN1"),
        ("m1", "M1"), ("h1", "H1"), ("d1", "D1"), ("w1", "W1"), ("mn1", "MN1"),
    ])
    def test_valid_timeframes_standard(self, timeframe, expected):
        assert validate_timeframe(timeframe) == expected

    @pytest.mark.parametrize("timeframe,expected", [
        ("1m", "1M"), ("5m", "5M"), ("15m", "15M"), ("30m", "30M"),
        ("1h", "1H"), ("4h", "4H"), ("12h", "12H"),
        ("1d", "1D"), ("1w", "1W"),
        ("1M", "1M"), ("5M", "5M"), ("1H", "1H"), ("4H", "4H"), ("1D", "1D"),
    ])
    def test_valid_timeframes_alternative(self, timeframe, expected):
        assert validate_timeframe(timeframe) == expected

    def test_empty_timeframe_raises(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_timeframe("")
        assert exc_info.value.status_code == 400

    def test_none_timeframe_raises(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_timeframe(None)
        assert exc_info.value.status_code == 400

    def test_exceeds_max_length_raises(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_timeframe("M" + "1" * MAX_TIMEFRAME_LENGTH)
        assert exc_info.value.status_code == 400

    @pytest.mark.parametrize("malicious_input", [
        "../M1", "M1/..", "M1/../H1", "..\\M1", "M1\\H1",
    ])
    def test_path_traversal_attacks_blocked(self, malicious_input):
        with pytest.raises(HTTPException) as exc_info:
            validate_timeframe(malicious_input)
        assert exc_info.value.status_code == 400

    @pytest.mark.parametrize("invalid_input", [
        "minute1", "HOUR1", "1", "M", "T1", "S1", "Y1", "MM1", "MN", "M-1", "M 1",
    ])
    def test_invalid_format_blocked(self, invalid_input):
        with pytest.raises(HTTPException) as exc_info:
            validate_timeframe(invalid_input)
        assert exc_info.value.status_code == 400


class TestSQLInjectionPrevention:

    @pytest.mark.parametrize("malicious_input", [
        "EURUSD'; DROP TABLE users;--",
        "EURUSD' OR '1'='1",
        "EURUSD; DELETE FROM partitions;",
        "EURUSD' UNION SELECT * FROM users--",
        "EURUSD\"; DROP TABLE--",
        "1; DROP TABLE users",
    ])
    def test_sql_injection_in_instrument_blocked(self, malicious_input):
        with pytest.raises(HTTPException):
            validate_instrument(malicious_input)

    @pytest.mark.parametrize("malicious_input", [
        "M1'; DROP TABLE--",
        "M1' OR '1'='1",
    ])
    def test_sql_injection_in_timeframe_blocked(self, malicious_input):
        with pytest.raises(HTTPException):
            validate_timeframe(malicious_input)
