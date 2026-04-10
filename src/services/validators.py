"""
Input validation to prevent path traversal and injection attacks.
"""
import re
from fastapi import HTTPException

# Instrument: alphanumeric, underscores, hyphens, ampersands (e.g., EURUSD, XAU_USD, S&P-500)
INSTRUMENT_PATTERN = re.compile(r'^[A-Za-z0-9_\-&]+$')

# Timeframe: standard MT5 formats (M1, H1, D1, W1, MN1) and alternatives (1m, 1h, 1d)
TIMEFRAME_PATTERN = re.compile(r'^(M|H|D|W|MN)\d+$', re.IGNORECASE)
TIMEFRAME_ALT_PATTERN = re.compile(r'^\d+(m|h|d|w|M|H|D|W)$')

VALID_SPECIAL_TIMEFRAMES = {"TICK"}

MAX_INSTRUMENT_LENGTH = 50
MAX_TIMEFRAME_LENGTH = 10


def validate_instrument(instrument: str) -> str:
    """
    Validate instrument name. Rejects path traversal, invalid characters,
    and excessively long names.
    """
    if not instrument:
        raise HTTPException(status_code=400, detail="Instrument parameter is required")

    if len(instrument) > MAX_INSTRUMENT_LENGTH:
        raise HTTPException(status_code=400, detail=f"Instrument name exceeds maximum length of {MAX_INSTRUMENT_LENGTH}")

    if '..' in instrument or '/' in instrument or '\\' in instrument:
        raise HTTPException(status_code=400, detail="Invalid instrument name: path traversal detected")

    if not INSTRUMENT_PATTERN.match(instrument):
        raise HTTPException(
            status_code=400,
            detail="Invalid instrument name: must contain only alphanumeric characters, underscores, hyphens, and ampersands"
        )

    return instrument


def validate_timeframe(timeframe: str) -> str:
    """
    Validate timeframe format. Accepts standard formats (M1, H1, D1)
    and alternatives (1m, 1h, 1d). Returns uppercase.
    """
    if not timeframe:
        raise HTTPException(status_code=400, detail="Timeframe parameter is required")

    if len(timeframe) > MAX_TIMEFRAME_LENGTH:
        raise HTTPException(status_code=400, detail=f"Timeframe exceeds maximum length of {MAX_TIMEFRAME_LENGTH}")

    if '..' in timeframe or '/' in timeframe or '\\' in timeframe:
        raise HTTPException(status_code=400, detail="Invalid timeframe: path traversal detected")

    if timeframe.upper() in VALID_SPECIAL_TIMEFRAMES:
        return timeframe.upper()

    if not TIMEFRAME_PATTERN.match(timeframe) and not TIMEFRAME_ALT_PATTERN.match(timeframe):
        raise HTTPException(
            status_code=400,
            detail="Invalid timeframe: must be in format like M1, M5, H1, H4, D1, W1, MN1 (or 1m, 5m, 1h, etc.)"
        )

    return timeframe.upper()
