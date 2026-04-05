"""
Pagination utilities for cursor-based pagination.

Provides cursor encoding/decoding for efficient pagination through large datasets.
"""
import base64
import json
from typing import Optional

from fastapi import HTTPException


def encode_cursor(timestamp: str, instrument: Optional[str] = None, timeframe: Optional[str] = None) -> str:
    """
    Encode pagination cursor from query position.

    The cursor contains the last timestamp seen plus optional filters
    to ensure the cursor is only valid for the same query context.

    Args:
        timestamp: The last timestamp from the current page
        instrument: Optional instrument filter for context validation
        timeframe: Optional timeframe filter for context validation

    Returns:
        URL-safe base64 encoded cursor string
    """
    cursor_data = {"ts": timestamp}
    if instrument:
        cursor_data["i"] = instrument
    if timeframe:
        cursor_data["tf"] = timeframe
    return base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode()


def decode_cursor(cursor: str, instrument: Optional[str] = None, timeframe: Optional[str] = None) -> str:
    """
    Decode pagination cursor to get the last timestamp.

    Validates that the cursor matches the current query context.

    Args:
        cursor: The cursor string from the client
        instrument: Expected instrument filter (must match cursor)
        timeframe: Expected timeframe filter (must match cursor)

    Returns:
        The timestamp to continue from (exclusive)

    Raises:
        HTTPException: If cursor is invalid or mismatched with query context
    """
    try:
        cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        timestamp = cursor_data.get("ts")
        if not timestamp:
            raise ValueError("Missing timestamp in cursor")

        # Validate cursor matches query context
        cursor_instrument = cursor_data.get("i")
        cursor_timeframe = cursor_data.get("tf")

        if cursor_instrument != instrument or cursor_timeframe != timeframe:
            raise HTTPException(
                status_code=400,
                detail="Cursor does not match query parameters. Do not change instrument/timeframe when paginating."
            )

        return timestamp
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid cursor: {str(e)}")
