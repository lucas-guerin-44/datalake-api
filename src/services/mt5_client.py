"""
Client for the Wine-hosted MT5 bridge.

The FastAPI container (Linux) cannot call the MetaTrader5 Python package directly -
it is Windows-only. Instead we run a tiny JSON-over-HTTP server inside Wine
(see scripts/mt5_bridge.py) that proxies to MetaTrader5 and exposes:

    POST /bars   {"symbol": "XAUUSD", "start": "...", "end": "...", "timeframe": "M1"}
    GET  /ping

This client talks to that bridge using stdlib only (no extra deps).
"""
import json
import os
from datetime import datetime, timezone
from typing import Optional
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError

import pandas as pd

from src.middleware.logging_config import get_logger

logger = get_logger(__name__)

MT5_BRIDGE_URL = os.getenv("MT5_BRIDGE_URL", "http://host.docker.internal:18812").rstrip("/")
MT5_BRIDGE_TIMEOUT = int(os.getenv("MT5_BRIDGE_TIMEOUT", "120"))


class MT5BridgeError(RuntimeError):
    """Raised when the Wine bridge is unreachable or returns an error."""


def _post(path: str, payload: dict) -> dict:
    url = f"{MT5_BRIDGE_URL}{path}"
    body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlrequest.urlopen(req, timeout=MT5_BRIDGE_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        raise MT5BridgeError(f"Bridge {path} returned {e.code}: {detail}") from e
    except URLError as e:
        raise MT5BridgeError(f"Bridge {path} unreachable at {url}: {e.reason}") from e


def ping() -> bool:
    """Return True if the bridge is reachable."""
    try:
        with urlrequest.urlopen(f"{MT5_BRIDGE_URL}/ping", timeout=5) as resp:
            return resp.status == 200
    except (URLError, HTTPError):
        return False


def fetch_m1_bars(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch M1 OHLC bars from MT5 for [start, end] (inclusive, UTC).
    Returns a DataFrame with columns: timestamp, open, high, low, close.
    """
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    payload = {
        "symbol": symbol,
        "timeframe": "M1",
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    data = _post("/bars", payload)

    bars = data.get("bars", [])
    if not bars:
        logger.info("MT5 bridge returned no bars", extra={"symbol": symbol, "start": str(start), "end": str(end)})
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close"])

    df = pd.DataFrame(bars)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df[["timestamp", "open", "high", "low", "close"]]
