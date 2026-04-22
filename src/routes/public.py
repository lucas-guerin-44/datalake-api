"""
Unauthenticated, aggregate-only endpoints for the public landing page.

Independent of ALLOW_PUBLIC_READS on purpose: the landing page's row-count
strip should work regardless of whether the rest of the read surface is
public. Aggregates only — no row-level data, no instrument filtering, no
date params — so flipping ALLOW_PUBLIC_READS later doesn't retroactively
widen this surface.

Cached server-side so a hot-loop can't pound DuckDB.
"""
import threading
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Request

from src.core.datalake import get_db_connection
from src.middleware.logging_config import get_logger
from src.middleware.ratelimit import limiter

logger = get_logger(__name__)
router = APIRouter()

_CACHE_TTL_SECONDS = 60
_cache_lock = threading.Lock()
_cache: dict = {"expires_at": 0.0, "value": None}


def _compute_stats() -> dict:
    with get_db_connection() as con:
        ohlc_rows = con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0]
        tick_rows = con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0]
        instrument_count = con.execute(
            "SELECT COUNT(DISTINCT instrument) FROM ("
            "  SELECT instrument FROM ohlc_data"
            "  UNION SELECT instrument FROM tick_data"
            ")"
        ).fetchone()[0]
        timeframes = [
            r[0] for r in con.execute(
                "SELECT DISTINCT timeframe FROM ohlc_data ORDER BY timeframe"
            ).fetchall()
        ]
        if tick_rows > 0:
            timeframes.append("TICK")

        ohlc_range = con.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM ohlc_data"
        ).fetchone()
        tick_range = con.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM tick_data"
        ).fetchone()

    mins = [t for t in (ohlc_range[0], tick_range[0]) if t is not None]
    maxs = [t for t in (ohlc_range[1], tick_range[1]) if t is not None]
    start = min(mins).isoformat() if mins else None
    end = max(maxs).isoformat() if maxs else None

    return {
        "ohlc_rows": ohlc_rows,
        "tick_rows": tick_rows,
        "total_rows": ohlc_rows + tick_rows,
        "instruments": instrument_count,
        "timeframes": timeframes,
        "date_range": {"start": start, "end": end},
        "last_refresh": datetime.now(timezone.utc).isoformat(),
        "cache_ttl_seconds": _CACHE_TTL_SECONDS,
    }


@router.get("/public/stats")
@limiter.limit("30/minute")
def public_stats(request: Request):
    """
    Aggregate-only stats for the landing page. Cached 60s.
    No auth. Never returns row-level data.
    """
    now = time.monotonic()
    if _cache["value"] is not None and now < _cache["expires_at"]:
        return _cache["value"]

    with _cache_lock:
        if _cache["value"] is not None and time.monotonic() < _cache["expires_at"]:
            return _cache["value"]
        try:
            value = _compute_stats()
        except Exception:
            logger.exception("public_stats: failed to compute")
            if _cache["value"] is not None:
                return _cache["value"]
            raise
        _cache["value"] = value
        _cache["expires_at"] = time.monotonic() + _CACHE_TTL_SECONDS
        return value
