"""Catalog routes - view database statistics and data coverage."""
from typing import Optional

from fastapi import APIRouter, Depends

from src.middleware.logging_config import get_logger
from src.config import ALLOW_PUBLIC_READS
from src.core.database import User
from src.core.datalake import get_database_stats, get_tick_database_stats, list_instruments, list_timeframes, get_data_range, list_tick_instruments, get_tick_coverage
from src.auth.auth import ScopedAuth

logger = get_logger(__name__)
router = APIRouter()


@router.get("/catalog")
def get_catalog(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS))
):
    """
    Return DuckDB database statistics including available instruments,
    timeframes, and data coverage per instrument/timeframe pair.
    """
    try:
        stats = get_database_stats()
        tick_stats = get_tick_database_stats()

        coverage = []
        for instrument in list_instruments():
            for timeframe in list_timeframes(instrument):
                data_range = get_data_range(instrument, timeframe)
                if data_range:
                    coverage.append({
                        "instrument": instrument,
                        "timeframe": timeframe,
                        "min_date": str(data_range["min_date"]) if data_range["min_date"] else None,
                        "max_date": str(data_range["max_date"]) if data_range["max_date"] else None,
                        "record_count": data_range["count"],
                    })

        # Include tick coverage
        for instrument in list_tick_instruments():
            tick_cov = get_tick_coverage(instrument)
            if tick_cov:
                coverage.append({
                    "instrument": instrument,
                    "timeframe": "TICK",
                    "min_date": str(tick_cov["min_date"]) if tick_cov["min_date"] else None,
                    "max_date": str(tick_cov["max_date"]) if tick_cov["max_date"] else None,
                    "record_count": tick_cov["count"],
                })

        return {"status": "ok", "database": stats, "tick_database": tick_stats, "coverage": coverage}
    except Exception as e:
        logger.error("Failed to retrieve catalog", exc_info=True)
        return {"status": "error", "message": str(e)}


@router.get("/catalog/stats")
def get_stats(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS))
):
    """Return quick database statistics."""
    try:
        return get_database_stats()
    except Exception as e:
        logger.error("Failed to retrieve stats", exc_info=True)
        return {"status": "error", "message": str(e)}
