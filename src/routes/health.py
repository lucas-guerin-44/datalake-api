"""Health routes - liveness and readiness probes."""
from fastapi import APIRouter, Response, status
from sqlalchemy import text

from src.core.database import SessionLocal
from src.core.datalake import duckdb_ready
from src.middleware.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/healthcheck")
def healthcheck():
    """Liveness probe - returns 200 if the process is up. No dependency checks."""
    return {"status": "healthy"}


@router.get("/healthcheck/ready")
def readiness(response: Response):
    """
    Readiness probe — "the process is up and can serve", NOT "the engine is idle".

    Deliberately cheap and non-contending: it never runs a DuckDB query, so a heavy
    in-flight `/query` (or a full-history pull) can't make this probe queue and
    time out into a false 502 for whatever cron is pointed at it. The DuckDB check
    is an in-memory "is the database open" flag; Postgres uses its own pooled
    connection, separate from the datalake query path. See datalake-api-1rh.
    """
    checks = {"postgres": "ok", "duckdb": "ok"}
    healthy = True

    try:
        db = SessionLocal()
        try:
            db.execute(text("SELECT 1"))
        finally:
            db.close()
    except Exception as e:
        checks["postgres"] = f"error: {e.__class__.__name__}"
        healthy = False
        logger.warning("Readiness check failed: postgres unreachable", exc_info=True)

    if not duckdb_ready():
        checks["duckdb"] = "error: not_initialized"
        healthy = False
        logger.warning("Readiness check failed: duckdb not initialized")

    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {"status": "ready" if healthy else "unready", "checks": checks}
