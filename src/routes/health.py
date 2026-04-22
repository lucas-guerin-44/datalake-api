"""Health routes - liveness and readiness probes."""
from fastapi import APIRouter, Response, status
from sqlalchemy import text

from src.core.database import SessionLocal
from src.core.datalake import get_db_connection
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
    Readiness probe - verifies Postgres and DuckDB are reachable.
    Returns 503 if any dependency is down (safe to point cronjob.org at this).
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

    try:
        with get_db_connection() as con:
            con.execute("SELECT 1").fetchone()
    except Exception as e:
        checks["duckdb"] = f"error: {e.__class__.__name__}"
        healthy = False
        logger.warning("Readiness check failed: duckdb unreachable", exc_info=True)

    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {"status": "ready" if healthy else "unready", "checks": checks}
