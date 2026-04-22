"""Main FastAPI application - wires up all route modules."""
from fastapi import FastAPI, Depends, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler

from src.middleware.logging_config import setup_logging, get_logger
from src.middleware.middleware import RequestLoggingMiddleware
from src.middleware.ratelimit import limiter
from src.core.database import init_db, User
from src.core.datalake import init_duckdb, _write_tx_lock
from src.config import validate_secrets
from src.auth.auth import ScopedAuth

SHUTDOWN_WRITE_WAIT_SECONDS = 25.0
from src.routes import (
    catalog_router,
    instruments_router,
    query_router,
    ingest_router,
    auth_router,
    health_router,
    stream_router,
    jobs_router,
    backup_router,
)

setup_logging()
logger = get_logger(__name__)

app = FastAPI(title="Datalake API")

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(RequestLoggingMiddleware)

# Instrument every route with request counters + latency histograms.
# /metrics is exposed manually below so we can gate it behind admin scope.
Instrumentator(excluded_handlers=["/metrics", "/healthcheck", "/healthcheck/ready"]).instrument(app)


@app.get("/metrics", include_in_schema=False)
def metrics(user: User = Depends(ScopedAuth("admin"))):
    """Prometheus scrape endpoint. Admin-only — scrape with an admin API key."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.include_router(catalog_router)
app.include_router(instruments_router)
app.include_router(query_router)
app.include_router(ingest_router)
app.include_router(auth_router)
app.include_router(health_router)
app.include_router(stream_router)
app.include_router(jobs_router)
app.include_router(backup_router)


@app.on_event("startup")
def startup_event():
    """Initialize PostgreSQL tables and DuckDB schema on startup."""
    logger.info("Starting up API")
    validate_secrets(logger)
    init_db()
    init_duckdb()
    logger.info("Database initialized successfully")


@app.on_event("shutdown")
def shutdown_event():
    """Block until in-flight DuckDB writes finish so SIGTERM can't interrupt a transaction."""
    logger.info("Shutdown: waiting for in-flight writes", extra={"timeout_s": SHUTDOWN_WRITE_WAIT_SECONDS})
    acquired = _write_tx_lock.acquire(timeout=SHUTDOWN_WRITE_WAIT_SECONDS)
    if acquired:
        _write_tx_lock.release()
        logger.info("Shutdown: no in-flight writes, exiting cleanly")
    else:
        logger.warning("Shutdown: timed out waiting for in-flight write; exiting anyway")
