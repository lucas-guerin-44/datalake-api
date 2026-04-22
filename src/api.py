"""Main FastAPI application - wires up all route modules."""
from fastapi import FastAPI
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler

from src.middleware.logging_config import setup_logging, get_logger
from src.middleware.middleware import RequestLoggingMiddleware
from src.middleware.ratelimit import limiter
from src.core.database import init_db
from src.core.datalake import init_duckdb
from src.config import validate_secrets
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
