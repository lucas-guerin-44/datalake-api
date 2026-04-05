"""Route modules for the API."""
from src.routes.catalog import router as catalog_router
from src.routes.instruments import router as instruments_router
from src.routes.query import router as query_router
from src.routes.ingest import router as ingest_router
from src.routes.auth_routes import router as auth_router
from src.routes.health import router as health_router

__all__ = [
    "catalog_router",
    "instruments_router",
    "query_router",
    "ingest_router",
    "auth_router",
    "health_router",
]
