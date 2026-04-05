"""Health route."""
from fastapi import APIRouter

router = APIRouter()


@router.get("/healthcheck")
def healthcheck():
    """Return a simple health check response."""
    return {"status": "healthy"}
