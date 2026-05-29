"""
Backpressure for read endpoints.

DuckDB already parallelizes each query across cores, so a flood of concurrent
heavy scans competes for the same CPU/memory (prod caps the container at 2 CPUs
and DuckDB at a 2GB memory_limit). Rather than let that pile up until the gateway
times out and emits a bare 502, we shed load honestly: once MAX_CONCURRENT_QUERIES
are in flight, further read requests get a 503 + `Retry-After` so clients can back
off and retry. A 503 means "busy, try again"; a 502 reads as "down". See
datalake-api-c45.
"""
import os
import threading

from fastapi import HTTPException

MAX_CONCURRENT_QUERIES = int(os.getenv("MAX_CONCURRENT_QUERIES", "8"))
RETRY_AFTER_SECONDS = int(os.getenv("QUERY_RETRY_AFTER_SECONDS", "1"))

_query_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_QUERIES)


def query_slot():
    """
    FastAPI dependency: reserve one concurrent-read slot for the request's
    lifetime (including streamed response bodies). Returns 503 + Retry-After
    immediately when all slots are taken instead of queuing into a timeout.
    """
    if not _query_semaphore.acquire(blocking=False):
        raise HTTPException(
            status_code=503,
            detail="Server busy — too many concurrent queries. Retry shortly.",
            headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
        )
    try:
        yield
    finally:
        _query_semaphore.release()
