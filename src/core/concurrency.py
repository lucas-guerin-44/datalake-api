"""
Backpressure for read and write endpoints.

DuckDB already parallelizes each query across cores, so a flood of concurrent
heavy scans competes for the same CPU/memory (prod caps the container at 2 CPUs
and DuckDB at a 2GB memory_limit). Rather than let that pile up until the gateway
times out and emits a bare 502, we shed load honestly: once MAX_CONCURRENT_QUERIES
are in flight, further read requests get a 503 + `Retry-After` so clients can back
off and retry. A 503 means "busy, try again"; a 502 reads as "down". See
datalake-api-c45.

Writes need the same treatment, for a different reason: every write serializes on
a single DuckDB connection + `_write_tx_lock` (see src/core/datalake.py), so only
one can make progress at a time regardless. A second concurrent ingest would
otherwise block a threadpool thread all the way to the 120s gateway timeout. We
shed it immediately with the same honest 503 + `Retry-After`. See datalake-api-29o.
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


# --- Write-path backpressure ---
#
# Writes serialize on the single DuckDB write connection, so the default depth is
# 1: one ingest in flight, any concurrent one gets shed. A *serial* client (POST,
# await response, POST again) never trips this — the prior write has already
# released the slot by the time the next request's dependency runs; only genuinely
# overlapping writers are turned away. Retry-After is a touch longer than the read
# value because a write+derive takes longer to clear than a read.
MAX_CONCURRENT_WRITES = int(os.getenv("MAX_CONCURRENT_WRITES", "1"))
WRITE_RETRY_AFTER_SECONDS = int(os.getenv("WRITE_RETRY_AFTER_SECONDS", "2"))

_write_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_WRITES)


def write_slot():
    """
    FastAPI dependency: reserve one write slot for the request's lifetime.
    Returns 503 + Retry-After immediately when a write is already in progress,
    instead of blocking a threadpool thread until the gateway times out.
    """
    if not _write_semaphore.acquire(blocking=False):
        raise HTTPException(
            status_code=503,
            detail="Server busy — a write is already in progress. Retry shortly.",
            headers={"Retry-After": str(WRITE_RETRY_AFTER_SECONDS)},
        )
    try:
        yield
    finally:
        _write_semaphore.release()
