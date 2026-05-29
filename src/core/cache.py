"""
In-memory, version-keyed cache for read endpoints whose answers only change on
ingest/delete (e.g. /catalog, /instruments). Each cached value is tagged with the
data-version token from src.core.datalake; a write bumps that token, so the next
read recomputes and everything in between is served from memory without touching
DuckDB. This keeps catalog/instrument listings from competing with data queries.
See datalake-api-w9a.
"""
import threading
from typing import Callable

from src.core.datalake import get_data_version

_lock = threading.Lock()
_store: dict = {}  # key -> (data_version, value)


def get_or_compute(key: str, producer: Callable[[], object]) -> object:
    """
    Return the cached value for `key` if it was computed at the current data
    version, else call `producer()`, cache, and return it. `producer` runs
    outside the lock so a slow build doesn't block other cache readers.
    """
    version = get_data_version()
    with _lock:
        hit = _store.get(key)
        if hit is not None and hit[0] == version:
            return hit[1]

    value = producer()

    with _lock:
        # Re-tag with the version captured before producing. If a write landed
        # mid-build, the entry is already stale-by-version and the next read
        # recomputes — never serving data older than its tag claims.
        _store[key] = (version, value)
    return value


def clear() -> None:
    """Drop all cached entries. Mainly for tests; production self-invalidates by version."""
    with _lock:
        _store.clear()
