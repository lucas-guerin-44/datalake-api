"""
Durable, single-writer derivation queue.

`POST /ingest` commits raw bars and a `pending` task row in ONE write
transaction (see pipeline.ingest_*_queued), so the queue table is an atomic
ledger: a window's raw bars are committed iff a derive task exists for it. A
single background worker drains pending tasks oldest-first; each task derives in
its own short `write_transaction` that also flips the row to `done` atomically.

Why this shape:
- **Off the hot path.** Ingest returns after the raw write; the heavy M5->D1
  derivation runs in the worker. Each derive is its own short transaction, so
  lock-hold and checkpoint windows shrink vs. the old one-giant-transaction —
  the corruption-resistance win, on top of lower ingest latency.
- **Crash-safe.** derive + `mark done` are atomic. A kill mid-derive rolls the
  derive back AND leaves the task `pending`, so the worker just re-runs it on
  restart. Derivation is idempotent (ON CONFLICT), so re-running is harmless.
- **Single-writer preserved.** Everything still funnels through
  `write_transaction()` / `_write_tx_lock`; the worker is one thread.

See datalake-api-k0s and the "Ingest -> auto-derivation invariant" note in CLAUDE.md.
"""
import os
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.middleware.logging_config import get_logger

logger = get_logger(__name__)

# After this many failed attempts a task is parked as 'error' (manual requeue),
# rather than retried forever. Derivation is deterministic SQL, so a repeated
# failure means a real problem, not transient load.
MAX_ATTEMPTS = int(os.getenv("DERIVATION_MAX_ATTEMPTS", "5"))

# Worker wakes on enqueue (via notify()); this is the fallback poll so tasks left
# pending by a crash get picked up even with no new ingest to notify it.
WORKER_POLL_SECONDS = float(os.getenv("DERIVATION_WORKER_POLL_SECONDS", "30"))


@dataclass
class Task:
    id: int
    instrument: str
    source_kind: str               # 'ohlc' | 'ticks'
    source_timeframe: Optional[str]  # source tf for 'ohlc'; None for 'ticks'
    window_start: datetime
    window_end: datetime

    def label(self) -> str:
        return (
            f"{self.instrument}/{self.source_timeframe}"
            if self.source_kind == "ohlc"
            else f"{self.instrument}/ticks"
        )


# --- enqueue (called inside the caller's write_transaction) -------------------

def enqueue(con, instrument: str, source_kind: str, source_timeframe: Optional[str],
            start, end) -> int:
    """
    Insert a pending derive task using the caller's connection so it commits
    atomically with the raw write. Returns the new task id.
    """
    from src.core.datalake import to_naive_utc
    new_id = con.execute("SELECT nextval('derivation_queue_seq')").fetchone()[0]
    con.execute(
        """
        INSERT INTO derivation_queue
            (id, instrument, source_kind, source_timeframe,
             window_start, window_end, status, attempts, enqueued_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [new_id, instrument, source_kind, source_timeframe,
         to_naive_utc(start), to_naive_utc(end)],
    )
    return new_id


# --- reads --------------------------------------------------------------------

def _read_next_pending() -> Optional[Task]:
    from src.core.datalake import read_connection
    with read_connection() as con:
        row = con.execute(
            """
            SELECT id, instrument, source_kind, source_timeframe, window_start, window_end
            FROM derivation_queue WHERE status = 'pending' ORDER BY id LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    return Task(*row)


def status_of(task_id: int) -> Optional[str]:
    from src.core.datalake import read_connection
    with read_connection() as con:
        row = con.execute(
            "SELECT status FROM derivation_queue WHERE id = ?", [task_id]
        ).fetchone()
    return row[0] if row else None


def queue_stats() -> dict:
    """Counts by status + age of the oldest pending task, for observability."""
    from src.core.datalake import read_connection
    with read_connection() as con:
        counts = {
            r[0]: r[1] for r in con.execute(
                "SELECT status, COUNT(*) FROM derivation_queue GROUP BY status"
            ).fetchall()
        }
        oldest = con.execute(
            "SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(enqueued_at))) "
            "FROM derivation_queue WHERE status = 'pending'"
        ).fetchone()[0]
    return {
        "pending": counts.get("pending", 0),
        "done": counts.get("done", 0),
        "error": counts.get("error", 0),
        "oldest_pending_age_seconds": int(oldest) if oldest is not None else None,
    }


# --- processing ---------------------------------------------------------------

def process_one() -> bool:
    """
    Derive the oldest pending task. Returns True if a task was handled (success
    OR a recorded failure), False if the queue had nothing pending.

    The status re-check inside the write transaction makes this safe to call
    from both the worker thread and a `wait=true` request: write transactions
    serialize, so whichever runs second sees the row already 'done' and skips —
    no duplicate derivation.
    """
    task = _read_next_pending()
    if task is None:
        return False

    from src.core.datalake import write_transaction
    from src.core.derive import derive_ohlc_timeframes, derive_ohlc_from_ticks
    try:
        with write_transaction() as con:
            still = con.execute(
                "SELECT status FROM derivation_queue WHERE id = ?", [task.id]
            ).fetchone()
            if not still or still[0] != "pending":
                return True  # handled by the other processor in the meantime
            if task.source_kind == "ticks":
                derive_ohlc_from_ticks(task.instrument, task.window_start, task.window_end)
            else:
                derive_ohlc_timeframes(
                    task.instrument, task.source_timeframe,
                    task.window_start, task.window_end,
                )
            con.execute(
                "UPDATE derivation_queue SET status = 'done', attempts = attempts + 1, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                [task.id],
            )
        logger.info("Derivation task done", extra={"task_id": task.id, "task": task.label()})
        return True
    except Exception as e:
        # The derive rolled back with its transaction; record the failure in a
        # fresh one. After MAX_ATTEMPTS the task parks as 'error' (no hot loop).
        logger.exception("Derivation task failed", extra={"task_id": task.id, "task": task.label()})
        try:
            with write_transaction() as con:
                con.execute(
                    "UPDATE derivation_queue SET attempts = attempts + 1, last_error = ?, "
                    "status = CASE WHEN attempts + 1 >= ? THEN 'error' ELSE 'pending' END, "
                    "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    [str(e)[:2000], MAX_ATTEMPTS, task.id],
                )
        except Exception:
            logger.exception("Failed to record derivation error", extra={"task_id": task.id})
        return True


def drain(max_tasks: Optional[int] = None) -> int:
    """Process pending tasks until the queue is empty (or max_tasks hit)."""
    n = 0
    while not _stop_event.is_set():
        if max_tasks is not None and n >= max_tasks:
            break
        if not process_one():
            break
        n += 1
    return n


def wait_for(task_id: int, timeout: float = 30.0) -> Optional[str]:
    """
    Block until `task_id` leaves 'pending', processing the queue ourselves so it
    completes even if the background worker is disabled. Returns its final status.
    """
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = status_of(task_id)
        if st != "pending":
            return st
        if not process_one():
            return status_of(task_id)
        time.sleep(0.05)
    return status_of(task_id)


# --- background worker --------------------------------------------------------

_worker_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_wake_event = threading.Event()
_worker_lock = threading.Lock()


def notify() -> None:
    """Wake the worker — call after enqueuing."""
    _wake_event.set()


def _worker_loop() -> None:
    logger.info("Derivation worker started")
    while not _stop_event.is_set():
        try:
            drain()
        except Exception:
            logger.exception("Derivation worker loop error")
        _wake_event.wait(timeout=WORKER_POLL_SECONDS)
        _wake_event.clear()
    logger.info("Derivation worker stopped")


def start_worker() -> None:
    """Start the single background worker thread (idempotent)."""
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return
        _stop_event.clear()
        _worker_thread = threading.Thread(
            target=_worker_loop, name="derivation-worker", daemon=True
        )
        _worker_thread.start()


def stop_worker(timeout: float = 10.0) -> None:
    """Signal the worker to stop after its current task and join it."""
    _stop_event.set()
    _wake_event.set()
    t = _worker_thread
    if t is not None:
        t.join(timeout=timeout)
        if t.is_alive():
            logger.warning("Derivation worker still alive after %.1fs timeout", timeout)
