"""
Durable queue for off-the-hot-path timeframe derivation.

`POST /ingest` commits the raw bars and a `pending` row in this table in the
SAME transaction, then a single background worker drains the queue and derives
each window in its own short transaction. Because enqueue is atomic with the
raw write, the table doubles as a crash-recovery ledger: any window whose raw
bars committed is guaranteed to have a derive task recorded, so a restart mid-
derive simply re-processes the still-`pending` task (derivation is idempotent).
See src/core/derivation_queue.py and datalake-api-k0s.
"""


def up(con):
    con.execute("CREATE SEQUENCE IF NOT EXISTS derivation_queue_seq START 1")
    con.execute("""
        CREATE TABLE IF NOT EXISTS derivation_queue (
            id BIGINT PRIMARY KEY,
            instrument VARCHAR NOT NULL,
            source_kind VARCHAR NOT NULL,        -- 'ohlc' | 'ticks'
            source_timeframe VARCHAR,            -- source tf for 'ohlc'; NULL for 'ticks'
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'pending',  -- pending | done | error
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error VARCHAR,
            enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # The worker scans for the oldest pending task; index keeps that O(log n).
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_derivation_queue_pending "
        "ON derivation_queue(status, id)"
    )
