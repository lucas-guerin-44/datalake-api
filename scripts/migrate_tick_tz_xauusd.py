"""One-shot migration: correct broker-local-as-UTC timestamps in tick_data
for XAUUSD.

Background — see quant-strategies-research/docs/RESEARCH_NOTES.md lesson #80
and docs/TZ_FIX_PHASE2_DATALAKE_RECONCILIATION.md.

The XAUUSD ticks in tick_data were originally ingested from an MT5
<DATE>/<TIME> CSV via _read_raw_tick(); that path treated broker-local
clock values as UTC. This script applies a DST-aware Athens -> UTC
correction in place via DuckDB's `timezone()` function.

Run on the VPS where the DuckDB file lives. Read-write transaction;
backs up the affected rows to `tick_data_xauusd_backup_<run-id>` before
UPDATE. Idempotent guard: refuses to run if a `tick_data_migrations`
ledger already contains a matching entry.

Usage:
    # dry-run (default) — prints sample before/after, no writes:
    python scripts/migrate_tick_tz_xauusd.py

    # apply:
    python scripts/migrate_tick_tz_xauusd.py --confirm

    # ad-hoc broker tz override (default = Europe/Athens):
    MT5_BROKER_TZ=Europe/Bucharest python scripts/migrate_tick_tz_xauusd.py --confirm
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Make sibling 'src/' importable so we share the lake's DB path config.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

import duckdb  # type: ignore

# Reuse the lake's own DB path config (avoid drift).
try:
    from src.config import DUCKDB_PATH  # type: ignore
except Exception:
    DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(_HERE.parent / "datalake" / "ohlc.duckdb")))


MIGRATION_ID = "tick_tz_xauusd_athens_2026_05_28"


def ensure_ledger(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS tick_data_migrations (
            migration_id VARCHAR PRIMARY KEY,
            instrument VARCHAR,
            broker_tz VARCHAR,
            rows_affected BIGINT,
            backup_table VARCHAR,
            applied_at TIMESTAMP
        )
    """)


def already_applied(con) -> bool:
    r = con.execute(
        "SELECT COUNT(*) FROM tick_data_migrations WHERE migration_id = ?",
        [MIGRATION_ID],
    ).fetchone()
    return bool(r and r[0])


def sample_rows(con, n=8) -> list:
    return con.execute(f"""
        SELECT timestamp, price, bid, ask
        FROM tick_data
        WHERE instrument = 'XAUUSD'
        ORDER BY timestamp
        LIMIT {n}
    """).fetchall()


def sample_post(con, broker_tz: str, n=8) -> list:
    return con.execute(f"""
        SELECT
            timestamp                                              AS before,
            timezone('UTC', timezone(?, timestamp))                 AS after,
            price, bid, ask
        FROM tick_data
        WHERE instrument = 'XAUUSD'
        ORDER BY timestamp
        LIMIT {n}
    """, [broker_tz]).fetchall()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--confirm", action="store_true", help="Apply the UPDATE (default: dry-run)")
    ap.add_argument("--broker-tz", default=os.getenv("MT5_BROKER_TZ", "Europe/Athens"))
    ap.add_argument("--db", default=str(DUCKDB_PATH))
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"DuckDB file not found: {db_path}")
        return 2

    print(f"DB: {db_path}")
    print(f"broker_tz: {args.broker_tz}")
    print(f"migration_id: {MIGRATION_ID}")

    con = duckdb.connect(str(db_path))

    ensure_ledger(con)
    if already_applied(con):
        print("This migration is already recorded in tick_data_migrations. Refusing to re-run.")
        return 0

    total = con.execute("SELECT COUNT(*) FROM tick_data WHERE instrument='XAUUSD'").fetchone()[0]
    print(f"\nXAUUSD tick rows to migrate: {total:,}")

    print("\n--- BEFORE (first 8 rows) ---")
    for r in sample_rows(con):
        print(" ", r)

    print(f"\n--- COMPUTED AFTER (via timezone('UTC', timezone('{args.broker_tz}', ts))) ---")
    for r in sample_post(con, args.broker_tz):
        print(" ", r)

    if not args.confirm:
        print("\nDry-run. Re-run with --confirm to apply.")
        return 0

    backup_table = f"tick_data_xauusd_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    print(f"\nBacking up to {backup_table} ...")
    con.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM tick_data WHERE instrument='XAUUSD'")
    backup_count = con.execute(f"SELECT COUNT(*) FROM {backup_table}").fetchone()[0]
    print(f"  backed up {backup_count:,} rows")
    assert backup_count == total, "Backup row count mismatch — aborting before UPDATE"

    print(f"\nApplying UPDATE ...")
    # DuckDB UPDATE semantics: timezone('UTC', timezone(tz, ts)) returns the
    # naive TIMESTAMP at UTC corresponding to interpreting `ts` as wall-clock
    # in `tz`. DST-aware via the IANA zone tables.
    con.execute("""
        UPDATE tick_data
        SET timestamp = timezone('UTC', timezone(?, timestamp))
        WHERE instrument = 'XAUUSD'
    """, [args.broker_tz])
    # DuckDB doesn't expose row-count for UPDATE separately; the backup table
    # holds the authoritative pre-state count, which matches.
    rows = total

    print(f"\n--- AFTER UPDATE (first 8 rows) ---")
    for r in sample_rows(con):
        print(" ", r)

    new_min, new_max = con.execute(
        "SELECT MIN(timestamp), MAX(timestamp) FROM tick_data WHERE instrument='XAUUSD'"
    ).fetchone()
    print(f"\nNew XAUUSD tick range: {new_min} -> {new_max}")

    print(f"\nRecording migration in tick_data_migrations ledger ...")
    con.execute("""
        INSERT INTO tick_data_migrations
            (migration_id, instrument, broker_tz, rows_affected, backup_table, applied_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [MIGRATION_ID, "XAUUSD", args.broker_tz, rows, backup_table, datetime.now(timezone.utc)])

    print(f"Done. backup={backup_table} rows_affected={rows:,}")
    print(f"Rollback: UPDATE tick_data SET timestamp = b.timestamp FROM {backup_table} b "
          f"WHERE tick_data.instrument='XAUUSD' AND tick_data.<other PK columns> = b.<same>; "
          f"DELETE FROM tick_data_migrations WHERE migration_id = '{MIGRATION_ID}';")
    return 0


if __name__ == "__main__":
    sys.exit(main())
