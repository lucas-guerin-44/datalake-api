"""
One-off cleanup: collapse offset-shifted duplicate OHLC bars.

Brokers on different server timezones stamp the same logical bar at different
hours (e.g. D1 at 00:00 vs 01:00 vs 02:00 UTC). Because each variant has a
distinct timestamp, the (instrument, timeframe, timestamp) primary key let all
of them coexist. This script snaps every bar's timestamp to the canonical UTC
bucket for its timeframe and keeps exactly one row per (instrument, timeframe,
canonical_timestamp), choosing the one whose original stamp was closest to the
bucket start.

Stop the API (or any other DuckDB writer) before running — DuckDB allows only
one read/write process at a time.

Usage:
    python scripts/dedupe_offset_bars.py [--db PATH] [--dry-run]
                                         [--timeframes D1,H4,W1,MN1]
                                         [--instrument XAUUSD]
"""
import argparse
import sys
from pathlib import Path

import duckdb


TF_TO_SQL_BUCKET = {
    "M1":  "time_bucket(INTERVAL '1 minute', timestamp)",
    "M5":  "time_bucket(INTERVAL '5 minute', timestamp)",
    "M15": "time_bucket(INTERVAL '15 minute', timestamp)",
    "M30": "time_bucket(INTERVAL '30 minute', timestamp)",
    "H1":  "time_bucket(INTERVAL '1 hour', timestamp)",
    "H4":  "time_bucket(INTERVAL '4 hour', timestamp)",
    "D1":  "time_bucket(INTERVAL '1 day', timestamp)",
    "W1":  "DATE_TRUNC('week', timestamp)",
    "MN1": "DATE_TRUNC('month', timestamp)",
}


def dedupe(con, instrument: str, timeframe: str, dry_run: bool) -> tuple[int, int]:
    bucket = TF_TO_SQL_BUCKET.get(timeframe.upper())
    if not bucket:
        print(f"  skip: no bucket rule for timeframe {timeframe!r}", file=sys.stderr)
        return 0, 0

    before = con.execute(
        "SELECT COUNT(*) FROM ohlc_data WHERE instrument = ? AND timeframe = ?",
        [instrument, timeframe],
    ).fetchone()[0]
    if before == 0:
        return 0, 0

    after = con.execute(f"""
        SELECT COUNT(DISTINCT {bucket})
        FROM ohlc_data
        WHERE instrument = ? AND timeframe = ?
    """, [instrument, timeframe]).fetchone()[0]

    removed = before - after
    if dry_run or removed == 0:
        return before, after

    # Per partition keep the row whose original stamp is closest to the bucket start
    # (i.e. MIN(timestamp) within the group). Transactionally: build the deduped
    # set, wipe the partition, reinsert.
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"""
            CREATE TEMP TABLE _dedup_stage AS
            SELECT instrument, timeframe, canonical_ts AS timestamp, open, high, low, close
            FROM (
                SELECT
                    instrument,
                    timeframe,
                    {bucket} AS canonical_ts,
                    open, high, low, close,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument, timeframe, {bucket}
                        ORDER BY timestamp
                    ) AS rn
                FROM ohlc_data
                WHERE instrument = ? AND timeframe = ?
            )
            WHERE rn = 1
        """, [instrument, timeframe])

        con.execute(
            "DELETE FROM ohlc_data WHERE instrument = ? AND timeframe = ?",
            [instrument, timeframe],
        )
        con.execute("""
            INSERT INTO ohlc_data (instrument, timeframe, timestamp, open, high, low, close)
            SELECT instrument, timeframe, timestamp, open, high, low, close
            FROM _dedup_stage
        """)
        con.execute("DROP TABLE _dedup_stage")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    return before, after


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(Path("datalake") / "ohlc.duckdb"),
                    help="Path to DuckDB file (default: datalake/ohlc.duckdb)")
    ap.add_argument("--dry-run", action="store_true", help="Report changes without applying them")
    ap.add_argument("--timeframes", default="D1,H4,W1,MN1",
                    help="Comma-separated timeframes to dedupe (default: D1,H4,W1,MN1)")
    ap.add_argument("--instrument", default=None,
                    help="Only operate on this instrument (default: all)")
    args = ap.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        print(f"error: DuckDB file not found at {db_path}", file=sys.stderr)
        return 2

    timeframes = [t.strip().upper() for t in args.timeframes.split(",") if t.strip()]

    con = duckdb.connect(str(db_path), read_only=args.dry_run)

    if args.instrument:
        instruments = [args.instrument]
    else:
        instruments = [
            r[0] for r in con.execute(
                "SELECT DISTINCT instrument FROM ohlc_data ORDER BY instrument"
            ).fetchall()
        ]

    print(f"{'DRY RUN: ' if args.dry_run else ''}dedupe {len(instruments)} instrument(s) x {len(timeframes)} timeframe(s) in {db_path}")
    total_removed = 0
    for inst in instruments:
        for tf in timeframes:
            before, after = dedupe(con, inst, tf, args.dry_run)
            removed = before - after
            if before:
                print(f"  {inst:10s} {tf:4s}  {before:>9,} -> {after:>9,}  (removed {removed:,})")
                total_removed += removed

    print(f"\n{'would remove' if args.dry_run else 'removed'}: {total_removed:,} row(s)")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
