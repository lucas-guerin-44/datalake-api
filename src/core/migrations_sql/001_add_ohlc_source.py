"""Add `source` column to ohlc_data, marking rows as 'raw' or 'derived'."""


def up(con):
    existing = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='ohlc_data'"
    ).fetchall()]
    if "source" not in existing:
        # DuckDB's ALTER TABLE doesn't support NOT NULL/DEFAULT together — add
        # nullable, backfill, and let app-level writes keep it populated.
        con.execute("ALTER TABLE ohlc_data ADD COLUMN source VARCHAR")
        con.execute("UPDATE ohlc_data SET source = 'raw' WHERE source IS NULL")
