"""
Catalog export/restore to partitioned Parquet.

Export writes `ohlc_data` partitioned by (instrument, timeframe) and `tick_data`
partitioned by instrument, plus a manifest.json with row counts and paths.
Restore reads a manifest and re-ingests via DuckDB's read_parquet, with hive
partitioning to recover the partition columns. Restore is idempotent (ON CONFLICT
merges rows).
"""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from src.config import PROJECT_ROOT
from src.core.datalake import get_db_connection, write_transaction
from src.middleware.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_BACKUP_ROOT = PROJECT_ROOT / "backups"
MANIFEST_FILENAME = "manifest.json"
SCHEMA_VERSION = 1


def list_backups(backup_root: Path = None) -> list[dict]:
    """List all backup directories under `backup_root`, newest first."""
    root = Path(backup_root) if backup_root else DEFAULT_BACKUP_ROOT
    if not root.exists():
        return []
    out = []
    for d in sorted(root.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        m = d / MANIFEST_FILENAME
        if m.exists():
            out.append({"name": d.name, "path": str(d), "manifest_path": str(m)})
    return out


def latest_manifest(backup_root: Path = None) -> Optional[Dict[str, Any]]:
    """Return the manifest dict of the most recent backup, or None if there are none."""
    backups = list_backups(backup_root)
    if not backups:
        return None
    return json.loads(Path(backups[0]["manifest_path"]).read_text())


def prune_old_backups(keep: int = 8, backup_root: Path = None) -> int:
    """Keep the `keep` newest backup directories; delete the rest. Returns count removed."""
    backups = list_backups(backup_root)
    removed = 0
    for b in backups[keep:]:
        shutil.rmtree(b["path"], ignore_errors=True)
        removed += 1
    return removed


def export_catalog(output_dir: Path = None) -> Dict[str, Any]:
    """
    Dump the datalake to partitioned Parquet + a manifest.
    Returns the manifest dict and the output directory path.
    """
    if output_dir is None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        output_dir = DEFAULT_BACKUP_ROOT / ts
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    ohlc_dir = output_dir / "ohlc"
    ticks_dir = output_dir / "ticks"

    # Wipe target subdirs so partition output is deterministic.
    if ohlc_dir.exists():
        shutil.rmtree(ohlc_dir)
    if ticks_dir.exists():
        shutil.rmtree(ticks_dir)

    with get_db_connection() as con:
        ohlc_count = con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0]
        tick_count = con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0]

        if ohlc_count > 0:
            con.execute(f"""
                COPY (SELECT * FROM ohlc_data)
                TO '{ohlc_dir.as_posix()}'
                (FORMAT PARQUET, PARTITION_BY (instrument, timeframe), OVERWRITE_OR_IGNORE)
            """)
        if tick_count > 0:
            con.execute(f"""
                COPY (SELECT * FROM tick_data)
                TO '{ticks_dir.as_posix()}'
                (FORMAT PARQUET, PARTITION_BY (instrument), OVERWRITE_OR_IGNORE)
            """)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(output_dir),
        "ohlc": {
            "path": str(ohlc_dir),
            "row_count": ohlc_count,
            "partition_by": ["instrument", "timeframe"],
        },
        "ticks": {
            "path": str(ticks_dir),
            "row_count": tick_count,
            "partition_by": ["instrument"],
        },
    }
    (output_dir / MANIFEST_FILENAME).write_text(json.dumps(manifest, indent=2))

    logger.info("Catalog exported", extra={
        "output_dir": str(output_dir),
        "ohlc_rows": ohlc_count,
        "tick_rows": tick_count,
    })
    return manifest


def restore_catalog(manifest_path: Path) -> Dict[str, Any]:
    """
    Re-ingest a previously exported catalog. Merges into existing tables via
    ON CONFLICT — existing rows are overwritten, new rows added, untouched
    rows left alone. Safe to run against a non-empty datalake.
    """
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported backup schema_version {manifest.get('schema_version')}; "
            f"this build expects {SCHEMA_VERSION}"
        )

    ohlc_path = Path(manifest["ohlc"]["path"])
    ticks_path = Path(manifest["ticks"]["path"])

    ohlc_restored = 0
    ticks_restored = 0

    with write_transaction() as con:
        if ohlc_path.exists() and any(ohlc_path.rglob("*.parquet")):
            pattern = (ohlc_path / "**" / "*.parquet").as_posix()
            con.execute(f"""
                INSERT INTO ohlc_data
                (instrument, timeframe, timestamp, open, high, low, close, source)
                SELECT instrument, timeframe, timestamp, open, high, low, close,
                       COALESCE(source, 'raw') AS source
                FROM read_parquet('{pattern}', hive_partitioning=true)
                ON CONFLICT (instrument, timeframe, timestamp) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low  = excluded.low,
                    close = excluded.close,
                    source = excluded.source
            """)
            ohlc_restored = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{pattern}', hive_partitioning=true)"
            ).fetchone()[0]

        if ticks_path.exists() and any(ticks_path.rglob("*.parquet")):
            pattern = (ticks_path / "**" / "*.parquet").as_posix()
            con.execute(f"""
                INSERT INTO tick_data
                (instrument, timestamp, price, volume, bid, ask)
                SELECT instrument, timestamp, price,
                       COALESCE(volume, 0.0) AS volume, bid, ask
                FROM read_parquet('{pattern}', hive_partitioning=true)
                ON CONFLICT (instrument, timestamp) DO UPDATE SET
                    price = excluded.price,
                    volume = excluded.volume,
                    bid = excluded.bid,
                    ask = excluded.ask
            """)
            ticks_restored = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{pattern}', hive_partitioning=true)"
            ).fetchone()[0]

    logger.info("Catalog restored", extra={
        "manifest": str(manifest_path),
        "ohlc_rows": ohlc_restored,
        "tick_rows": ticks_restored,
    })
    return {
        "status": "ok",
        "manifest": str(manifest_path),
        "ohlc_rows_restored": ohlc_restored,
        "tick_rows_restored": ticks_restored,
    }
