"""
Lightweight DuckDB migration runner.

Migrations live as numbered Python modules (e.g. 001_add_ohlc_source.py) in
src/core/migrations_sql/. Each module exposes `up(con)` that applies the change
idempotently. The runner records applied versions in _schema_migrations so
each runs exactly once per database file.
"""
import importlib
import pkgutil
from typing import List, Tuple

from src.middleware.logging_config import get_logger

logger = get_logger(__name__)


def _ensure_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _schema_migrations (
            version VARCHAR PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _applied_versions(con) -> set:
    rows = con.execute("SELECT version FROM _schema_migrations").fetchall()
    return {r[0] for r in rows}


def _discover_migrations() -> List[Tuple[str, object]]:
    """Return [(version_id, module), ...] sorted by version_id."""
    from src.core import migrations_sql  # noqa
    found = []
    for mod_info in pkgutil.iter_modules(migrations_sql.__path__):
        name = mod_info.name
        if not name[:3].isdigit():
            continue
        mod = importlib.import_module(f"src.core.migrations_sql.{name}")
        found.append((name, mod))
    return sorted(found, key=lambda x: x[0])


def run_migrations(con):
    """Apply any pending migrations in version order. Idempotent."""
    _ensure_table(con)
    applied = _applied_versions(con)

    for version, mod in _discover_migrations():
        if version in applied:
            continue
        if not hasattr(mod, "up"):
            logger.warning("Migration missing up()", extra={"version": version})
            continue
        logger.info("Applying migration", extra={"version": version})
        mod.up(con)
        con.execute("INSERT INTO _schema_migrations (version) VALUES (?)", [version])
