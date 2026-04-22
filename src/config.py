"""Application configuration loaded from environment variables."""
from pathlib import Path
from dotenv import load_dotenv
import os

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

# API
API_URL = os.getenv("API_URL", "http://127.0.0.1").strip()
API_PORT = int(os.getenv("API_PORT", 8000))

# If True, read endpoints are publicly accessible without authentication.
# Defaults to False for safety - flip to true only if you intentionally want public reads.
ALLOW_PUBLIC_READS = os.getenv("ALLOW_PUBLIC_READS", "false").lower() in ("true", "1", "yes")

# PostgreSQL
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "datalake")

# DuckDB
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(PROJECT_ROOT / "datalake" / "ohlc.duckdb")))
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "2GB")

# Ingest limits
MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", 500))
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024

_DEFAULT_POSTGRES_PASSWORD = "datalake"


def validate_secrets(logger=None):
    """Warn if POSTGRES_PASSWORD is still using the default."""
    if POSTGRES_PASSWORD != _DEFAULT_POSTGRES_PASSWORD:
        return

    separator = "=" * 60
    warning_block = (
        f"\n{separator}\n  SECURITY WARNING\n{separator}\n\n"
        f"  POSTGRES_PASSWORD is using the default value 'datalake'. "
        f"Set a strong password in your .env file.\n\n{separator}\n"
    )

    if logger:
        logger.warning(warning_block)
    else:
        import sys
        print(warning_block, file=sys.stderr)
