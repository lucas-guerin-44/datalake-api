"""Application configuration loaded from environment variables."""
from pathlib import Path
from dotenv import load_dotenv
import os

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

# API
API_URL = os.getenv("API_URL", "http://127.0.0.1").strip()
API_PORT = int(os.getenv("API_PORT", 8000))

# Authentication
SECRET_KEY = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError(
        "SECRET_KEY environment variable is required. "
        "Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(32))\""
    )
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))

# If True, read endpoints are publicly accessible without authentication.
# Defaults to False for safety - flip to true only if you intentionally want public reads.
ALLOW_PUBLIC_READS = os.getenv("ALLOW_PUBLIC_READS", "false").lower() in ("true", "1", "yes")

# If True, POST /auth/register is open. Defaults to False — production deployments
# should mint API keys via scripts/mint_api_key.py instead of self-service registration.
# Flip to true explicitly if you want an open registration window.
ALLOW_REGISTRATION = os.getenv("ALLOW_REGISTRATION", "false").lower() in ("true", "1", "yes")

# PostgreSQL
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "datalake")

# DuckDB
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(PROJECT_ROOT / "datalake" / "ohlc.duckdb")))
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "2GB")

# Ingest limits
MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", 500))
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024

# Default values that should be changed in production
_DEFAULT_SECRET_KEY = "secretToken"
_DEFAULT_POSTGRES_PASSWORD = "datalake"


def validate_secrets(logger=None):
    """Warn if SECRET_KEY or POSTGRES_PASSWORD are still using defaults."""
    warnings = []

    if SECRET_KEY == _DEFAULT_SECRET_KEY:
        warnings.append(
            "SECRET_KEY is using the default value. "
            "Generate a secure key with: python -c \"import secrets; print(secrets.token_urlsafe(32))\""
        )

    if POSTGRES_PASSWORD == _DEFAULT_POSTGRES_PASSWORD:
        warnings.append(
            "POSTGRES_PASSWORD is using the default value 'datalake'. "
            "Set a strong password in your .env file."
        )

    if warnings:
        separator = "=" * 60
        warning_block = f"\n{separator}\n  SECURITY WARNING\n{separator}\n"
        for w in warnings:
            warning_block += f"\n  {w}\n"
        warning_block += f"\n{separator}\n"

        if logger:
            logger.warning(warning_block)
        else:
            import sys
            print(warning_block, file=sys.stderr)
