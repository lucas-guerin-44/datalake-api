"""
Rate limiting via slowapi.

Keyed by client IP. Behind Caddy the real client IP arrives in X-Forwarded-For
(Caddyfile sets this); slowapi's get_remote_address reads it when the app is
started with FORWARDED_ALLOW_IPS.

Enabled by default. Disable in tests by setting RATE_LIMIT_ENABLED=false.
"""
import os

from slowapi import Limiter
from slowapi.util import get_remote_address

RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() in ("true", "1", "yes")

# Default budget applies to every route that doesn't opt in/out explicitly.
DEFAULT_LIMIT = os.getenv("RATE_LIMIT_DEFAULT", "120/minute")

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[DEFAULT_LIMIT] if RATE_LIMIT_ENABLED else [],
    enabled=RATE_LIMIT_ENABLED,
)
