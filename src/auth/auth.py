"""Authentication and authorization — API keys only. JWT was removed intentionally;
any browser/UI that needs session auth should layer on top of API keys."""
import json
import secrets
from datetime import datetime
from typing import Optional, List

from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status, Header, Request, WebSocket
from sqlalchemy.orm import Session

from src.core.database import (
    get_db, get_user_by_id, User,
    APIKey, get_api_key_by_prefix, update_api_key_last_used
)
from src.schemas import VALID_SCOPES

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

API_KEY_PREFIX = "dk_"


# --- Password utilities ---
# Kept because `create_user` stores a hashed password in the `users.hashed_password`
# NOT NULL column. Nothing authenticates with passwords anymore, so bootstrap users
# with `get_password_hash(secrets.token_urlsafe(32))` and throw the plaintext away.

def get_password_hash(password: str) -> str:
    """Hash a password for storage in users.hashed_password."""
    return pwd_context.hash(password)


# --- API Key utilities ---

def generate_api_key() -> tuple[str, str]:
    """Generate a new API key. Returns (full_key, prefix)."""
    random_part = secrets.token_urlsafe(32)
    full_key = f"{API_KEY_PREFIX}{random_part}"
    prefix = full_key[:12]
    return full_key, prefix


def hash_api_key(key: str) -> str:
    """Hash an API key for storage."""
    return pwd_context.hash(key)


def verify_api_key(plain_key: str, hashed_key: str) -> bool:
    """Verify an API key against its hash."""
    return pwd_context.verify(plain_key, hashed_key)


def validate_scopes(scopes: List[str]) -> List[str]:
    """Validate that all scopes are valid. Raises HTTPException if invalid."""
    invalid = [s for s in scopes if s not in VALID_SCOPES]
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid scopes: {invalid}. Valid scopes are: {VALID_SCOPES}"
        )
    return scopes


def check_scope(required_scope: str, user_scopes: List[str]) -> bool:
    """Check if user has the required scope. admin > write > read."""
    if "admin" in user_scopes:
        return True
    if required_scope == "write" and "write" in user_scopes:
        return True
    if required_scope == "read" and ("read" in user_scopes or "write" in user_scopes):
        return True
    return required_scope in user_scopes


def authenticate_api_key(db: Session, api_key: str) -> Optional[tuple[User, APIKey]]:
    """Authenticate using an API key. Returns (User, APIKey) or None."""
    if not api_key or not api_key.startswith(API_KEY_PREFIX):
        return None

    prefix = api_key[:12]
    key_record = get_api_key_by_prefix(db, prefix)
    if not key_record or not verify_api_key(api_key, key_record.key_hash):
        return None
    if not key_record.is_active:
        return None
    if key_record.expires_at and key_record.expires_at < datetime.utcnow():
        return None

    user = get_user_by_id(db, key_record.user_id)
    if not user or not user.is_active:
        return None

    update_api_key_last_used(db, key_record)
    return user, key_record


# --- FastAPI dependencies ---

class ScopedAuth:
    """
    Dependency that requires an API key with a specific scope.

    If allow_public=True, returns None instead of raising 401 when no credentials
    are provided (used for public read endpoints).

    Usage:
        @router.get("/data")
        def get_data(user: User = Depends(ScopedAuth("read"))):
            ...
    """
    def __init__(self, required_scope: str, allow_public: bool = False):
        self.required_scope = required_scope
        self.allow_public = allow_public

    def __call__(
        self,
        request: Request = None,
        db: Session = Depends(get_db),
        x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    ) -> Optional[User]:
        if x_api_key:
            result = authenticate_api_key(db, x_api_key)
            if result:
                user, key_record = result
                if not check_scope(self.required_scope, key_record.scopes):
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"API key does not have required scope: {self.required_scope}"
                    )
                return user

        if self.allow_public:
            return None

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "API-Key"},
        )


# --- WebSocket authentication ---

async def ws_require_auth(
    ws: WebSocket,
    db: Session,
    required_scope: str,
    allow_public: bool,
) -> bool:
    """
    Validate an already-accepted WebSocket connection.

    Credentials are read from the `X-API-Key` header only. Query-string credentials
    are rejected outright — they leak into proxy access logs. Browser clients that
    can't set WS headers should use a signed short-lived token flow instead; none
    of our current clients are browser-based.

    Returns True when the caller should proceed with streaming:
      - authenticated API key (scope satisfied), or
      - no credentials supplied AND allow_public is True.

    Returns False after closing the socket with 4401 / 4403 when auth is
    required and missing/invalid. Callers should `return` immediately.
    """
    api_key = ws.headers.get("x-api-key")

    if api_key:
        result = authenticate_api_key(db, api_key)
        if result:
            _, key_record = result
            if check_scope(required_scope, key_record.scopes):
                return True
            await _ws_close_with_reason(ws, 4403, f"insufficient scope: {required_scope} required")
            return False

    if allow_public and not api_key:
        return True

    await _ws_close_with_reason(ws, 4401, "unauthorized")
    return False


async def _ws_close_with_reason(ws: WebSocket, code: int, message: str) -> None:
    try:
        await ws.send_text(json.dumps({"error": message}))
    except Exception:
        pass
    try:
        await ws.close(code=code)
    except Exception:
        pass
