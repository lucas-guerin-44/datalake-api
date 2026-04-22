"""Authentication and authorization utilities for JWT-based auth and API keys."""
import json
import secrets
from datetime import datetime, timedelta
from typing import Optional, List

from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status, Header, Request, WebSocket
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from src.core.database import (
    get_db, get_user_by_username, get_user_by_id, User,
    APIKey, get_api_key_by_prefix, update_api_key_last_used
)
from src.config import SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from src.schemas import VALID_SCOPES

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

API_KEY_PREFIX = "dk_"


# --- Password utilities ---

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash a password."""
    return pwd_context.hash(password)


# --- JWT utilities ---

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=JWT_ALGORITHM)


# --- User authentication ---

def authenticate_user(db: Session, username: str, password: str) -> Optional[User]:
    """Authenticate a user by username and password. Returns None on failure."""
    user = get_user_by_username(db, username)
    if not user or not verify_password(password, user.hashed_password) or not user.is_active:
        return None
    return user


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

def _try_authenticate(
    credentials: Optional[HTTPAuthorizationCredentials],
    request: Optional[Request],
    db: Session,
    x_api_key: Optional[str],
) -> Optional[tuple[User, Optional[APIKey]]]:
    """
    Core authentication logic shared by all auth dependencies.
    Returns (User, APIKey|None) or None if no valid auth found.
    JWT users return (User, None). API key users return (User, APIKey).
    """
    # Try JWT
    if credentials:
        try:
            payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[JWT_ALGORITHM])
            username: str = payload.get("sub")
            if username:
                user = get_user_by_username(db, username=username)
                if user and user.is_active:
                    return user, None
        except JWTError:
            pass

    # Try API key (header only — query params leak keys into logs)
    if x_api_key:
        result = authenticate_api_key(db, x_api_key)
        if result:
            return result

    return None


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Dependency: requires a valid JWT Bearer token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = get_user_by_username(db, username=username)
    if user is None:
        raise credentials_exception
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user")
    return user


class ScopedAuth:
    """
    Dependency that requires authentication with a specific scope.
    Accepts JWT Bearer tokens and API keys.

    If allow_public=True, returns None instead of raising 401 when
    no credentials are provided (used for public read endpoints).

    Usage:
        @router.get("/data")
        def get_data(user: User = Depends(ScopedAuth("read"))):
            ...

        @router.get("/public-data")
        def get_public_data(user: Optional[User] = Depends(ScopedAuth("read", allow_public=True))):
            ...
    """
    def __init__(self, required_scope: str, allow_public: bool = False):
        self.required_scope = required_scope
        self.allow_public = allow_public

    def __call__(
        self,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
        request: Request = None,
        db: Session = Depends(get_db),
        x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    ) -> Optional[User]:
        result = _try_authenticate(credentials, request, db, x_api_key)

        if result:
            user, key_record = result
            # JWT users have all scopes; API key users need scope check
            if key_record and not check_scope(self.required_scope, key_record.scopes):
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
            headers={"WWW-Authenticate": "Bearer"},
        )


# --- WebSocket authentication ---

def _extract_bearer(header_value: Optional[str]) -> Optional[str]:
    if header_value and header_value.lower().startswith("bearer "):
        return header_value[7:].strip()
    return None


async def ws_require_auth(
    ws: WebSocket,
    db: Session,
    token: Optional[str],
    api_key: Optional[str],
    required_scope: str,
    allow_public: bool,
) -> bool:
    """
    Validate an already-accepted WebSocket connection.

    Credentials are read from query params (`token=`, `api_key=`) first, with a
    fallback to headers (`Authorization: Bearer`, `X-API-Key`). Query params are
    the common fallback for browser `new WebSocket()` which can't set headers;
    be aware they appear in access logs unless the proxy filters them.

    Returns True when the caller should proceed with streaming:
      - authenticated JWT or API key (scope satisfied), or
      - no credentials supplied AND allow_public is True.

    Returns False after closing the socket with 4401 / 4403 when auth is
    required and missing/invalid. Callers should `return` immediately.
    """
    token = token or _extract_bearer(ws.headers.get("authorization"))
    api_key = api_key or ws.headers.get("x-api-key")

    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
            username: str = payload.get("sub")
            if username:
                user = get_user_by_username(db, username=username)
                if user and user.is_active:
                    return True
        except JWTError:
            pass

    if api_key:
        result = authenticate_api_key(db, api_key)
        if result:
            _, key_record = result
            if check_scope(required_scope, key_record.scopes):
                return True
            await _ws_close_with_reason(ws, 4403, f"insufficient scope: {required_scope} required")
            return False

    if allow_public and not token and not api_key:
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
