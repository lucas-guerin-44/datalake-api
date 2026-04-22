"""Pydantic models for request/response serialization."""
from typing import Optional, List
from pydantic import BaseModel


# --- API Keys ---

VALID_SCOPES = ["read", "write", "admin"]


class APIKeyCreate(BaseModel):
    name: str
    scopes: List[str] = ["read"]
    expires_in_days: Optional[int] = None


class APIKeyUpdate(BaseModel):
    name: Optional[str] = None
    scopes: Optional[List[str]] = None
    expires_in_days: Optional[int] = None
    is_active: Optional[bool] = None


class APIKeyResponse(BaseModel):
    id: int
    prefix: str
    name: str
    scopes: List[str]
    expires_at: Optional[str] = None
    last_used_at: Optional[str] = None
    is_active: bool
    created_at: str


class APIKeyCreatedResponse(BaseModel):
    """Returned once at creation time — includes the full key."""
    id: int
    key: str
    prefix: str
    name: str
    scopes: List[str]
    expires_at: Optional[str] = None
    is_active: bool
    created_at: str
