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

    @classmethod
    def from_orm_key(cls, k) -> "APIKeyResponse":
        return cls(
            id=k.id, prefix=k.prefix, name=k.name, scopes=k.scopes,
            expires_at=k.expires_at.isoformat() if k.expires_at else None,
            last_used_at=k.last_used_at.isoformat() if k.last_used_at else None,
            is_active=k.is_active, created_at=k.created_at.isoformat(),
        )


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
