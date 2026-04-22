"""API-key management routes. All endpoints require an admin-scope API key.

User creation and key minting is an operator task — use `scripts/mint_api_key.py`
or the bootstrap snippet in deploy/RUNBOOK.md. There is no HTTP self-service
registration or login flow by design.
"""
from datetime import datetime, timedelta
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.middleware.logging_config import get_logger
from src.core.database import (
    get_db, User,
    create_api_key, get_api_key_by_id, get_api_keys_by_user,
    update_api_key, delete_api_key,
)
from src.schemas import (
    APIKeyCreate, APIKeyUpdate, APIKeyResponse, APIKeyCreatedResponse,
)
from src.auth.auth import (
    ScopedAuth, generate_api_key, hash_api_key, validate_scopes,
)

logger = get_logger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/api-keys", response_model=APIKeyCreatedResponse, status_code=201)
def create_new_api_key(
    key_data: APIKeyCreate,
    current_user: User = Depends(ScopedAuth("admin")),
    db: Session = Depends(get_db),
):
    """
    Create a new API key for the calling admin's own user. The full key is only
    returned once in this response.

    Scopes: read (query/download), write (read + ingest), admin (all)
    """
    scopes = validate_scopes(key_data.scopes)

    expires_at = None
    if key_data.expires_in_days:
        expires_at = datetime.utcnow() + timedelta(days=key_data.expires_in_days)

    full_key, prefix = generate_api_key()
    api_key = create_api_key(
        db=db,
        user_id=current_user.id,
        key_hash=hash_api_key(full_key),
        prefix=prefix,
        name=key_data.name,
        scopes=scopes,
        expires_at=expires_at,
    )
    logger.info("API key created", extra={"username": current_user.username, "prefix": prefix})

    return APIKeyCreatedResponse(
        id=api_key.id, key=full_key, prefix=prefix,
        name=api_key.name, scopes=api_key.scopes,
        expires_at=api_key.expires_at.isoformat() if api_key.expires_at else None,
        is_active=api_key.is_active, created_at=api_key.created_at.isoformat(),
    )


@router.get("/api-keys", response_model=List[APIKeyResponse])
def list_api_keys(
    current_user: User = Depends(ScopedAuth("admin")),
    db: Session = Depends(get_db),
):
    """List all API keys owned by the calling admin's user (metadata only)."""
    keys = get_api_keys_by_user(db, current_user.id)
    return [
        APIKeyResponse(
            id=k.id, prefix=k.prefix, name=k.name, scopes=k.scopes,
            expires_at=k.expires_at.isoformat() if k.expires_at else None,
            last_used_at=k.last_used_at.isoformat() if k.last_used_at else None,
            is_active=k.is_active, created_at=k.created_at.isoformat(),
        )
        for k in keys
    ]


@router.get("/api-keys/{key_id}", response_model=APIKeyResponse)
def get_api_key(
    key_id: int,
    current_user: User = Depends(ScopedAuth("admin")),
    db: Session = Depends(get_db),
):
    """Get details of a specific API key owned by the calling admin's user."""
    api_key = get_api_key_by_id(db, key_id)
    if not api_key or api_key.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="API key not found")

    return APIKeyResponse(
        id=api_key.id, prefix=api_key.prefix, name=api_key.name, scopes=api_key.scopes,
        expires_at=api_key.expires_at.isoformat() if api_key.expires_at else None,
        last_used_at=api_key.last_used_at.isoformat() if api_key.last_used_at else None,
        is_active=api_key.is_active, created_at=api_key.created_at.isoformat(),
    )


@router.patch("/api-keys/{key_id}", response_model=APIKeyResponse)
def update_api_key_endpoint(
    key_id: int,
    key_data: APIKeyUpdate,
    current_user: User = Depends(ScopedAuth("admin")),
    db: Session = Depends(get_db),
):
    """Update an API key's name, scopes, expiration, or active status."""
    api_key = get_api_key_by_id(db, key_id)
    if not api_key or api_key.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="API key not found")

    scopes = validate_scopes(key_data.scopes) if key_data.scopes is not None else None

    expires_at = api_key.expires_at
    if key_data.expires_in_days is not None:
        expires_at = (
            None if key_data.expires_in_days == 0
            else datetime.utcnow() + timedelta(days=key_data.expires_in_days)
        )

    updated = update_api_key(
        db=db, api_key=api_key,
        name=key_data.name, scopes=scopes,
        expires_at=expires_at, is_active=key_data.is_active,
    )
    logger.info("API key updated", extra={"prefix": api_key.prefix, "username": current_user.username})

    return APIKeyResponse(
        id=updated.id, prefix=updated.prefix, name=updated.name, scopes=updated.scopes,
        expires_at=updated.expires_at.isoformat() if updated.expires_at else None,
        last_used_at=updated.last_used_at.isoformat() if updated.last_used_at else None,
        is_active=updated.is_active, created_at=updated.created_at.isoformat(),
    )


@router.delete("/api-keys/{key_id}", status_code=204)
def revoke_api_key(
    key_id: int,
    current_user: User = Depends(ScopedAuth("admin")),
    db: Session = Depends(get_db),
):
    """Permanently revoke an API key."""
    api_key = get_api_key_by_id(db, key_id)
    if not api_key or api_key.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="API key not found")

    logger.info("API key revoked", extra={"prefix": api_key.prefix, "username": current_user.username})
    delete_api_key(db, api_key)
