"""
PostgreSQL database setup and models for user/auth metadata.
OHLC data is stored in DuckDB (see datalake.py).
"""
import os
from datetime import datetime
from typing import Optional, List, Dict
from contextlib import contextmanager

from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.pool import QueuePool

from src.middleware.logging_config import get_logger

logger = get_logger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://datalake:datalake@localhost:6543/datalake"
)

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class User(Base):
    """User model for authentication."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    api_keys = relationship("APIKey", back_populates="user", cascade="all, delete-orphan")

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class APIKey(Base):
    """API Key model for programmatic authentication."""
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    key_hash = Column(String(255), nullable=False)
    prefix = Column(String(16), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    scopes = Column(JSON, nullable=False, default=["read"])
    expires_at = Column(DateTime, nullable=True)
    last_used_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="api_keys")

    def to_dict(self, include_prefix: bool = True) -> Dict:
        result = {
            "id": self.id,
            "user_id": self.user_id,
            "name": self.name,
            "scopes": self.scopes,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
        if include_prefix:
            result["prefix"] = self.prefix
        return result


# --- Database lifecycle ---

def init_db():
    """Create all tables."""
    logger.info("Initializing database tables")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize database tables", exc_info=True)
        raise


def get_db():
    """FastAPI dependency for database sessions."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """Context manager for database sessions (non-FastAPI usage)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- User CRUD ---

def create_user(db: Session, username: str, email: str, hashed_password: str) -> User:
    user = User(username=username, email=email, hashed_password=hashed_password)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def get_user_by_username(db: Session, username: str) -> Optional[User]:
    return db.query(User).filter(User.username == username).first()


def get_user_by_email(db: Session, email: str) -> Optional[User]:
    return db.query(User).filter(User.email == email).first()


def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    return db.query(User).filter(User.id == user_id).first()


# --- API Key CRUD ---

def create_api_key(
    db: Session, user_id: int, key_hash: str, prefix: str,
    name: str, scopes: List[str], expires_at: Optional[datetime] = None,
) -> APIKey:
    api_key = APIKey(
        user_id=user_id, key_hash=key_hash, prefix=prefix,
        name=name, scopes=scopes, expires_at=expires_at,
    )
    db.add(api_key)
    db.commit()
    db.refresh(api_key)
    return api_key


def get_api_key_by_id(db: Session, key_id: int) -> Optional[APIKey]:
    return db.query(APIKey).filter(APIKey.id == key_id).first()


def get_api_key_by_prefix(db: Session, prefix: str) -> Optional[APIKey]:
    return db.query(APIKey).filter(APIKey.prefix == prefix).first()


def get_api_keys_by_user(db: Session, user_id: int) -> List[APIKey]:
    return db.query(APIKey).filter(APIKey.user_id == user_id).order_by(APIKey.created_at.desc()).all()


def update_api_key_last_used(db: Session, api_key: APIKey) -> None:
    api_key.last_used_at = datetime.utcnow()
    db.commit()


def update_api_key(
    db: Session, api_key: APIKey,
    name: Optional[str] = None, scopes: Optional[List[str]] = None,
    expires_at: Optional[datetime] = None, is_active: Optional[bool] = None,
) -> APIKey:
    if name is not None:
        api_key.name = name
    if scopes is not None:
        api_key.scopes = scopes
    if expires_at is not None:
        api_key.expires_at = expires_at
    if is_active is not None:
        api_key.is_active = is_active
    db.commit()
    db.refresh(api_key)
    return api_key


def delete_api_key(db: Session, api_key: APIKey) -> bool:
    db.delete(api_key)
    db.commit()
    return True
