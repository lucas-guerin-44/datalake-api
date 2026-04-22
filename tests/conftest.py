"""
Pytest configuration and fixtures for datalake-api tests.

Uses in-memory SQLite to avoid requiring PostgreSQL for unit tests.
"""
import os

# Set required environment variables before importing src modules.
# ScopedAuth / ws_require_auth capture ALLOW_PUBLIC_READS at route-import time, so it
# must be set before any src module loads — not inside a fixture.
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing")
os.environ.setdefault("RATE_LIMIT_ENABLED", "false")
os.environ.setdefault("ALLOW_REGISTRATION", "true")
os.environ.setdefault("ALLOW_PUBLIC_READS", "true")

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).parent.parent))

Base = declarative_base()


class User(Base):
    """Test User model (mirrors database.User without PostgreSQL dependency)."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


TEST_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture(scope="function")
def test_engine():
    """Create a test database engine with in-memory SQLite."""
    engine = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session(test_engine) -> Generator[Session, None, None]:
    """Create a test database session."""
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


def get_password_hash(password: str) -> str:
    """Hash a password using bcrypt."""
    from passlib.context import CryptContext
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__truncate_error=False)
    return pwd_context.hash(password)


@pytest.fixture
def sample_user(db_session: Session) -> User:
    """Create a sample active user."""
    user = User(
        username="testuser",
        email="test@example.com",
        hashed_password=get_password_hash("testpassword123"),
        is_active=True,
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def inactive_user(db_session: Session) -> User:
    """Create an inactive user."""
    user = User(
        username="inactiveuser",
        email="inactive@example.com",
        hashed_password=get_password_hash("password123"),
        is_active=False,
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


def create_access_token(data: dict, expires_delta: timedelta = None) -> str:
    """Create a JWT access token for testing."""
    from jose import jwt
    from src.config import SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=JWT_ALGORITHM)


@pytest.fixture
def valid_token(sample_user: User) -> str:
    """Create a valid JWT token."""
    return create_access_token(data={"sub": sample_user.username})


@pytest.fixture
def expired_token(sample_user: User) -> str:
    """Create an expired JWT token."""
    return create_access_token(data={"sub": sample_user.username}, expires_delta=timedelta(minutes=-10))


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing."""
    env_vars = {
        "SECRET_KEY": "test-secret-key-for-testing",
        "JWT_ALGORITHM": "HS256",
        "ACCESS_TOKEN_EXPIRE_MINUTES": "60",
        "ALLOW_PUBLIC_READS": "true",
        "DATABASE_URL": TEST_DATABASE_URL,
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars
