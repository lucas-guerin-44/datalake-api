"""
Tests for User model and CRUD operations.
Uses test models from conftest to avoid PostgreSQL dependency.
"""
import sys
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

sys.path.insert(0, str(Path(__file__).parent.parent))

from tests.conftest import User


def create_user(db, username: str, email: str, hashed_password: str) -> User:
    """Create a new user."""
    user = User(username=username, email=email, hashed_password=hashed_password)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def get_user_by_username(db, username: str):
    return db.query(User).filter(User.username == username).first()


def get_user_by_email(db, email: str):
    return db.query(User).filter(User.email == email).first()


def get_user_by_id(db, user_id: int):
    return db.query(User).filter(User.id == user_id).first()


class TestUserModel:
    """Tests for User model."""

    def test_create_user_success(self, db_session):
        user = User(
            username="newuser", email="newuser@example.com",
            hashed_password="hashed_password_here", is_active=True,
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)

        assert user.id is not None
        assert user.username == "newuser"
        assert user.email == "newuser@example.com"
        assert user.is_active is True
        assert user.created_at is not None

    def test_user_to_dict(self, sample_user):
        user_dict = sample_user.to_dict()
        assert user_dict["id"] == sample_user.id
        assert user_dict["username"] == "testuser"
        assert user_dict["email"] == "test@example.com"
        assert user_dict["is_active"] is True
        assert "hashed_password" not in user_dict

    def test_unique_username_constraint(self, db_session, sample_user):
        duplicate = User(username="testuser", email="different@example.com", hashed_password="hash")
        db_session.add(duplicate)
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_unique_email_constraint(self, db_session, sample_user):
        duplicate = User(username="differentuser", email="test@example.com", hashed_password="hash")
        db_session.add(duplicate)
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestUserOperations:
    """Tests for user CRUD operations."""

    def test_create_user_function(self, db_session):
        user = create_user(db=db_session, username="createduser", email="created@example.com", hashed_password="hashed_password")
        assert user.id is not None
        assert user.username == "createduser"

    def test_get_user_by_username(self, db_session, sample_user):
        user = get_user_by_username(db_session, "testuser")
        assert user is not None
        assert user.id == sample_user.id

    def test_get_user_by_username_not_found(self, db_session):
        assert get_user_by_username(db_session, "nonexistent") is None

    def test_get_user_by_email(self, db_session, sample_user):
        user = get_user_by_email(db_session, "test@example.com")
        assert user is not None
        assert user.id == sample_user.id

    def test_get_user_by_email_not_found(self, db_session):
        assert get_user_by_email(db_session, "nonexistent@example.com") is None

    def test_get_user_by_id(self, db_session, sample_user):
        user = get_user_by_id(db_session, sample_user.id)
        assert user is not None
        assert user.username == "testuser"

    def test_get_user_by_id_not_found(self, db_session):
        assert get_user_by_id(db_session, 99999) is None
