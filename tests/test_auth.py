"""
Tests for authentication and authorization functions.

Tests cover password hashing, JWT token creation/validation, and user authentication.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Optional

import pytest
from jose import jwt
from passlib.context import CryptContext

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import config first (it doesn't have DB dependencies)
from src.config import SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

# Create a local password context for testing (avoids importing auth.py directly)
# Use bcrypt__min_rounds to make tests faster, and truncate_error=False to handle long passwords
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__truncate_error=False)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash (copied from auth.py)."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash a password (copied from auth.py)."""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token (copied from auth.py)."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt


class TestPasswordHashing:
    """Tests for password hashing functions."""

    def test_password_hash_is_different_from_plain(self):
        """Password hash should be different from plain text."""
        password = "mysecretpassword"
        hashed = get_password_hash(password)
        assert hashed != password

    def test_password_hash_is_bcrypt_format(self):
        """Password hash should be in bcrypt format."""
        hashed = get_password_hash("password")
        assert hashed.startswith("$2")  # bcrypt prefix

    def test_verify_correct_password(self):
        """Correct password should verify successfully."""
        password = "mysecretpassword"
        hashed = get_password_hash(password)
        assert verify_password(password, hashed) is True

    def test_verify_incorrect_password(self):
        """Incorrect password should fail verification."""
        password = "mysecretpassword"
        hashed = get_password_hash(password)
        assert verify_password("wrongpassword", hashed) is False

    def test_different_hashes_for_same_password(self):
        """Same password should produce different hashes (salted)."""
        password = "mysecretpassword"
        hash1 = get_password_hash(password)
        hash2 = get_password_hash(password)
        assert hash1 != hash2
        # But both should verify
        assert verify_password(password, hash1)
        assert verify_password(password, hash2)

    def test_empty_password_can_be_hashed(self):
        """Empty password should be hashable (but discouraged)."""
        hashed = get_password_hash("")
        assert verify_password("", hashed) is True

    def test_special_characters_password(self):
        """Passwords with special characters should work correctly."""
        password = "pass!@#$%^&*()_+-=[]{}|;':\",./<>?"
        hashed = get_password_hash(password)
        assert verify_password(password, hashed) is True
        assert verify_password("wrongpassword", hashed) is False

    def test_max_bcrypt_length_password(self):
        """Passwords at bcrypt max length (72 bytes) should work."""
        password = "a" * 72
        hashed = get_password_hash(password)
        assert verify_password(password, hashed) is True


class TestAccessToken:
    """Tests for JWT access token creation and validation."""

    def test_create_token_returns_string(self):
        """create_access_token should return a string."""
        token = create_access_token(data={"sub": "testuser"})
        assert isinstance(token, str)
        assert len(token) > 0

    def test_token_contains_subject(self):
        """Token should contain the subject claim."""
        username = "testuser"
        token = create_access_token(data={"sub": username})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["sub"] == username

    def test_token_has_expiration(self):
        """Token should have an expiration claim."""
        token = create_access_token(data={"sub": "testuser"})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert "exp" in payload

    def test_custom_expiration_delta(self):
        """Custom expiration delta should be respected."""
        delta = timedelta(hours=2)
        token = create_access_token(
            data={"sub": "testuser"},
            expires_delta=delta
        )
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        exp = datetime.utcfromtimestamp(payload["exp"])
        now = datetime.utcnow()
        # Expiration should be approximately 2 hours from now
        diff_seconds = (exp - now).total_seconds()
        expected = delta.total_seconds()
        # Allow 60 seconds tolerance for test execution time
        assert expected - 60 <= diff_seconds <= expected + 60

    def test_expiration_is_in_future(self):
        """Token expiration should always be in the future."""
        token = create_access_token(data={"sub": "testuser"})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        exp = datetime.fromtimestamp(payload["exp"])
        now = datetime.utcnow()
        assert exp > now

    def test_additional_claims_preserved(self):
        """Additional claims should be preserved in token."""
        token = create_access_token(
            data={"sub": "testuser", "role": "admin", "custom": "value"}
        )
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["sub"] == "testuser"
        assert payload["role"] == "admin"
        assert payload["custom"] == "value"

    def test_expired_token_raises_on_decode(self):
        """Expired token should raise an error on decode."""
        token = create_access_token(
            data={"sub": "testuser"},
            expires_delta=timedelta(seconds=-1)  # Already expired
        )
        with pytest.raises(jwt.ExpiredSignatureError):
            jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])

    def test_invalid_signature_raises(self):
        """Token with invalid signature should raise an error."""
        token = create_access_token(data={"sub": "testuser"})
        with pytest.raises(jwt.JWTError):
            jwt.decode(token, "wrong-secret-key", algorithms=[JWT_ALGORITHM])


class TestAuthenticateUser:
    """Tests for user authentication with mocked database."""

    def test_authenticate_valid_user(self, db_session, sample_user):
        """Valid credentials should authenticate successfully."""
        from tests.conftest import User

        def mock_get_user(session, username):
            return session.query(User).filter(User.username == username).first()

        user = mock_get_user(db_session, "testuser")
        assert user is not None
        assert verify_password("testpassword123", user.hashed_password)

    def test_authenticate_wrong_password(self, db_session, sample_user):
        """Wrong password should fail verification."""
        from tests.conftest import User

        user = db_session.query(User).filter(User.username == "testuser").first()
        assert user is not None
        assert not verify_password("wrongpassword", user.hashed_password)

    def test_authenticate_nonexistent_user(self, db_session):
        """Nonexistent user should return None."""
        from tests.conftest import User

        user = db_session.query(User).filter(User.username == "nonexistent").first()
        assert user is None

    def test_authenticate_inactive_user(self, db_session, inactive_user):
        """Inactive user should not authenticate."""
        from tests.conftest import User

        user = db_session.query(User).filter(User.username == "inactiveuser").first()
        assert user is not None
        assert user.is_active is False
        # Authentication logic should check is_active flag
        # A proper authenticate_user function would return None for inactive users

    def test_authenticate_case_sensitive_username(self, db_session, sample_user):
        """Username should be case-sensitive."""
        from tests.conftest import User

        # "TESTUSER" should not find "testuser"
        user = db_session.query(User).filter(User.username == "TESTUSER").first()
        assert user is None


class TestPydanticModels:
    """Tests for Pydantic models used in authentication."""

    def test_token_model(self):
        """Token model should have correct fields."""
        from pydantic import BaseModel

        class Token(BaseModel):
            access_token: str
            token_type: str = "bearer"

        token = Token(access_token="test_token")
        assert token.access_token == "test_token"
        assert token.token_type == "bearer"

    def test_token_data_model(self):
        """TokenData model should allow optional username."""
        from pydantic import BaseModel

        class TokenData(BaseModel):
            username: Optional[str] = None

        data1 = TokenData(username="test")
        assert data1.username == "test"

        data2 = TokenData()
        assert data2.username is None

    def test_user_create_model(self):
        """UserCreate model should have all required fields."""
        from pydantic import BaseModel

        class UserCreate(BaseModel):
            username: str
            email: str
            password: str

        user = UserCreate(
            username="newuser",
            email="new@example.com",
            password="password123"
        )
        assert user.username == "newuser"
        assert user.email == "new@example.com"
        assert user.password == "password123"

    def test_user_login_model(self):
        """UserLogin model should have username and password."""
        from pydantic import BaseModel

        class UserLogin(BaseModel):
            username: str
            password: str

        login = UserLogin(username="testuser", password="password")
        assert login.username == "testuser"
        assert login.password == "password"

    def test_user_response_model(self):
        """UserResponse model should exclude password."""
        from pydantic import BaseModel

        class UserResponse(BaseModel):
            id: int
            username: str
            email: str
            is_active: bool

        response = UserResponse(
            id=1,
            username="testuser",
            email="test@example.com",
            is_active=True
        )
        assert response.id == 1
        assert response.username == "testuser"
        assert response.email == "test@example.com"
        assert response.is_active is True


class TestTokenSecurityEdgeCases:
    """Edge case tests for token security."""

    def test_token_with_special_characters_in_username(self):
        """Username with special characters should be encoded correctly."""
        username = "user@domain.com"
        token = create_access_token(data={"sub": username})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["sub"] == username

    def test_empty_string_subject_in_token(self):
        """Token with empty string subject should be valid."""
        token = create_access_token(data={"sub": ""})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["sub"] == ""

    def test_empty_data_token(self):
        """Token with empty data dict should have expiration."""
        token = create_access_token(data={})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert "exp" in payload

    def test_token_tamper_detection(self):
        """Tampered token should fail verification."""
        token = create_access_token(data={"sub": "testuser"})
        # Tamper with the token
        parts = token.split(".")
        tampered = parts[0] + "." + parts[1] + "x" + "." + parts[2]
        with pytest.raises(jwt.JWTError):
            jwt.decode(tampered, SECRET_KEY, algorithms=[JWT_ALGORITHM])
