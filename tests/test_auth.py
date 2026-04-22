"""
Tests for password hashing. JWT was removed from the stack — the only reason we
still hash a password is to satisfy the `users.hashed_password` NOT NULL column,
and those bytes are never used for HTTP auth. Kept tests cover the hashing
primitive we rely on at user-creation time.
"""
import sys
from pathlib import Path

from passlib.context import CryptContext

sys.path.insert(0, str(Path(__file__).parent.parent))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__truncate_error=False)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


class TestPasswordHashing:
    def test_password_hash_is_different_from_plain(self):
        password = "mysecretpassword"
        assert get_password_hash(password) != password

    def test_password_hash_is_bcrypt_format(self):
        assert get_password_hash("password").startswith("$2")

    def test_verify_correct_password(self):
        password = "mysecretpassword"
        assert verify_password(password, get_password_hash(password)) is True

    def test_verify_incorrect_password(self):
        assert verify_password("wrongpassword", get_password_hash("mysecretpassword")) is False

    def test_different_hashes_for_same_password(self):
        password = "mysecretpassword"
        h1 = get_password_hash(password)
        h2 = get_password_hash(password)
        assert h1 != h2
        assert verify_password(password, h1)
        assert verify_password(password, h2)

    def test_max_bcrypt_length_password(self):
        password = "a" * 72
        assert verify_password(password, get_password_hash(password)) is True
