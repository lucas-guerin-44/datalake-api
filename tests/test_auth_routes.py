"""
Tests for /auth/api-keys CRUD endpoints.

Patches the real database engine to use in-memory SQLite so the full
ScopedAuth → authenticate_api_key → bcrypt verification path runs end-to-end.
"""
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("RATE_LIMIT_ENABLED", "false")

sys.modules.setdefault("psycopg2", MagicMock())
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient

from src.core.database import Base, User, APIKey
from src.auth.auth import generate_api_key, hash_api_key


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def setup_sqlite_db(monkeypatch):
    """Replace the production Postgres engine with in-memory SQLite for every test."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    import src.core.database as db_mod
    monkeypatch.setattr(db_mod, "engine", engine)
    monkeypatch.setattr(db_mod, "SessionLocal", TestSession)

    yield engine

    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def admin_user(setup_sqlite_db):
    """Create an admin user in the test DB and return (user, api_key_plaintext)."""
    session = sessionmaker(bind=setup_sqlite_db)()
    user = User(username="admin", email="admin@test.com", hashed_password="x", is_active=True)
    session.add(user)
    session.commit()
    session.refresh(user)

    full_key, prefix = generate_api_key()
    key_record = APIKey(
        user_id=user.id,
        key_hash=hash_api_key(full_key),
        prefix=prefix,
        name="admin-key",
        scopes=["admin"],
        is_active=True,
    )
    session.add(key_record)
    session.commit()
    session.close()

    return user, full_key


@pytest.fixture()
def client():
    from src.api import app
    with patch("src.api.init_db"):
        return TestClient(app, raise_server_exceptions=False)


def _auth_header(key: str) -> dict:
    return {"X-API-Key": key}


# ── POST /auth/api-keys ─────────────────────────────────────────────────────


class TestCreateApiKey:
    def test_create_key_returns_full_key(self, client, admin_user):
        _, admin_key = admin_user
        resp = client.post(
            "/auth/api-keys",
            json={"name": "new-key", "scopes": ["read"], "expires_in_days": 30},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["key"].startswith("dk_")
        assert data["prefix"] == data["key"][:12]
        assert data["name"] == "new-key"
        assert data["scopes"] == ["read"]
        assert data["is_active"] is True
        assert data["expires_at"] is not None

    def test_create_key_without_expiry(self, client, admin_user):
        _, admin_key = admin_user
        resp = client.post(
            "/auth/api-keys",
            json={"name": "no-expiry", "scopes": ["write"]},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 201
        assert resp.json()["expires_at"] is None

    def test_create_key_rejects_invalid_scopes(self, client, admin_user):
        _, admin_key = admin_user
        resp = client.post(
            "/auth/api-keys",
            json={"name": "bad", "scopes": ["invalid_scope"]},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 400

    def test_create_key_requires_admin(self, client, admin_user):
        _, admin_key = admin_user
        # Create a read-only key, then try to use it for admin operation
        resp = client.post(
            "/auth/api-keys",
            json={"name": "read-key", "scopes": ["read"]},
            headers=_auth_header(admin_key),
        )
        read_key = resp.json()["key"]

        resp2 = client.post(
            "/auth/api-keys",
            json={"name": "should-fail", "scopes": ["read"]},
            headers=_auth_header(read_key),
        )
        assert resp2.status_code == 403

    def test_create_key_requires_auth(self, client):
        resp = client.post("/auth/api-keys", json={"name": "x", "scopes": ["read"]})
        assert resp.status_code == 401


# ── GET /auth/api-keys ──────────────────────────────────────────────────────


class TestListApiKeys:
    def test_lists_keys_for_user(self, client, admin_user):
        _, admin_key = admin_user
        # Create two keys
        client.post("/auth/api-keys", json={"name": "k1", "scopes": ["read"]}, headers=_auth_header(admin_key))
        client.post("/auth/api-keys", json={"name": "k2", "scopes": ["write"]}, headers=_auth_header(admin_key))

        resp = client.get("/auth/api-keys", headers=_auth_header(admin_key))
        assert resp.status_code == 200
        keys = resp.json()
        assert len(keys) == 3  # admin-key + k1 + k2
        names = {k["name"] for k in keys}
        assert "k1" in names
        assert "k2" in names

    def test_list_does_not_include_full_key(self, client, admin_user):
        _, admin_key = admin_user
        resp = client.get("/auth/api-keys", headers=_auth_header(admin_key))
        for k in resp.json():
            assert "key" not in k  # full key should never be in list response


# ── GET /auth/api-keys/{id} ─────────────────────────────────────────────────


class TestGetApiKey:
    def test_get_key_by_id(self, client, admin_user):
        _, admin_key = admin_user
        create_resp = client.post(
            "/auth/api-keys", json={"name": "fetch-me", "scopes": ["read"]},
            headers=_auth_header(admin_key),
        )
        key_id = create_resp.json()["id"]

        resp = client.get(f"/auth/api-keys/{key_id}", headers=_auth_header(admin_key))
        assert resp.status_code == 200
        assert resp.json()["name"] == "fetch-me"

    def test_get_nonexistent_key_returns_404(self, client, admin_user):
        _, admin_key = admin_user
        resp = client.get("/auth/api-keys/99999", headers=_auth_header(admin_key))
        assert resp.status_code == 404


# ── PATCH /auth/api-keys/{id} ───────────────────────────────────────────────


class TestUpdateApiKey:
    def test_update_name(self, client, admin_user):
        _, admin_key = admin_user
        create_resp = client.post(
            "/auth/api-keys", json={"name": "old-name", "scopes": ["read"]},
            headers=_auth_header(admin_key),
        )
        key_id = create_resp.json()["id"]

        resp = client.patch(
            f"/auth/api-keys/{key_id}",
            json={"name": "new-name"},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == "new-name"

    def test_update_scopes(self, client, admin_user):
        _, admin_key = admin_user
        create_resp = client.post(
            "/auth/api-keys", json={"name": "scope-test", "scopes": ["read"]},
            headers=_auth_header(admin_key),
        )
        key_id = create_resp.json()["id"]

        resp = client.patch(
            f"/auth/api-keys/{key_id}",
            json={"scopes": ["read", "write"]},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 200
        assert resp.json()["scopes"] == ["read", "write"]

    def test_deactivate_key(self, client, admin_user):
        _, admin_key = admin_user
        create_resp = client.post(
            "/auth/api-keys", json={"name": "toggle", "scopes": ["read"]},
            headers=_auth_header(admin_key),
        )
        key_id = create_resp.json()["id"]

        resp = client.patch(
            f"/auth/api-keys/{key_id}",
            json={"is_active": False},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 200
        assert resp.json()["is_active"] is False

    def test_set_expiry_to_zero_clears_it(self, client, admin_user):
        _, admin_key = admin_user
        create_resp = client.post(
            "/auth/api-keys",
            json={"name": "exp-test", "scopes": ["read"], "expires_in_days": 30},
            headers=_auth_header(admin_key),
        )
        key_id = create_resp.json()["id"]
        assert create_resp.json()["expires_at"] is not None

        resp = client.patch(
            f"/auth/api-keys/{key_id}",
            json={"expires_in_days": 0},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 200
        assert resp.json()["expires_at"] is None

    def test_update_rejects_invalid_scopes(self, client, admin_user):
        _, admin_key = admin_user
        create_resp = client.post(
            "/auth/api-keys", json={"name": "x", "scopes": ["read"]},
            headers=_auth_header(admin_key),
        )
        key_id = create_resp.json()["id"]

        resp = client.patch(
            f"/auth/api-keys/{key_id}",
            json={"scopes": ["bogus"]},
            headers=_auth_header(admin_key),
        )
        assert resp.status_code == 400


# ── DELETE /auth/api-keys/{id} ──────────────────────────────────────────────


class TestRevokeApiKey:
    def test_revoke_returns_204(self, client, admin_user):
        _, admin_key = admin_user
        create_resp = client.post(
            "/auth/api-keys", json={"name": "revoke-me", "scopes": ["read"]},
            headers=_auth_header(admin_key),
        )
        key_id = create_resp.json()["id"]

        resp = client.delete(f"/auth/api-keys/{key_id}", headers=_auth_header(admin_key))
        assert resp.status_code == 204

        # Verify it's gone
        resp2 = client.get(f"/auth/api-keys/{key_id}", headers=_auth_header(admin_key))
        assert resp2.status_code == 404

    def test_revoke_nonexistent_returns_404(self, client, admin_user):
        _, admin_key = admin_user
        resp = client.delete("/auth/api-keys/99999", headers=_auth_header(admin_key))
        assert resp.status_code == 404


# ── Cross-user isolation ────────────────────────────────────────────────────


class TestCrossUserIsolation:
    def test_user_cannot_see_other_users_keys(self, client, setup_sqlite_db):
        """Two admin users should not see each other's keys."""
        session = sessionmaker(bind=setup_sqlite_db)()

        # User A
        user_a = User(username="userA", email="a@test.com", hashed_password="x", is_active=True)
        session.add(user_a)
        session.commit()
        session.refresh(user_a)
        full_key_a, prefix_a = generate_api_key()
        session.add(APIKey(user_id=user_a.id, key_hash=hash_api_key(full_key_a), prefix=prefix_a, name="a-key", scopes=["admin"], is_active=True))
        session.commit()

        # User B
        user_b = User(username="userB", email="b@test.com", hashed_password="x", is_active=True)
        session.add(user_b)
        session.commit()
        session.refresh(user_b)
        full_key_b, prefix_b = generate_api_key()
        session.add(APIKey(user_id=user_b.id, key_hash=hash_api_key(full_key_b), prefix=prefix_b, name="b-key", scopes=["admin"], is_active=True))
        session.commit()
        session.close()

        # A lists — should only see A's keys
        resp = client.get("/auth/api-keys", headers=_auth_header(full_key_a))
        assert resp.status_code == 200
        names = {k["name"] for k in resp.json()}
        assert "a-key" in names
        assert "b-key" not in names

        # A tries to get B's key by ID — should 404
        b_key_id = session.query(APIKey).filter(APIKey.prefix == prefix_b).first().id
        resp2 = client.get(f"/auth/api-keys/{b_key_id}", headers=_auth_header(full_key_a))
        assert resp2.status_code == 404
