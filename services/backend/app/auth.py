from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt
import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text

from .database import ENGINE

SECURITY = HTTPBearer(auto_error=False)


def jwt_secret() -> str:
    value = os.getenv("JWT_SECRET", "").strip()
    if len(value) < 32:
        raise RuntimeError("JWT_SECRET must be at least 32 characters")
    return value


def hash_password(password: str) -> str:
    if len(password) < 10:
        raise HTTPException(status_code=400, detail="Password must contain at least 10 characters")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def session_hash(session_id: str) -> str:
    """Store only a one-way digest of viewer session IDs in PostgreSQL."""
    return hashlib.sha256(session_id.encode("utf-8")).hexdigest()


def start_login_session(user: dict[str, Any]) -> str | None:
    """
    Start a login session.

    Viewer accounts are deliberately single-session accounts. A new successful
    login replaces active_session_hash, immediately invalidating any older token
    for the same Viewer account. Admin accounts may keep multiple sessions.
    """
    if user.get("role") != "viewer":
        return None

    session_id = secrets.token_urlsafe(32)
    with ENGINE.begin() as conn:
        result = conn.execute(text("""
            UPDATE app_users
            SET active_session_hash = :active_session_hash,
                active_session_started_at = now(),
                updated_at = now()
            WHERE id = :id AND is_active = true AND role = 'viewer'
        """), {
            "id": int(user["id"]),
            "active_session_hash": session_hash(session_id),
        })
    if result.rowcount != 1:
        raise HTTPException(status_code=401, detail="User is disabled or no longer exists")
    return session_id


def create_token(user: dict[str, Any], session_id: str | None = None) -> str:
    now = datetime.now(timezone.utc)
    days = max(1, min(30, int(os.getenv("SESSION_DAYS", "7"))))
    payload = {
        "sub": str(user["id"]),
        "email": user["email"],
        "role": user["role"],
        "pwd": int(user.get("password_changed_at", now).timestamp()) if hasattr(user.get("password_changed_at", now), "timestamp") else 0,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=days)).timestamp()),
    }
    if user.get("role") == "viewer":
        if not session_id:
            raise RuntimeError("Viewer token requires a server-tracked session ID")
        payload["sid"] = session_id
    return jwt.encode(payload, jwt_secret(), algorithm="HS256")


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with ENGINE.connect() as conn:
        row = conn.execute(text("""
            SELECT id, email, password_hash, role, is_active, created_at, updated_at,
                   password_changed_at, active_session_hash, active_session_started_at
            FROM app_users WHERE id = :id
        """), {"id": user_id}).mappings().first()
    return dict(row) if row else None


def get_user_by_email(email: str) -> dict[str, Any] | None:
    with ENGINE.connect() as conn:
        row = conn.execute(text("""
            SELECT id, email, password_hash, role, is_active, created_at, updated_at,
                   password_changed_at, active_session_hash, active_session_started_at
            FROM app_users WHERE lower(email) = lower(:email)
        """), {"email": email.strip()}).mappings().first()
    return dict(row) if row else None


def authenticated_session(
    credentials: HTTPAuthorizationCredentials | None = Depends(SECURITY),
) -> dict[str, Any]:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(credentials.credentials, jwt_secret(), algorithms=["HS256"])
        user_id = int(payload["sub"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    user = get_user_by_id(user_id)
    if not user or not user["is_active"]:
        raise HTTPException(status_code=401, detail="User is disabled or no longer exists")

    changed = user.get("password_changed_at")
    changed_ts = int(changed.timestamp()) if hasattr(changed, "timestamp") else 0
    if int(payload.get("pwd", 0)) < changed_ts:
        raise HTTPException(status_code=401, detail="Session expired after password change")

    if user.get("role") == "viewer":
        session_id = str(payload.get("sid") or "")
        active_hash = str(user.get("active_session_hash") or "")
        if not session_id or not active_hash or not hmac.compare_digest(session_hash(session_id), active_hash):
            raise HTTPException(
                status_code=401,
                detail="This Viewer account is active in another session. Please sign in again to use this device.",
            )

    return {"user": user, "payload": payload}


def current_user(session: dict[str, Any] = Depends(authenticated_session)) -> dict[str, Any]:
    return session["user"]


def require_admin(user: dict[str, Any] = Depends(current_user)) -> dict[str, Any]:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Administrator access required")
    return user


def revoke_current_session(session: dict[str, Any]) -> None:
    """Revoke the current Viewer session. Admin logout remains client-cookie based."""
    user = session["user"]
    if user.get("role") != "viewer":
        return
    session_id = str(session.get("payload", {}).get("sid") or "")
    if not session_id:
        return
    expected_hash = session_hash(session_id)
    with ENGINE.begin() as conn:
        conn.execute(text("""
            UPDATE app_users
            SET active_session_hash = NULL,
                active_session_started_at = NULL,
                updated_at = now()
            WHERE id = :id AND active_session_hash = :active_session_hash
        """), {
            "id": int(user["id"]),
            "active_session_hash": expected_hash,
        })


def public_user(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": user["id"],
        "email": user["email"],
        "role": user["role"],
        "is_active": bool(user["is_active"]),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
    }


def seed_initial_admin() -> None:
    email = os.getenv("INITIAL_ADMIN_EMAIL", "").strip().lower()
    password = os.getenv("INITIAL_ADMIN_PASSWORD", "")
    if not email or not password:
        print("[AUTH] INITIAL_ADMIN_EMAIL/PASSWORD not set; no initial admin seeded.", flush=True)
        return
    with ENGINE.begin() as conn:
        admin_count = conn.execute(text("SELECT count(*) FROM app_users WHERE role='admin'" )).scalar_one()
        if int(admin_count or 0) > 0:
            print("[AUTH] Existing admin found; seed skipped.", flush=True)
            return
        conn.execute(text("""
            INSERT INTO app_users (email, password_hash, role, is_active)
            VALUES (:email, :password_hash, 'admin', true)
            ON CONFLICT (email) DO NOTHING
        """), {"email": email, "password_hash": hash_password(password)})
        print(f"[AUTH] Initial admin created: {email}", flush=True)
