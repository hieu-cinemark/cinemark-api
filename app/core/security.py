"""Password hashing (bcrypt) and JWT access tokens - the two primitives
app.api.routes.auth builds register/login on top of. Kept separate from
app.models.user on purpose: hashing/token concerns don't belong on the ORM
model, and this module has zero DB dependency, so it's trivially testable
on its own."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt
import jwt

from app.core.config import settings


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8"))


def create_access_token(user_id: uuid.UUID) -> str:
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "iat": now,
        "exp": now + timedelta(minutes=settings.access_token_expire_minutes),
        "type": "access",
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> uuid.UUID | None:
    """Returns the user id encoded in a valid, non-expired access token, or
    None for anything invalid/expired/tampered-with/wrong-type - callers
    turn a None into a 401, this module doesn't know about HTTP."""
    payload = _decode(token)
    if payload is None or payload.get("type") != "access":
        return None
    try:
        return uuid.UUID(payload["sub"])
    except (KeyError, ValueError):
        return None


def create_refresh_token(user_id: uuid.UUID, token_version: int) -> tuple[str, str]:
    """Long-lived companion to create_access_token, meant to be exchanged
    for a fresh access token (see POST /auth/refresh) instead of forcing a
    full re-login every access_token_expire_minutes. Embeds the user's
    current token_version so bumping it (e.g. on password change) silently
    invalidates every refresh token issued before the bump - no revocation
    table needed. Returns (token, jti) - the caller stores jti on
    User.current_refresh_jti so a later refresh can detect reuse of an
    already-rotated-out token (see decode_refresh_token / POST
    /auth/refresh) rather than only checking expiry."""
    now = datetime.now(timezone.utc)
    jti = uuid.uuid4().hex
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "iat": now,
        "exp": now + timedelta(days=settings.refresh_token_expire_days),
        "type": "refresh",
        "ver": token_version,
        "jti": jti,
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm), jti


def decode_refresh_token(token: str) -> tuple[uuid.UUID, int, str] | None:
    """Returns (user_id, token_version, jti) for a valid, non-expired
    refresh token, or None for anything invalid/expired/tampered-with/
    wrong-type. The caller (POST /auth/refresh) still has to check
    token_version and jti against the user's current ones - a
    valid-but-superseded refresh token decodes fine here, those checks are
    what actually reject it."""
    payload = _decode(token)
    if payload is None or payload.get("type") != "refresh":
        return None
    try:
        return uuid.UUID(payload["sub"]), int(payload["ver"]), str(payload["jti"])
    except (KeyError, ValueError, TypeError):
        return None


def _decode(token: str) -> dict[str, Any] | None:
    try:
        return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except jwt.PyJWTError:
        return None
