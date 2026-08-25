from __future__ import annotations

from fastapi import APIRouter, Cookie, Depends, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.config import settings
from app.core.errors import AuthenticationError, ConflictError
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    hash_password,
    verify_password,
)
from app.db.session import get_db
from app.models.user import User
from app.schemas.user import UserCreate, UserRead
from app.services.users import UserRepository

router = APIRouter(prefix="/auth", tags=["auth"])

_ACCESS_COOKIE = "access_token"
_REFRESH_COOKIE = "refresh_token"
_REFRESH_COOKIE_PATH = "/auth"


def _set_auth_cookies(response: Response, access_token: str, refresh_token: str) -> None:
    response.set_cookie(
        _ACCESS_COOKIE,
        access_token,
        max_age=settings.access_token_expire_minutes * 60,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        path="/",
    )
    response.set_cookie(
        _REFRESH_COOKIE,
        refresh_token,
        max_age=settings.refresh_token_expire_days * 24 * 3600,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        path=_REFRESH_COOKIE_PATH,
    )


def _clear_auth_cookies(response: Response) -> None:
    response.delete_cookie(_ACCESS_COOKIE, path="/")
    response.delete_cookie(_REFRESH_COOKIE, path=_REFRESH_COOKIE_PATH)


@router.post("/register", response_model=UserRead, status_code=201)
async def register(payload: UserCreate, db: AsyncSession = Depends(get_db)) -> UserRead:
    repo = UserRepository(db)
    if await repo.get_by_username(payload.username) is not None:
        raise ConflictError(f"Username {payload.username!r} is already taken.")
    if await repo.get_by_email(payload.email) is not None:
        raise ConflictError(f"An account with email {payload.email!r} already exists.")

    return await repo.create(
        username=payload.username,
        email=payload.email,
        hashed_password=hash_password(payload.password),
    )


@router.post("/login", response_model=UserRead)
async def login(
    response: Response, form: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)
) -> User:
    repo = UserRepository(db)
    user = await repo.get_by_username(form.username) or await repo.get_by_email(form.username)

    if user is None or not verify_password(form.password, user.hashed_password):
        raise AuthenticationError("Incorrect username/email or password.")
    if not user.is_active:
        raise AuthenticationError("This account is disabled.")

    refresh_token, jti = create_refresh_token(user.id, user.token_version)
    user.current_refresh_jti = jti
    access_token = create_access_token(user.id)
    _set_auth_cookies(response, access_token, refresh_token)

    return await repo.get_with_scope(user.id)


@router.post("/refresh", response_model=UserRead)
async def refresh(
    response: Response,
    refresh_token: str | None = Cookie(default=None, alias=_REFRESH_COOKIE),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Exchanges the still-valid refresh_token cookie for a fresh
    access+refresh pair, so a client stays logged in past
    access_token_expire_minutes without the user re-entering credentials.
    Rotates on every call: the cookie's jti must match
    User.current_refresh_jti exactly, and a match immediately replaces it
    with a freshly generated one - so a refresh token only ever works once.
    Presenting one that's already been rotated past (its jti no longer
    matches) is treated as possible token theft, not just staleness: it
    bumps token_version, revoking every refresh token issued before this
    point instead of only rejecting the one presented."""
    if refresh_token is None:
        raise AuthenticationError("Invalid or expired refresh token.")

    decoded = decode_refresh_token(refresh_token)
    if decoded is None:
        raise AuthenticationError("Invalid or expired refresh token.")
    user_id, token_version, jti = decoded

    user = await UserRepository(db).get_with_scope(user_id)
    if user is None or not user.is_active:
        raise AuthenticationError("Invalid or expired refresh token.")
    if token_version != user.token_version or jti != user.current_refresh_jti:
        user.token_version += 1
        user.current_refresh_jti = None
        # get_db() rolls back on any exception leaving this route, which
        # would otherwise silently discard this exact revocation - commit
        # explicitly before raising so the mismatch permanently sticks
        # instead of only rejecting this one request.
        await db.commit()
        _clear_auth_cookies(response)
        raise AuthenticationError("Invalid or expired refresh token.")

    new_refresh_token, new_jti = create_refresh_token(user.id, user.token_version)
    user.current_refresh_jti = new_jti
    new_access_token = create_access_token(user.id)
    _set_auth_cookies(response, new_access_token, new_refresh_token)

    return user


@router.post("/logout", status_code=204)
async def logout(response: Response) -> None:
    _clear_auth_cookies(response)


@router.get("/me", response_model=UserRead)
async def me(current_user: User = Depends(get_current_user)) -> User:
    return current_user
