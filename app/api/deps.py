"""Route dependencies shared across app/api/routes/* - currently just
authentication. Any route that needs the logged-in user adds
`user: User = Depends(get_current_user)` to its signature; a route that
needs an admin adds `user: User = Depends(require_admin)` instead."""

from __future__ import annotations

from fastapi import Depends
from fastapi.security import APIKeyCookie
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import AuthenticationError, AuthorizationError
from app.core.security import decode_access_token
from app.db.session import get_db
from app.models.user import User
from app.services.users import UserRepository

access_token_cookie = APIKeyCookie(name="access_token", auto_error=False)


async def get_current_user(
    token: str | None = Depends(access_token_cookie), db: AsyncSession = Depends(get_db)
) -> User:
    if token is None:
        raise AuthenticationError("Not authenticated.")

    user_id = decode_access_token(token)
    if user_id is None:
        raise AuthenticationError("Invalid or expired token.")

    user = await UserRepository(db).get_with_scope(user_id)
    if user is None:
        raise AuthenticationError("Invalid or expired token.")
    if not user.is_active:
        raise AuthenticationError("This account is disabled.")

    return user


async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.scope is None or current_user.scope.key != "admin":
        raise AuthorizationError("Admin access required.")
    return current_user
