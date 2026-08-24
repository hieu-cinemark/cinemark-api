from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.repository import BaseRepository
from app.models.user import User


class UserRepository(BaseRepository[User]):
    model = User

    async def get_by_username(self, username: str) -> User | None:
        result = await self.session.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()

    async def get_by_email(self, email: str) -> User | None:
        result = await self.session.execute(select(User).where(User.email == email))
        return result.scalar_one_or_none()

    async def get_with_scope(self, id: uuid.UUID) -> User | None:
        stmt = select(User).where(User.id == id).options(selectinload(User.scope))
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
