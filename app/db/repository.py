"""Generic base repository - the common get/list/create/update/delete
operations every resource (movies, keywords, posts, ...) needs, so each
concrete repository only adds resource-specific queries instead of
re-implementing basic CRUD every time. Subclass and set `model`:

    class MovieRepository(BaseRepository[Movie]):
        model = Movie

        async def get_by_slug(self, slug: str) -> Movie | None:
            result = await self.session.execute(select(Movie).where(Movie.slug == slug))
            return result.scalar_one_or_none()

Methods flush (not commit) - get_db() in app/db/session.py owns the
commit/rollback boundary for the whole request, so a service that calls
into several repositories stays one atomic transaction.
"""

from __future__ import annotations

import uuid
from typing import Any, ClassVar, Generic, TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import NotFoundError
from app.models.base import Base

ModelType = TypeVar("ModelType", bound=Base)


class BaseRepository(Generic[ModelType]):
    model: ClassVar[type[Base]]

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, id: uuid.UUID) -> ModelType | None:
        return await self.session.get(self.model, id)

    async def get_or_404(self, id: uuid.UUID) -> ModelType:
        obj = await self.get(id)
        if obj is None:
            raise NotFoundError(f"{self.model.__name__} {id} not found")
        return obj

    async def list(self, *, offset: int = 0, limit: int = 50, **filters: Any) -> list[ModelType]:
        stmt = select(self.model)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        stmt = stmt.offset(offset).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def create(self, **data: Any) -> ModelType:
        obj = self.model(**data)
        self.session.add(obj)
        await self.session.flush()
        return obj

    async def update(self, obj: ModelType, **data: Any) -> ModelType:
        for field, value in data.items():
            setattr(obj, field, value)
        await self.session.flush()
        return obj

    async def delete(self, obj: ModelType) -> None:
        await self.session.delete(obj)
        await self.session.flush()
