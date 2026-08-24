"""Movie-specific repository - inherits get/list/create/update/delete from
BaseRepository, only adds queries that are actually specific to movies."""

from __future__ import annotations

from sqlalchemy import select

from app.db.repository import BaseRepository
from app.models.movie import Movie


class MovieRepository(BaseRepository[Movie]):
    model = Movie

    async def get_by_slug(self, slug: str) -> Movie | None:
        result = await self.session.execute(select(Movie).where(Movie.slug == slug))
        return result.scalar_one_or_none()
