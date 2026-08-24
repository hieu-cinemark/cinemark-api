from __future__ import annotations

import uuid

from sqlalchemy import select

from app.db.repository import BaseRepository
from app.models.keyword import Keyword


class KeywordRepository(BaseRepository[Keyword]):
    model = Keyword

    async def get_by_unique(self, *, movie_id: uuid.UUID, platform: str, keyword: str) -> Keyword | None:
        stmt = select(Keyword).where(
            Keyword.movie_id == movie_id, Keyword.platform == platform, Keyword.keyword == keyword
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_enabled(self, *, movie_id: uuid.UUID | None = None) -> list[Keyword]:
        """All enabled keywords, optionally scoped to one movie - used by
        both the manual "run this movie's keywords" trigger and the daily
        job's "run everything" pass (movie_id=None)."""
        stmt = select(Keyword).where(Keyword.enabled.is_(True))
        if movie_id is not None:
            stmt = stmt.where(Keyword.movie_id == movie_id)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
