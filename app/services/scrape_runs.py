from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select

from app.db.repository import BaseRepository
from app.models.scrape_run import ScrapeRun, ScrapeRunLog


class ScrapeRunRepository(BaseRepository[ScrapeRun]):
    model = ScrapeRun

    async def list_logs(self, run_id: uuid.UUID, *, after: datetime | None = None) -> list[ScrapeRunLog]:
        stmt = select(ScrapeRunLog).where(ScrapeRunLog.run_id == run_id).order_by(ScrapeRunLog.created_at)
        if after is not None:
            stmt = stmt.where(ScrapeRunLog.created_at > after)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
