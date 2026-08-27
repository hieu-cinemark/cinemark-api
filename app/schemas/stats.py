from __future__ import annotations

from pydantic import BaseModel


class PlatformStat(BaseModel):
    platform: str
    count: int
    last_scraped_at: str | None = None


class TimeseriesPoint(BaseModel):
    day: str
    platform: str
    count: int
