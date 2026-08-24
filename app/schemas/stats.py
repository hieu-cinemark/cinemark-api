from __future__ import annotations

from datetime import date

from pydantic import BaseModel

from app.models.enums import Platform


class OverviewStats(BaseModel):
    movies: int
    keywords: int
    posts: int
    comments: int


class PostsTimeseriesPoint(BaseModel):
    day: date
    platform: Platform
    count: int
