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


class Post(BaseModel):
    id: str
    platform: str
    external_id: str
    url: str | None = None
    author: str | None = None
    content: str | None = None
    media_type: str | int | None = None
    media_url: str | None = None
    like_count: int
    reply_count: int
    repost_count: int
    quote_count: int
    reshare_count: int
    view_count: int
    posted_at: str | None = None
    scraped_at: str
    keyword_match: bool
    keyword: str | None = None
    movie_title: str | None = None


class PostPage(BaseModel):
    items: list[Post]
    total: int
    limit: int
    offset: int
