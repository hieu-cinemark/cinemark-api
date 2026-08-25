from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from app.models.enums import Platform


class PostRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    movie_id: uuid.UUID
    movie_title: str
    keyword_id: uuid.UUID | None
    keyword_text: str | None
    platform: Platform
    external_id: str
    url: str | None
    author: str | None
    content: str | None
    media: Any | None
    like_count: int
    reply_count: int
    repost_count: int
    quote_count: int
    reshare_count: int
    view_count: int
    posted_at: datetime | None
    scraped_at: datetime
    keyword_match: bool


class PostListResponse(BaseModel):
    items: list[PostRead]
    total: int
