from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.models.enums import Platform


class KeywordCreate(BaseModel):
    movie_id: uuid.UUID
    platform: Platform
    keyword: str
    enabled: bool = True


class KeywordUpdate(BaseModel):
    enabled: bool


class KeywordRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    movie_id: uuid.UUID
    platform: Platform
    keyword: str
    enabled: bool
    created_at: datetime
