from __future__ import annotations

import uuid
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class MovieCreate(BaseModel):
    title: str
    slug: str
    enabled: bool = True
    released_at: date | None = None


class MovieRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    slug: str
    enabled: bool
    released_at: date | None
    created_at: datetime
    updated_at: datetime
