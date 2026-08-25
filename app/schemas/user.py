from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    email: EmailStr
    password: str = Field(min_length=8)


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    username: str
    email: str
    is_active: bool
    is_superuser: bool
    scope: str | None = None
    created_at: datetime

    @field_validator("scope", mode="before")
    @classmethod
    def _scope_key(cls, value: object) -> str | None:
        if value is None or isinstance(value, str):
            return value
        return value.key
