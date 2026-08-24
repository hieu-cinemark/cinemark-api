from __future__ import annotations

from pydantic import BaseModel, Field


class SettingRead(BaseModel):
    key: str
    value: str
    # True for values the FE should mask-by-default with a reveal toggle
    # (passwords/cookies/tokens) - the endpoint itself is already
    # admin-only (see require_admin), this is a display hint, not access
    # control.
    is_secret: bool


class SettingUpdate(BaseModel):
    value: str = Field(min_length=1)
