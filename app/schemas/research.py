"""Tham số tìm Google News Light; không nhận key/URL từ client."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class GoogleNewsRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    q: str = Field(min_length=1, max_length=10000)
    gl: str = Field(default="vn", pattern=r"^[a-z]{2}$")
    hl: str = Field(default="vi", pattern=r"^[a-z]{2}(?:-[A-Za-z]{2,4})?$")
    location: str | None = Field(default=None, min_length=1, max_length=256)
    device: Literal["desktop", "tablet", "mobile"] = "desktop"
    start: int = Field(default=0, ge=0, le=1000)
    no_cache: bool = False
