from __future__ import annotations

import uuid
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict

from app.models.enums import Platform, RunStatus


class RunScraperRequest(BaseModel):
    """All fields optional and mutually exclusive in effect:
    - keyword_id set: trigger just that one keyword.
    - movie_id set (keyword_id absent): trigger every enabled keyword for that movie.
    - neither set: trigger every enabled keyword across every movie (the daily job's case).

    start_date/end_date only make sense paired with keyword_id (a single ad-hoc
    search, e.g. from the Platforms page) - forwarded as-is to the platform's
    spider, which only Facebook's search currently understands (see
    spider-hub's crawl_request_consumer.py + facebook_search spider).
    """

    keyword_id: uuid.UUID | None = None
    movie_id: uuid.UUID | None = None
    max_pages: int | None = None
    start_date: date | None = None
    end_date: date | None = None


class RunScraperResponse(BaseModel):
    requested: int
    published: int
    run_id: uuid.UUID | None = None


class TriggerTokenRefreshResponse(BaseModel):
    run_id: uuid.UUID


class FacebookTokenStatus(BaseModel):
    valid: bool
    account: str | None = None
    expires_in_seconds: int | None = None


class ScrapeRunLogRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    line: str
    created_at: datetime


class ScrapeRunLogsResponse(BaseModel):
    status: RunStatus
    keyword: str | None = None
    platform: Platform | None = None
    logs: list[ScrapeRunLogRead]


class ActiveRunResponse(BaseModel):
    run_id: uuid.UUID | None
    keyword: str | None = None
    platform: Platform | None = None
