from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class RunScraperRequest(BaseModel):
    """All fields optional and mutually exclusive in effect:
    - keyword_id set: trigger just that one keyword.
    - movie_id set (keyword_id absent): trigger every enabled keyword for that movie.
    - neither set: trigger every enabled keyword across every movie (the daily job's case).

    keyword_id/movie_id are D1 ids (see app/services/d1.py), not UUIDs.

    start_date/end_date only make sense paired with keyword_id (a single ad-hoc
    search, e.g. from the Platforms page) - forwarded as-is to the platform's
    spider, which only Facebook's search currently understands (see
    spider-hub's crawl_request_consumer.py + facebook_search spider).
    """

    keyword_id: str | None = None
    movie_id: str | None = None
    max_pages: int | None = None
    start_date: date | None = None
    end_date: date | None = None


class RunScraperResponse(BaseModel):
    requested: int
    published: int


class TriggerTokenRefreshResponse(BaseModel):
    ok: bool


class TokenStatus(BaseModel):
    valid: bool
    account: str | None = None
    expires_in_seconds: int | None = None


class KeywordOut(BaseModel):
    id: str
    movie_id: str
    movie_title: str
    keyword: str
