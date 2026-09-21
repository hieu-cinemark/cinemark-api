from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class RunScraperRequest(BaseModel):
    """All fields optional and mutually exclusive in effect:
    - keyword_id set: trigger just that one keyword.
    - movie_id set (keyword_id absent): trigger every enabled keyword for that movie.
    - neither set: trigger every enabled keyword across every movie (the daily job's case).

    keyword_id/movie_id are D1 ids (see app/services/d1.py), not UUIDs.

    start_date/end_date only make sense for Facebook search (date-posted
    sweep). Threads has no date filter; TikTok is hashtag-only. The API
    ignores these fields for non-Facebook platforms.
    """

    keyword_id: str | None = None
    movie_id: str | None = None
    max_pages: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    # Operator-approved TikTok related-hashtag hop. Ignored unless keyword_id
    # is set. spider-hub caps pages and stops suggesting further tags at depth 2.
    bfs_depth: int | None = None


class RunScraperResponse(BaseModel):
    requested: int
    published: int


class JobStatus(BaseModel):
    running: bool
    keyword: str | None = None
    keyword_id: str | None = None
    started_at: int | None = None
    type: str | None = None
    account: str | None = None
    post_id: str | None = None
    # Free-text TikTok channel handle for type=channel_videos jobs.
    username: str | None = None


class StopScraperResponse(BaseModel):
    stopped: bool


class RunCommentsResponse(BaseModel):
    published: bool


class RunChannelVideosRequest(BaseModel):
    """TikTok channel grid crawl - username without or with leading @."""

    username: str
    max_pages: int | None = None
    keyword_id: str | None = None


class RunChannelVideosResponse(BaseModel):
    published: bool


class TriggerTokenRefreshResponse(BaseModel):
    ok: bool


class TokenStatus(BaseModel):
    valid: bool
    account: str | None = None
    expires_in_seconds: int | None = None


class ImportCookiesRequest(BaseModel):
    # platform_accounts.id (the numeric row id) - resolved server-side to
    # the actual account_key (email or account_id) bootstrap.py's --account
    # expects, see app/api/routes/token_refresh.py.
    account_id: int
    # Raw JSON text as exported from DevTools - either {"c_user": "...",
    # "xs": "...", ...} or a full Playwright-style cookie list. Passed
    # through as-is to spider-hub's import_cookies(), which validates the
    # required cookie names are present.
    cookies: str


class RestoreSessionRequest(BaseModel):
    # Same row id as ImportCookiesRequest - spider-hub reuses that account's
    # Redis storage_state (or the cookie column) and recaptures GraphQL
    # tokens. No new cookies from the operator.
    account_id: int


class KeywordEnabledUpdate(BaseModel):
    enabled: bool


class KeywordOut(BaseModel):
    id: str
    movie_id: str
    movie_title: str
    keyword: str
