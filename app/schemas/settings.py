from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

FilterKeywordCategory = Literal["movie_relevant", "spam_offtopic"]


class AccountOut(BaseModel):
    id: int
    platform: str
    account_id: str
    password: str
    totp_secret: str
    cookie: str
    token: str
    email: str
    email_password: str
    enabled: bool
    created_at: datetime
    updated_at: datetime
    last_checked_at: datetime | None = None
    last_check_status: str | None = None
    # AI-generated diagnosis (see spider-hub's services/kira.
    # diagnose_account_failure) whenever this account gets hard-disabled -
    # cleared back to null on the next successful login.
    last_check_note: str | None = None
    # Pool / circuit-breaker + sticky proxy pinning - written by spider-hub
    # (services/pool.py, services/db.py), read-only here (see
    # platform_config_db.ACCOUNT_CREATE_COLUMNS, which excludes them).
    # pool_status is "active" | "checkpoint" (see db.record_account_outcome)
    # - distinct from last_check_status above. Optional despite the column
    # having a NOT NULL DEFAULT (see platform_config_db._ensure_pool_columns):
    # a row from before that default existed, or written through a path that
    # doesn't apply it, must not 500 the whole account list.
    pool_status: str | None = "active"
    cooldown_until: datetime | None = None
    consecutive_failures: int | None = 0
    last_used_at: datetime | None = None
    assigned_proxy_id: int | None = None


class AccountCreate(BaseModel):
    platform: str
    account_id: str
    password: str = ""
    totp_secret: str = ""
    cookie: str = ""
    token: str = ""
    email: str = ""
    email_password: str = ""
    enabled: bool = True


class AccountSetProxy(BaseModel):
    proxy_id: int


class AccountUpdate(BaseModel):
    account_id: str | None = None
    password: str | None = None
    totp_secret: str | None = None
    cookie: str | None = None
    token: str | None = None
    email: str | None = None
    email_password: str | None = None
    enabled: bool | None = None


class ProxyOut(BaseModel):
    id: int
    platform: str
    proxy_url: str
    username: str
    password: str
    login_use_proxy: bool
    enabled: bool
    created_at: datetime
    updated_at: datetime
    # Pool / circuit-breaker - written by spider-hub (services/db.py),
    # read-only here. pool_status is "active" | "degraded". Optional for
    # the same not-yet-migrated/NULL-tolerance reason as AccountOut above.
    pool_status: str | None = "active"
    cooldown_until: datetime | None = None
    consecutive_failures: int | None = 0
    last_used_at: datetime | None = None
    # How many accounts are currently sticky-pinned to this proxy - see
    # platform_config_db.list_proxies. Only populated by the list endpoint.
    assigned_account_count: int = 0


class ProxyCreate(BaseModel):
    platform: str = "all"
    proxy_url: str
    username: str = ""
    password: str = ""
    login_use_proxy: bool = False
    enabled: bool = True


class ProxyUpdate(BaseModel):
    platform: str | None = None
    proxy_url: str | None = None
    username: str | None = None
    password: str | None = None
    login_use_proxy: bool | None = None
    enabled: bool | None = None


class FilterKeywordOut(BaseModel):
    id: int
    keyword: str
    category: FilterKeywordCategory
    enabled: bool
    created_at: datetime
    updated_at: datetime


class FilterKeywordCreate(BaseModel):
    keyword: str
    category: FilterKeywordCategory
    enabled: bool = True


class FilterKeywordUpdate(BaseModel):
    keyword: str | None = None
    category: FilterKeywordCategory | None = None
    enabled: bool | None = None


class CrawlScheduleOut(BaseModel):
    platform: str
    run_time: str
    enabled: bool
    last_triggered_date: date | None = None
    nurture_before: bool = False
    nurture_after: bool = False
    updated_at: datetime


class CrawlScheduleUpdate(BaseModel):
    # "HH:MM", 24h, interpreted in Asia/Ho_Chi_Minh by app/services/
    # scheduler.py - see that module for why a fixed timezone is enough
    # (single operator, no per-platform timezone need).
    run_time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    enabled: bool = True
    nurture_before: bool = False
    nurture_after: bool = False


# --- AI-assisted account/proxy import - see app/kira/import_parser.py ---


class ImportParseRequest(BaseModel):
    target: Literal["accounts", "proxies"]
    format_hint: str
    content: str


class ImportParseResponse(BaseModel):
    rows: list[dict[str, Any]]


class ImportCommitRequest(BaseModel):
    target: Literal["accounts", "proxies"]
    platform: str
    rows: list[dict[str, Any]]


class ImportCommitResponse(BaseModel):
    created: int
    failed: int


class NurtureRequest(BaseModel):
    platform: Literal["facebook", "threads", "all"] = "all"
    account_id: int | None = None
    like: bool = True
    comment: bool = True
    visits: int = Field(default=3, ge=0, le=8)


class NurtureResponse(BaseModel):
    ok: bool
    queued: int


class TotpCodeResponse(BaseModel):
    # Current 6-digit code, computed from the account's own stored
    # totp_secret - the same math any authenticator app does. Shown to a
    # human completing a *manual* login themselves (see spider-hub's
    # "unattended_login_refused" guard) - never typed in automatically.
    code: str
    expires_in_seconds: int


class AiPromptOut(BaseModel):
    task: str
    system_prompt: str
    default_system_prompt: str


class AiSettingsOut(BaseModel):
    enabled: bool
    model: str
    # Whether KIRA_API_KEY + KIRA_BASE_URL are present - the dashboard
    # toggle cannot call the provider without these.
    configured: bool
    prompts: list[AiPromptOut]
    updated_at: datetime | None = None


class AiSettingsUpdate(BaseModel):
    enabled: bool
    model: str = Field(min_length=1, max_length=120)
    prompts: dict[str, str] = Field(default_factory=dict)


class CronJob(BaseModel):
    name: str
    schedule: str
    source: str
    description: str
    last_run_at: datetime | None = None
