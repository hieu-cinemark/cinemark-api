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


class CommentScheduleOut(BaseModel):
    platform: str
    run_time: str
    enabled: bool
    top_n: int
    last_triggered_date: date | None = None
    updated_at: datetime


class CommentScheduleUpdate(BaseModel):
    run_time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    enabled: bool = True
    # Per enabled keyword, how many of its top-engagement posts to check for
    # missing comments each run - see app/services/d1.py's
    # list_posts_needing_comments.
    top_n: int = Field(default=100, ge=1, le=500)


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
    # Whether the "kira" row in ai_providers has both base_url and api_key
    # set - the dashboard toggle cannot call the provider without these.
    configured: bool
    prompts: list[AiPromptOut]
    # Which provider app/bee/report.py's social-topic-report generation
    # calls - "kira" or "bee", independent of `enabled` above (that gate is
    # ingest-time classifiers only; report generation is an
    # operator/schedule-triggered action, same category as Settings import).
    active_report_provider: str
    updated_at: datetime | None = None


class AiSettingsUpdate(BaseModel):
    enabled: bool
    model: str = Field(min_length=1, max_length=120)
    prompts: dict[str, str] = Field(default_factory=dict)
    active_report_provider: str = Field(default="bee", pattern="^(kira|bee)$")


class AiProviderOut(BaseModel):
    key: str
    base_url: str
    # Whether a secret is stored - the raw api_key is never returned.
    api_key_set: bool
    model: str
    updated_at: datetime | None = None


class AiProviderUpdate(BaseModel):
    base_url: str = Field(min_length=1, max_length=500)
    # None/blank keeps whatever secret is already stored, so the dashboard
    # can change base_url/model without resending the key every time.
    api_key: str | None = Field(default=None, max_length=500)
    model: str = Field(min_length=1, max_length=120)


class CronJob(BaseModel):
    name: str
    schedule: str
    source: str
    description: str
    last_run_at: datetime | None = None


# --- Proxy behavior -----------------------------------------------------
# Mirrors spider-hub's social_crawler/services/proxy_settings.py DEFAULTS -
# the field defaults below ARE the fallback values spider-hub uses when a
# key is absent, so keep the two in sync when adding a key. Stored as one
# jsonb object in the proxy_settings singleton row (see
# platform_config_db.get_proxy_settings).


class ProxySettings(BaseModel):
    # Sticky pinning (services/pool.py)
    repin_after_consecutive_failures: int = Field(default=5, ge=1, le=100)
    # Circuit-breaker cooldown: base * 2^failures, capped (services/db.py)
    cooldown_base_minutes: float = Field(default=5.0, gt=0, le=240)
    cooldown_max_minutes: float = Field(default=120.0, gt=0, le=10080)
    # proxy_health_check.py (cron, every 5 min)
    health_check_ping_url: str = Field(
        default="https://www.google.com/generate_204", min_length=8, max_length=500, pattern=r"^https?://"
    )
    health_check_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    health_check_alert_after_failures: int = Field(default=2, ge=1, le=100)
    health_check_streak_ttl_hours: float = Field(default=6.0, gt=0, le=168)
    # Rotating-lease vendor API (services/proxy_provider.py)
    provider_request_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    provider_min_get_new_interval_seconds: float = Field(default=60.0, ge=0, le=3600)
    provider_max_cooldown_wait_seconds: float = Field(default=120.0, ge=0, le=3600)
    # crawl_request_consumer.py - requeue backoff when a platform's whole proxy pool is down
    exhausted_backoff_base_seconds: float = Field(default=30.0, gt=0, le=3600)
    exhausted_backoff_growth_factor: float = Field(default=2.0, ge=1, le=10)
    exhausted_backoff_max_seconds: float = Field(default=300.0, gt=0, le=86400)
    exhausted_max_requeues: int = Field(default=3, ge=0, le=100)
    # TikTok synthetic guest identities (spiders/tiktok/client.py)
    tiktok_synthetic_provider: str = Field(default="proxiestrust_tiktok_us", min_length=1, max_length=64)
    tiktok_hashtag_max_attempts: int = Field(default=8, ge=1, le=50)
    tiktok_comments_max_attempts: int = Field(default=8, ge=1, le=50)


class ProxySettingsOut(BaseModel):
    values: ProxySettings
    defaults: ProxySettings
    updated_at: datetime | None = None


# Vendor plans this project already uses. Listed even before they have a
# proxy_providers row, since spider-hub falls back to its own .env token
# (PROXIESTRUST_API_TOKEN / PROXIESTRUST_TIKTOK_US_API_TOKEN) for them.
KNOWN_PROXY_PROVIDERS: dict[str, dict[str, Any]] = {
    "proxiestrust_default": {
        "api_url": "https://proxiestrust.com/sp07api/get_new",
        "ip_allowlist": False,
        "legacy_env_var": "PROXIESTRUST_API_TOKEN",
    },
    "proxiestrust_tiktok_us": {
        "api_url": "https://proxiestrust.com/sp07api/get_new",
        "ip_allowlist": True,
        "legacy_env_var": "PROXIESTRUST_TIKTOK_US_API_TOKEN",
    },
}


class ProxyProviderOut(BaseModel):
    key: str
    api_url: str
    # Whether a token is stored in the DB - the raw token is never returned.
    token_set: bool
    ip_allowlist: bool
    # False = no DB row yet; spider-hub is using legacy_env_var from its .env.
    in_db: bool
    legacy_env_var: str | None = None
    updated_at: datetime | None = None


class ProxyProviderUpdate(BaseModel):
    api_url: str = Field(min_length=8, max_length=500, pattern=r"^https?://")
    # None/blank keeps whatever token is already stored.
    token: str | None = Field(default=None, max_length=500)
    ip_allowlist: bool = False
