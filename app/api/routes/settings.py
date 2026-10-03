"""Dashboard Settings page: CRUD over the platform_accounts /
platform_proxies Supabase tables (see app/services/platform_config_db.py) -
the same tables spider-hub reads from for login credentials and proxy
config. Read-only elsewhere; this is the only place that writes them."""

from __future__ import annotations

import asyncio
import re
import time

from fastapi import APIRouter, Query
from pydantic import ValidationError as PydanticValidationError
from pyotp import TOTP

from app.ai.tasks.import_parser import parse_import
from app.clients.kafka import publish_nurture_request, publish_tiktok_identity_reset
from app.core.errors import NotFoundError, UpstreamError, ValidationError
from app.core.logging import get_logger
from app.schemas.settings import (
    AccountCreate,
    AccountOut,
    AccountSetProxy,
    AccountUpdate,
    AiProviderOut,
    AiProviderUpdate,
    AiSettingsOut,
    AiSettingsUpdate,
    AutoLoginRunHistoryEntry,
    AutoLoginSettings,
    AutoLoginSettingsOut,
    AutoLoginSettingsUpdate,
    CleanupRunHistoryEntry,
    CleanupSettings,
    CleanupSettingsOut,
    CleanupSettingsUpdate,
    CommentScheduleOut,
    CommentScheduleUpdate,
    CrawlScheduleOut,
    CrawlScheduleUpdate,
    FilterKeywordCreate,
    FilterKeywordOut,
    FilterKeywordUpdate,
    ImportCommitRequest,
    ImportCommitResponse,
    ImportParseRequest,
    ImportParseResponse,
    NurtureRequest,
    NurtureResponse,
    ProxyCreate,
    ProxyOut,
    ProxyProviderOut,
    ProxyProviderUpdate,
    ProxySettings,
    ProxySettingsOut,
    ProxyUpdate,
    TotpCodeResponse,
)
from app.services import auto_login as auto_login_svc
from app.services import platform_config_db as db
from app.services.account_health import evaluate_account_health
from app.services.platform_token import account_key as _account_key
from app.services.scheduler import purge_in_progress, run_purge_now

logger = get_logger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])

# Strong refs for fire-and-forget tasks - the event loop only keeps weak
# ones, so an unreferenced task can be garbage-collected mid-run.
_background_tasks: set[asyncio.Task] = set()


@router.get("/accounts", response_model=list[AccountOut])
async def list_accounts(platform: str | None = Query(default=None)) -> list[AccountOut]:
    rows = await db.list_accounts(platform)
    return [AccountOut(**row) for row in rows]


@router.post("/accounts", response_model=AccountOut)
async def create_account(payload: AccountCreate) -> AccountOut:
    row = await db.create_account(payload.model_dump())
    return AccountOut(**row)


@router.patch("/accounts/{account_id}", response_model=AccountOut)
async def update_account(account_id: int, payload: AccountUpdate) -> AccountOut:
    row = await db.update_account(account_id, payload.model_dump(exclude_unset=True))
    return AccountOut(**row)


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: int) -> dict[str, bool]:
    await db.delete_account(account_id)
    return {"ok": True}


_NURTURE_PLATFORMS = ("facebook", "threads")


@router.post("/accounts/nurture", response_model=NurtureResponse)
async def nurture_accounts(payload: NurtureRequest) -> NurtureResponse:
    """Queue a Facebook/Threads home-feed warm-up (scroll, a few likes,
    at most one short comment, open a couple of posts then go back).
    Accounts without a saved browser session are skipped downstream -
    this never types a password. TikTok is not supported."""
    targets: list[tuple[str, str | None]] = []
    if payload.account_id is not None:
        account = await db.get_account(payload.account_id)
        if account is None:
            raise NotFoundError(f"Account {payload.account_id} not found")
        platform = account["platform"]
        if platform not in _NURTURE_PLATFORMS:
            raise ValidationError("Session warm-up is only supported for Facebook and Threads accounts")
        needle = _account_key(account)
        targets.append((platform, needle))
    else:
        platforms = _NURTURE_PLATFORMS if payload.platform == "all" else (payload.platform,)
        targets.extend((platform, None) for platform in platforms)

    queued = 0
    for platform, account_key in targets:
        ok = await publish_nurture_request(
            platform,
            account_key,
            like=payload.like,
            comment=payload.comment,
            visits=payload.visits,
        )
        if ok:
            queued += 1
    if queued == 0:
        raise UpstreamError("Could not queue session warm-up")
    return NurtureResponse(ok=True, queued=queued)


@router.post("/accounts/{account_id}/check", response_model=AccountOut)
async def check_account(account_id: int) -> AccountOut:
    """Passive health check - reads signals spider-hub already maintains
    (Redis block counters, session cache) instead of sending a fresh
    request to the platform. See app/services/account_health.py."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    status = await evaluate_account_health(account)
    row = await db.update_account_check_result(account_id, status=status)
    return AccountOut(**row)


@router.post("/accounts/{account_id}/totp-code", response_model=TotpCodeResponse)
async def get_totp_code(account_id: int) -> TotpCodeResponse:
    """Current 6-digit 2FA code for this account, computed from its own
    stored totp_secret - the exact math an authenticator app does. For a
    human completing a *manual* login themselves after spider-hub's
    "unattended_login_refused" guard blocked an automated one - they still
    read the code and type it in, this endpoint just saves them running
    `pyotp.TOTP(secret).now()` in a terminal."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    secret = (account.get("totp_secret") or "").strip()
    if not secret:
        raise ValidationError(f"Account {account_id} has no 2FA secret configured")
    totp = TOTP(secret)
    expires_in = totp.interval - (int(time.time()) % totp.interval)
    return TotpCodeResponse(code=totp.now(), expires_in_seconds=expires_in)


@router.post("/accounts/{account_id}/reset-proxy", response_model=AccountOut)
async def reset_account_proxy(account_id: int) -> AccountOut:
    """Clears an account's sticky proxy pinning (see
    app/services/platform_config_db.reset_account_proxy) - it gets re-pinned
    to whichever proxy has the fewest accounts on its next crawl/bootstrap
    run. Use from the dashboard when retiring a proxy or rebalancing after
    adding new ones."""
    row = await db.reset_account_proxy(account_id)
    return AccountOut(**row)


@router.post("/accounts/{account_id}/set-proxy", response_model=AccountOut)
async def set_account_proxy(account_id: int, payload: AccountSetProxy) -> AccountOut:
    """Manually pins an account to a specific proxy - see
    app/services/platform_config_db.set_account_proxy. Counterpart to
    reset-proxy above (which clears the pin back to auto-assign)."""
    row = await db.set_account_proxy(account_id, payload.proxy_id)
    return AccountOut(**row)


@router.post("/accounts/{account_id}/reset-cookies")
async def reset_tiktok_cookies(account_id: int) -> dict[str, bool]:
    """Triggers spider-hub's headless TikTok identity re-capture
    (device_id/odinId) for this one account row - see
    app/clients/kafka.py's publish_tiktok_identity_reset. TikTok-only:
    Facebook/Threads use the platform-wide token_refresh routes instead
    (see app/api/routes/token_refresh.py), which re-run their own
    password/2FA browser-bootstrap flow rather than targeting one row."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    if account["platform"] != "tiktok":
        raise ValidationError("Cookie reset is only supported for tiktok accounts")
    ok = await publish_tiktok_identity_reset(account_id)
    return {"ok": ok}


@router.get("/proxies", response_model=list[ProxyOut])
async def list_proxies(platform: str | None = Query(default=None)) -> list[ProxyOut]:
    rows = await db.list_proxies(platform)
    return [ProxyOut(**row) for row in rows]


@router.post("/proxies", response_model=ProxyOut)
async def create_proxy(payload: ProxyCreate) -> ProxyOut:
    row = await db.create_proxy(payload.model_dump())
    return ProxyOut(**row)


@router.patch("/proxies/{proxy_id}", response_model=ProxyOut)
async def update_proxy(proxy_id: int, payload: ProxyUpdate) -> ProxyOut:
    row = await db.update_proxy(proxy_id, payload.model_dump(exclude_unset=True))
    return ProxyOut(**row)


@router.delete("/proxies/{proxy_id}")
async def delete_proxy(proxy_id: int) -> dict[str, bool]:
    await db.delete_proxy(proxy_id)
    return {"ok": True}


def _proxy_settings_out(row: dict) -> ProxySettingsOut:
    # Unknown/stale keys are dropped; a stored value that no longer passes
    # validation (e.g. bounds tightened later) falls back to its default
    # rather than 500ing the whole Settings page.
    stored = row.get("settings") if isinstance(row.get("settings"), dict) else {}
    merged = ProxySettings().model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            merged[key] = getattr(ProxySettings.model_validate({key: value}), key)
        except PydanticValidationError:
            logger.warning("proxy_setting_invalid_stored_value", key=key)
    return ProxySettingsOut(values=ProxySettings(**merged), defaults=ProxySettings(), updated_at=row.get("updated_at"))


@router.get("/proxy", response_model=ProxySettingsOut)
async def get_proxy_settings() -> ProxySettingsOut:
    """Proxy behavior tunables spider-hub reads (pinning, cooldown, health
    check, vendor API pacing, requeue backoff, TikTok synthetic attempts)."""
    return _proxy_settings_out(await db.get_proxy_settings())


@router.put("/proxy", response_model=ProxySettingsOut)
async def set_proxy_settings(payload: ProxySettings) -> ProxySettingsOut:
    if payload.cooldown_base_minutes > payload.cooldown_max_minutes:
        raise ValidationError("cooldown_base_minutes must not exceed cooldown_max_minutes")
    if payload.exhausted_backoff_base_seconds > payload.exhausted_backoff_max_seconds:
        raise ValidationError("exhausted_backoff_base_seconds must not exceed exhausted_backoff_max_seconds")
    row = await db.upsert_proxy_settings(payload.model_dump())
    logger.info("proxy_settings_updated", **payload.model_dump())
    return _proxy_settings_out(row)


def _proxy_provider_out(row: dict) -> ProxyProviderOut:
    return ProxyProviderOut(
        key=row["key"],
        api_url=row.get("api_url") or "",
        token_set=bool(row.get("token")),
        ip_allowlist=bool(row["ip_allowlist"]),
        updated_at=row.get("updated_at"),
    )


@router.get("/proxy/providers", response_model=list[ProxyProviderOut])
async def list_proxy_providers() -> list[ProxyProviderOut]:
    """Rotating-proxy vendor plans (proxy_providers rows) - the only place
    spider-hub reads vendor tokens from. Tokens are never returned, only
    whether one is stored."""
    return [_proxy_provider_out(row) for row in await db.list_proxy_providers()]


@router.put("/proxy/providers/{key}", response_model=ProxyProviderOut)
async def set_proxy_provider(key: str, payload: ProxyProviderUpdate) -> ProxyProviderOut:
    """Upsert - a new provider key can be added from here. token omitted or
    blank keeps whatever token is already stored."""
    if not re.fullmatch(r"[a-z0-9_]{1,64}", key):
        raise ValidationError("provider key must be 1-64 chars of a-z, 0-9, _")
    token = payload.token.strip() if payload.token and payload.token.strip() else None
    row = await db.upsert_proxy_provider(
        key, api_url=payload.api_url.strip(), token=token, ip_allowlist=payload.ip_allowlist
    )
    logger.info("proxy_provider_updated", key=key, api_url=payload.api_url.strip(), token_changed=token is not None)
    return _proxy_provider_out(row)


@router.get("/filter-keywords", response_model=list[FilterKeywordOut])
async def list_filter_keywords(category: str | None = Query(default=None)) -> list[FilterKeywordOut]:
    rows = await db.list_filter_keywords(category)
    return [FilterKeywordOut(**row) for row in rows]


@router.post("/filter-keywords", response_model=FilterKeywordOut)
async def create_filter_keyword(payload: FilterKeywordCreate) -> FilterKeywordOut:
    row = await db.create_filter_keyword(payload.model_dump())
    return FilterKeywordOut(**row)


@router.patch("/filter-keywords/{keyword_id}", response_model=FilterKeywordOut)
async def update_filter_keyword(keyword_id: int, payload: FilterKeywordUpdate) -> FilterKeywordOut:
    row = await db.update_filter_keyword(keyword_id, payload.model_dump(exclude_unset=True))
    return FilterKeywordOut(**row)


@router.delete("/filter-keywords/{keyword_id}")
async def delete_filter_keyword(keyword_id: int) -> dict[str, bool]:
    await db.delete_filter_keyword(keyword_id)
    return {"ok": True}


@router.get("/crawl-schedule", response_model=list[CrawlScheduleOut])
async def list_crawl_schedule() -> list[CrawlScheduleOut]:
    rows = await db.list_crawl_schedules()
    return [CrawlScheduleOut(**row) for row in rows]


@router.put("/crawl-schedule/{platform}", response_model=CrawlScheduleOut)
async def set_crawl_schedule(platform: str, payload: CrawlScheduleUpdate) -> CrawlScheduleOut:
    """Upsert - a platform has no row at all until its schedule is first
    saved (see app/services/scheduler.py, which just skips platforms with
    no row rather than treating that as "every midnight" or some other
    implicit default)."""
    row = await db.upsert_crawl_schedule(
        platform,
        run_time=payload.run_time,
        enabled=payload.enabled,
        nurture_before=payload.nurture_before,
        nurture_after=payload.nurture_after,
    )
    return CrawlScheduleOut(**row)


@router.get("/comment-schedule", response_model=list[CommentScheduleOut])
async def list_comment_schedule() -> list[CommentScheduleOut]:
    rows = await db.list_comment_crawl_schedules()
    return [CommentScheduleOut(**row) for row in rows]


@router.put("/comment-schedule/{platform}", response_model=CommentScheduleOut)
async def set_comment_schedule(platform: str, payload: CommentScheduleUpdate) -> CommentScheduleOut:
    """Upsert - a platform has no row here until its comments sweep is
    first saved, same as /crawl-schedule/{platform} above."""
    row = await db.upsert_comment_crawl_schedule(platform, run_time=payload.run_time, enabled=payload.enabled, top_n=payload.top_n)
    return CommentScheduleOut(**row)


async def _ai_settings_out(row: dict) -> AiSettingsOut:
    from app.ai.defaults import AI_PROMPT_TASKS, DEFAULT_KIRA_MODEL, default_system_prompts

    provider = await db.get_ai_provider("kira")
    defaults = default_system_prompts()
    stored = row.get("prompts") if isinstance(row.get("prompts"), dict) else {}
    prompts = []
    for task in AI_PROMPT_TASKS:
        default = defaults.get(task, "")
        current = str(stored.get(task) or "").strip() or default
        prompts.append(
            {
                "task": task,
                "system_prompt": current,
                "default_system_prompt": default,
            }
        )
    return AiSettingsOut(
        enabled=bool(row.get("enabled")),
        model=str((provider or {}).get("model") or DEFAULT_KIRA_MODEL),
        configured=bool(provider and provider.get("base_url") and provider.get("api_key")),
        prompts=prompts,
        active_report_provider=str(row.get("active_report_provider") or "kira"),
        updated_at=row.get("updated_at"),
    )


@router.get("/ai", response_model=AiSettingsOut)
async def get_ai_settings() -> AiSettingsOut:
    row = await db.get_ai_settings()
    return await _ai_settings_out(row)


@router.put("/ai", response_model=AiSettingsOut)
async def set_ai_settings(payload: AiSettingsUpdate) -> AiSettingsOut:
    from app.ai.client import invalidate_provider_cache
    from app.ai.defaults import AI_PROMPT_TASKS
    from app.ai.kira import invalidate_ai_runtime_cache

    allowed = set(AI_PROMPT_TASKS)
    prompts = {key: value for key, value in payload.prompts.items() if key in allowed and isinstance(value, str)}
    row = await db.upsert_ai_settings(
        enabled=payload.enabled, prompts=prompts, active_report_provider=payload.active_report_provider
    )

    existing_provider = await db.get_ai_provider("kira")
    await db.upsert_ai_provider(
        "kira",
        base_url=(existing_provider or {}).get("base_url") or "",
        api_key=None,
        model=payload.model.strip(),
    )
    invalidate_provider_cache("kira")
    invalidate_ai_runtime_cache()
    logger.info("ai_settings_updated", enabled=payload.enabled, model=payload.model.strip(), prompt_tasks=sorted(prompts))
    return await _ai_settings_out(row)


def _ai_provider_out(row: dict) -> AiProviderOut:
    return AiProviderOut(
        key=row["key"],
        base_url=row.get("base_url") or "",
        api_key_set=bool(row.get("api_key")),
        model=row.get("model") or "",
        updated_at=row.get("updated_at"),
    )


@router.get("/ai/providers", response_model=list[AiProviderOut])
async def list_ai_providers() -> list[AiProviderOut]:
    """Every configured LLM provider ({key, base_url, model} - api_key is
    never returned, only whether one is set). See app/ai/client.py."""
    rows = await db.list_ai_providers()
    return [_ai_provider_out(row) for row in rows]


@router.put("/ai/providers/{key}", response_model=AiProviderOut)
async def set_ai_provider(key: str, payload: AiProviderUpdate) -> AiProviderOut:
    """Upsert - `key` doesn't have to already exist, so a new provider can
    be added from here with no code/schema change. api_key omitted or
    blank keeps whatever secret is already stored."""
    from app.ai.client import invalidate_provider_cache

    api_key = payload.api_key.strip() if payload.api_key and payload.api_key.strip() else None
    row = await db.upsert_ai_provider(key, base_url=payload.base_url.strip(), api_key=api_key, model=payload.model.strip())
    invalidate_provider_cache(key)
    logger.info("ai_provider_updated", key=key, base_url=payload.base_url.strip(), model=payload.model.strip())
    return _ai_provider_out(row)


@router.post("/import/parse", response_model=ImportParseResponse)
async def import_parse(payload: ImportParseRequest) -> ImportParseResponse:
    """Step 1 of the AI-assisted bulk import (see app/ai/tasks/import_parser.py)
    - turns freeform pasted account/proxy data into structured candidate
    rows for the dashboard to show as an editable preview. Never writes to
    the DB itself; see /import/commit below for that."""
    try:
        rows = await parse_import(payload.target, payload.format_hint, payload.content)
    except Exception as exc:
        raise ValidationError(
            f"Could not parse the pasted content ({exc}) - try describing the format more precisely, "
            "or pasting a smaller batch."
        ) from exc
    if not rows:
        raise ValidationError("No rows could be extracted from the pasted content - check the format description.")
    return ImportParseResponse(rows=rows)


@router.post("/import/commit", response_model=ImportCommitResponse)
async def import_commit(payload: ImportCommitRequest) -> ImportCommitResponse:
    """Step 2 - the operator has reviewed/edited the /import/parse preview
    and is confirming it. Writes each row with create_account/create_proxy
    (same whitelisted-column path the regular add-account/proxy form uses,
    see platform_config_db.py) - a row that fails (e.g. a duplicate
    account_id) is skipped, not fatal to the rest of the batch."""
    created = 0
    failed = 0
    for row in payload.rows:
        # Preserve an explicit "enabled" the operator set while editing the
        # /import/parse preview (e.g. unchecking a suspect row) - only
        # default to True when the row doesn't carry one at all.
        fields = {**row, "platform": payload.platform, "enabled": row.get("enabled", True)}
        try:
            if payload.target == "accounts":
                await db.create_account(fields)
            else:
                await db.create_proxy(fields)
            created += 1
        except Exception as exc:
            logger.warning("import_commit_row_failed", target=payload.target, error=str(exc))
            failed += 1
    return ImportCommitResponse(created=created, failed=failed)


# --- Irrelevant-post cleanup (app/services/cleanup.py) ---
# Same shape as /settings/proxy: stored values merged over schema defaults
# so a row from before a new field was added (or with a missing key) just
# gets the default for that key instead of 500ing the page.


def _cleanup_settings_out(row: dict, *, last_run: dict | None) -> CleanupSettingsOut:
    stored = row.get("settings") if isinstance(row.get("settings"), dict) else {}
    defaults = CleanupSettings()
    merged: dict = defaults.model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            merged[key] = getattr(CleanupSettings.model_validate({key: value}), key)
        except PydanticValidationError:
            logger.warning("cleanup_setting_invalid_stored_value", key=key)
    return CleanupSettingsOut(
        values=CleanupSettings(**merged),
        defaults=defaults,
        last_run_at=(last_run or {}).get("started_at") if last_run else None,
        last_run_summary=last_run,
        running=purge_in_progress(),
        updated_at=row.get("updated_at"),
    )


@router.get("/cleanup", response_model=CleanupSettingsOut)
async def get_cleanup_route() -> CleanupSettingsOut:
    """Effective knobs (dashboard-stored values over env defaults), the most
    recent run's row from cleanup_run_history, and whether a run is
    currently in flight (the dashboard uses that to disable its Run-now
    button and show "Running…")."""
    row = await db.get_cleanup_settings()
    history = await db.list_cleanup_run_history(limit=1)
    return _cleanup_settings_out(row, last_run=history[0] if history else None)


@router.put("/cleanup", response_model=CleanupSettingsOut)
async def set_cleanup_route(payload: CleanupSettingsUpdate) -> CleanupSettingsOut:
    """Partial update - keys absent from the payload keep their previously
    stored value. Same as the proxy-settings PUT: no need to re-send every
    field just to flip the toggle."""
    allowed = {"run_time", "enabled", "grace_hours"}
    set_fields = set(payload.model_fields_set) & allowed
    values_to_write = {key: getattr(payload, key) for key in set_fields}
    row = await db.upsert_cleanup_settings(values_to_write)
    logger.info("cleanup_settings_updated", **values_to_write)
    history = await db.list_cleanup_run_history(limit=1)
    return _cleanup_settings_out(row, last_run=history[0] if history else None)


@router.post("/cleanup/run", response_model=dict)
async def run_cleanup_route() -> dict:
    """Manual trigger from the dashboard. Returns immediately with
    {"started": True/False}; the actual purge runs in the background task
    the scheduler also uses, so the operator can navigate away without
    cancelling it. `started: false` means a previous run is still in
    flight - the dashboard should show that, not retry."""
    started = await run_purge_now()
    return {"started": started}


@router.get("/cleanup/history", response_model=list[CleanupRunHistoryEntry])
async def cleanup_history_route(limit: int = 20) -> list[CleanupRunHistoryEntry]:
    """Most-recent-first list of cleanup_run_history rows, capped by `limit`
    (default 20). Each entry's counts are the actual DELETE RETURNING ids
    from cleanup.py - the same numbers logged as
    `irrelevant_purge_done telegram=True`."""
    rows = await db.list_cleanup_run_history(limit=limit)
    return [CleanupRunHistoryEntry(**row) for row in rows]


# --- Auto-login scheduler (app/services/auto_login.py) ---
# Same shape as /cleanup: GET shows effective settings over defaults +
# running flag + last_run_at; PUT is a partial update (the dashboard's
# toggle UI just sends `{enabled: true}`); POST /run kicks a one-off
# tick and returns whether a run actually started (False if another
# tick is mid-flight). /history exposes the recent runs.


@router.get("/auto-login", response_model=AutoLoginSettingsOut)
async def get_auto_login_route() -> AutoLoginSettingsOut:
    """Effective knobs (dashboard-stored values over env defaults) plus
    the running flag (so the dashboard can grey out "Run now" while a
    tick is in flight) and the most-recent run's started_at."""
    return await auto_login_svc.get_auto_login_settings_out()


@router.put("/auto-login", response_model=AutoLoginSettingsOut)
async def set_auto_login_route(payload: AutoLoginSettingsUpdate) -> AutoLoginSettingsOut:
    """Partial update - keys absent from the payload keep their previously
    stored value. Same as the cleanup/proxy PUTs: no need to re-send
    every field just to flip the toggle.

    Validates via the schema BEFORE writing - a bad interval_seconds
    (e.g. <60) or an unknown platform name should bounce here, not
    blow up the scheduler loop later.
    """
    # Build the post-merge view first so a single bad key 400s with a
    # useful message rather than writing half a payload. Same approach
    # as /cleanup - schema_update_settings_update's PutRecord pattern.
    set_fields = set(payload.model_fields_set) & set(AutoLoginSettings().model_dump().keys())
    if not set_fields:
        # Nothing to update - just return current state.
        return await auto_login_svc.get_auto_login_settings_out()
    values_to_write = {key: getattr(payload, key) for key in set_fields}
    # Validate the would-be merged values against the schema to catch
    # constraint failures before they reach Supabase. Resolve the
    # current settings, overlay the partial, and Pydantic-validate
    # the result.
    merged = (await auto_login_svc.resolve_auto_login_settings()).model_dump()
    merged.update(values_to_write)
    AutoLoginSettings(**merged)  # raises ValidationError if invalid
    await db.upsert_auto_login_settings(values_to_write)
    logger.info("auto_login_settings_updated", **values_to_write)
    return await auto_login_svc.get_auto_login_settings_out()


@router.post("/auto-login/run", response_model=dict)
async def run_auto_login_route() -> dict:
    """Manual trigger from the dashboard's "Run now" button. Returns
    `{"started": True/False}` immediately; the actual tick runs as a
    background asyncio task so the operator can navigate away without
    cancelling it. `started: false` means a previous tick is still
    in flight (the dashboard should show "running..." rather than
    retry)."""
    started = await _kick_auto_login_tick(triggered_by="manual")
    return {"started": started}


async def _kick_auto_login_tick(*, triggered_by: str) -> bool:
    """Schedules run_auto_login_tick as a fire-and-forget asyncio task,
    returns True if a task was actually scheduled. Returns False if
    another tick is already in flight (the in-flight guard inside
    run_auto_login_tick is the real mutex; this helper just makes
    the "schedule vs skip" decision eagerly so the HTTP response
    is meaningful to the dashboard).

    Mirrors the same fire-and-forget shape scheduler.py applies to its
    crawl/comment/auto_login tasks - a slow Kafka publish for one
    account shouldn't block the API request that asked for the run
    in the first place. Errors inside the tick are caught and
    persisted to auto_login_run_history.error; nothing bubbles up
    to the HTTP layer.
    """
    if auto_login_svc.is_auto_login_in_flight():
        return False
    # force=True: an explicit "Run now" runs even while the schedule is
    # switched off - otherwise the tick returns -1 without doing anything.
    task = asyncio.create_task(auto_login_svc.run_auto_login_tick(triggered_by=triggered_by, force=True))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return True


@router.get("/auto-login/history", response_model=list[AutoLoginRunHistoryEntry])
async def auto_login_history_route(limit: int = 20) -> list[AutoLoginRunHistoryEntry]:
    """Most-recent-first list of auto_login_run_history rows, capped
    by `limit` (default 20). Each row's `kafka_published` /
    `kafka_publish_failed` columns tell the operator how many of the
    attempted accounts actually made it to spider-hub (vs. were
    dropped by a Kafka outage mid-tick)."""
    return await auto_login_svc.list_auto_login_history(limit=limit)
