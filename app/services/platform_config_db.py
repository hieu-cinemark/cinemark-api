"""Async Postgres client (Supabase) for the platform_accounts /
platform_proxies tables - same tables spider-hub's own
social_crawler/services/db.py reads from (see that module there for the
full rationale: these change too often for env vars + process restarts to
be worth it). This side has read AND write access, backing the dashboard's
Settings page; spider-hub only ever reads.

Every write here builds its SQL from a fixed, whitelisted column list plus
values coming from a Pydantic model (never a raw request dict) - the model
strips unknown fields by default, so a caller can never smuggle an
arbitrary column name into the query string."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.core.config import settings
from app.core.errors import ConflictError, NotFoundError, UpstreamError
from app.core.logging import get_logger

logger = get_logger(__name__)

ACCOUNT_COLUMNS = (
    "id, platform, account_id, password, totp_secret, cookie, token, email, email_password, "
    "enabled, created_at, updated_at, last_checked_at, last_check_status, last_check_note, "
    # Pool / circuit-breaker + sticky proxy pinning columns - written by
    # spider-hub's services/pool.py & services/db.py, read-only from this
    # side (system-written, never part of the account form) - see
    # ACCOUNT_CREATE_COLUMNS below, which deliberately excludes them. The
    # raw column is just "status" - aliased to pool_status so it can't be
    # confused with last_check_status above (a different, manually-
    # triggered signal - see app/services/account_health.py).
    "status AS pool_status, cooldown_until, consecutive_failures, last_used_at, assigned_proxy_id"
)
PROXY_COLUMNS = (
    "id, platform, proxy_url, username, password, login_use_proxy, enabled, created_at, updated_at, "
    "status AS pool_status, cooldown_until, consecutive_failures, last_used_at"
)

_pool_columns_ready = False


async def _ensure_pool_columns() -> None:
    """Adds the pool/circuit-breaker + sticky-proxy-pinning columns
    (status, cooldown_until, consecutive_failures, last_used_at,
    assigned_proxy_id) if this Supabase database predates spider-hub's
    services/pool.py - mirrors the exact ALTER TABLE ... IF NOT EXISTS
    statements in spider-hub's scripts/dev_db_schema.sql, so ACCOUNT_COLUMNS/
    PROXY_COLUMNS/_PROXY_LIST_COLUMNS (which SELECT these columns
    unconditionally) don't 500 the whole Settings page with
    UndefinedColumn on a not-yet-migrated database. IF NOT EXISTS so an
    already-migrated database is a no-op."""
    global _pool_columns_ready
    if _pool_columns_ready:
        return
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'")
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS cooldown_until timestamptz")
        await cur.execute(
            "ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0"
        )
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS assigned_proxy_id int")
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'")
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS cooldown_until timestamptz")
        await cur.execute(
            "ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0"
        )
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
        await conn.commit()
    _pool_columns_ready = True


_ACCOUNT_CREATE_COLUMNS = (
    "platform",
    "account_id",
    "password",
    "totp_secret",
    "cookie",
    "token",
    "email",
    "email_password",
    "enabled",
)
_PROXY_CREATE_COLUMNS = ("platform", "proxy_url", "username", "password", "login_use_proxy", "enabled")


async def _connect() -> psycopg.AsyncConnection[Any]:
    if not settings.database_url:
        raise UpstreamError("DATABASE_URL is not configured on cinemark-api")
    try:
        return await psycopg.AsyncConnection.connect(settings.database_url, row_factory=dict_row, connect_timeout=5)
    except psycopg.Error as exc:
        logger.error("db_connect_failed", error=str(exc))
        raise UpstreamError("Could not connect to the settings database") from exc


# --- accounts ---------------------------------------------------------


async def list_accounts(platform: str | None = None) -> list[dict[str, Any]]:
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        if platform:
            await cur.execute(
                f"SELECT {ACCOUNT_COLUMNS} FROM platform_accounts WHERE platform = %s ORDER BY id", (platform,)
            )
        else:
            await cur.execute(f"SELECT {ACCOUNT_COLUMNS} FROM platform_accounts ORDER BY platform, id")
        return await cur.fetchall()


async def get_account(account_id: int) -> dict[str, Any] | None:
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {ACCOUNT_COLUMNS} FROM platform_accounts WHERE id = %s", (account_id,))
        return await cur.fetchone()


async def create_account(fields: dict[str, Any]) -> dict[str, Any]:
    await _ensure_pool_columns()
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with await _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO platform_accounts ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                f"RETURNING {ACCOUNT_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(f"An account for {fields.get('platform')}/{fields.get('account_id')} already exists") from exc


async def update_account(account_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [account_id]
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET {set_clause}, updated_at = now() WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def update_account_check_result(account_id: int, *, status: str) -> dict[str, Any]:
    """Records the outcome of a health check (see app/services/account_health.py)
    - deliberately separate from update_account above: these columns are
    system-written from an automated check, never user-editable through the
    account form, so they're not part of _ACCOUNT_CREATE_COLUMNS at all."""
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET last_checked_at = now(), last_check_status = %s "
            f"WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (status, account_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def reset_account_proxy(account_id: int) -> dict[str, Any]:
    """Clears one account's sticky proxy pinning (assigned_proxy_id -> NULL)
    - see spider-hub's services/pool.acquire_proxy_for_account. The account
    gets re-pinned to whichever proxy currently has the fewest accounts on
    its next crawl/bootstrap run; use this from the dashboard when a
    pinned proxy is being retired, or to manually rebalance after adding
    new proxies to the pool."""
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET assigned_proxy_id = NULL, updated_at = now() "
            f"WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (account_id,),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def set_account_proxy(account_id: int, proxy_id: int) -> dict[str, Any]:
    """Manually pins one account to a specific proxy - the dashboard
    counterpart to spider-hub's own auto-pin-to-least-loaded logic (see
    services/pool.acquire_proxy_for_account), for an operator who wants
    direct control over which account sits on which IP instead of letting
    the pool balance it automatically. FK constraint on assigned_proxy_id
    (see spider-hub's scripts/dev_db_schema.sql) rejects a proxy_id that
    doesn't exist - surfaces here as a plain psycopg error, not specially
    handled, since the dashboard's proxy picker only ever offers real ids."""
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET assigned_proxy_id = %s, updated_at = now() "
            f"WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (proxy_id, account_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def delete_account(account_id: int) -> None:
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_accounts WHERE id = %s", (account_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Account {account_id} not found")


_PROXY_LIST_COLUMNS = (
    "pp.id, pp.platform, pp.proxy_url, pp.username, pp.password, pp.login_use_proxy, pp.enabled, "
    "pp.created_at, pp.updated_at, pp.status AS pool_status, pp.cooldown_until, pp.consecutive_failures, "
    "pp.last_used_at, "
    # How many live platform_accounts rows are sticky-pinned to this
    # proxy (enabled, not checkpointed) - matches spider-hub's
    # get_least_loaded_proxy so the dashboard "accounts pinned" number is
    # the same load the pool actually balances on. Dead pins still sit on
    # assigned_proxy_id but must not make an IP look full.
    "(SELECT count(*) FROM platform_accounts pa WHERE pa.assigned_proxy_id = pp.id "
    "AND pa.enabled = true AND pa.status != 'checkpoint') AS assigned_account_count"
)


async def list_proxies(platform: str | None = None) -> list[dict[str, Any]]:
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        if platform:
            await cur.execute(
                f"SELECT {_PROXY_LIST_COLUMNS} FROM platform_proxies pp WHERE pp.platform = %s ORDER BY pp.id",
                (platform,),
            )
        else:
            await cur.execute(f"SELECT {_PROXY_LIST_COLUMNS} FROM platform_proxies pp ORDER BY pp.platform, pp.id")
        return await cur.fetchall()


async def create_proxy(fields: dict[str, Any]) -> dict[str, Any]:
    await _ensure_pool_columns()
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with await _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO platform_proxies ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                f"RETURNING {PROXY_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(f"A proxy for {fields.get('platform')}/{fields.get('proxy_url')} already exists") from exc


async def update_proxy(proxy_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [proxy_id]
    await _ensure_pool_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_proxies SET {set_clause}, updated_at = now() WHERE id = %s RETURNING {PROXY_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Proxy {proxy_id} not found")
    return row


async def delete_proxy(proxy_id: int) -> None:
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_proxies WHERE id = %s", (proxy_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Proxy {proxy_id} not found")


# --- filter keywords ---------------------------------------------------
# filter_keywords: generic (not platform-scoped) content filter - movie-
# relevant vs spam/off-topic keywords, CRUD'd here and read by spider-hub
# to decide whether a scraped post/comment is worth keeping. Same "no ORM,
# fixed whitelisted column list" shape as accounts/proxies above.

FILTER_KEYWORD_COLUMNS = "id, keyword, category, enabled, created_at, updated_at"
_FILTER_KEYWORD_CREATE_COLUMNS = ("keyword", "category", "enabled")


async def list_filter_keywords(category: str | None = None) -> list[dict[str, Any]]:
    async with await _connect() as conn, conn.cursor() as cur:
        if category:
            await cur.execute(
                f"SELECT {FILTER_KEYWORD_COLUMNS} FROM filter_keywords WHERE category = %s ORDER BY id",
                (category,),
            )
        else:
            await cur.execute(f"SELECT {FILTER_KEYWORD_COLUMNS} FROM filter_keywords ORDER BY category, id")
        return await cur.fetchall()


async def create_filter_keyword(fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _FILTER_KEYWORD_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with await _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO filter_keywords ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                f"RETURNING {FILTER_KEYWORD_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(f"A {fields.get('category')} keyword {fields.get('keyword')!r} already exists") from exc


async def update_filter_keyword(keyword_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _FILTER_KEYWORD_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [keyword_id]
    try:
        async with await _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"UPDATE filter_keywords SET {set_clause}, updated_at = now() WHERE id = %s "
                f"RETURNING {FILTER_KEYWORD_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError("A keyword with that category/text already exists") from exc
    if row is None:
        raise NotFoundError(f"Filter keyword {keyword_id} not found")
    return row


async def delete_filter_keyword(keyword_id: int) -> None:
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM filter_keywords WHERE id = %s", (keyword_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Filter keyword {keyword_id} not found")


# --- crawl schedule ----------------------------------------------------
# crawl_schedules: per-platform daily crawl time, CRUD'd from the
# dashboard's "Crawl schedule" card, read by app/services/scheduler.py's
# in-process loop - see that table's own comment in spider-hub's
# scripts/dev_db_schema.sql for the full rationale (replaces both an OS
# crontab entry and cinemark-scraper's Cloudflare Cron Triggers).

CRAWL_SCHEDULE_COLUMNS = "platform, run_time, enabled, last_triggered_date, nurture_before, nurture_after, updated_at"

_nurture_columns_ready = False


async def _ensure_crawl_schedule_nurture_columns() -> None:
    """Adds nurture_before/after if this database was created before those
    columns existed (dev_db_schema.sql now includes them). IF NOT EXISTS
    so a fresh database is a no-op."""
    global _nurture_columns_ready
    if _nurture_columns_ready:
        return
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "ALTER TABLE crawl_schedules ADD COLUMN IF NOT EXISTS nurture_before boolean NOT NULL DEFAULT false"
        )
        await cur.execute(
            "ALTER TABLE crawl_schedules ADD COLUMN IF NOT EXISTS nurture_after boolean NOT NULL DEFAULT false"
        )
        await conn.commit()
    _nurture_columns_ready = True


async def list_crawl_schedules() -> list[dict[str, Any]]:
    await _ensure_crawl_schedule_nurture_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {CRAWL_SCHEDULE_COLUMNS} FROM crawl_schedules ORDER BY platform")
        return await cur.fetchall()


async def ensure_default_crawl_schedules(platforms: set[str]) -> None:
    """Called once at startup (see app/main.py) for every registered
    platform. Before this table existed, Facebook/TikTok/Threads crawling
    was guaranteed by a fixed OS crontab / Cloudflare Cron Trigger
    regardless of any dashboard setting; now a platform with no row here
    simply never appears in scheduler.py's list_crawl_schedules() loop and
    never runs again, with nothing surfacing the gap (see
    upsert_crawl_schedule's own docstring: "every platform starts with no
    row at all until its schedule is first saved"). Seeds the same
    run_time='07:00'/enabled=true the table's own column defaults already
    encode (scripts/dev_db_schema.sql), ON CONFLICT DO NOTHING so a platform
    an operator has already configured (or deliberately disabled) from the
    dashboard is never touched."""
    await _ensure_crawl_schedule_nurture_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        for platform in platforms:
            await cur.execute(
                "INSERT INTO crawl_schedules (platform) VALUES (%s) ON CONFLICT (platform) DO NOTHING",
                (platform,),
            )
        await conn.commit()


async def upsert_crawl_schedule(
    platform: str, *, run_time: str, enabled: bool, nurture_before: bool = False, nurture_after: bool = False
) -> dict[str, Any]:
    """Dashboard-facing write - run_time/enabled/nurture_* are user-set.
    ON CONFLICT so the dashboard doesn't need to know whether a platform's
    row already exists (every platform starts with no row at all until its
    schedule is first saved)."""
    await _ensure_crawl_schedule_nurture_columns()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"INSERT INTO crawl_schedules (platform, run_time, enabled, nurture_before, nurture_after) "
            f"VALUES (%s, %s, %s, %s, %s) "
            f"ON CONFLICT (platform) DO UPDATE SET run_time = EXCLUDED.run_time, enabled = EXCLUDED.enabled, "
            f"nurture_before = EXCLUDED.nurture_before, nurture_after = EXCLUDED.nurture_after, "
            f"updated_at = now() RETURNING {CRAWL_SCHEDULE_COLUMNS}",
            (platform, run_time, enabled, nurture_before, nurture_after),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def mark_crawl_schedule_triggered(platform: str, triggered_date: str) -> None:
    """scheduler.py's own re-entrancy guard write - see crawl_schedules.
    last_triggered_date's column comment. Not user-facing, no route calls
    this directly."""
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE crawl_schedules SET last_triggered_date = %s WHERE platform = %s",
            (triggered_date, platform),
        )
        await conn.commit()


# --- comment crawl schedule ---------------------------------------------
# comment_crawl_schedules: per-platform daily "top comments sweep" time -
# separate from crawl_schedules (posts) above since a platform's comments
# sweep runs on its own cadence, independent of when that platform's post
# crawl runs. At its run_time, for each of that platform's enabled
# keywords, queues a comments crawl (app/services/kafka.py's
# publish_comments_crawl_request) for the keyword's top `top_n`-by-
# engagement posts that still have zero comments stored
# (app/services/d1.py's list_posts_needing_comments) - see
# app/services/scheduler.py's _comments_tick, the exact same poll/fire/
# last_triggered_date-guard shape as _tick uses for crawl_schedules.

COMMENT_SCHEDULE_COLUMNS = "platform, run_time, enabled, top_n, last_triggered_date, updated_at"

_comment_schedule_ready = False


async def _ensure_comment_crawl_schedules_table() -> None:
    global _comment_schedule_ready
    if _comment_schedule_ready:
        return
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS comment_crawl_schedules (
                platform text PRIMARY KEY,
                run_time text NOT NULL DEFAULT '08:00',
                enabled boolean NOT NULL DEFAULT false,
                top_n integer NOT NULL DEFAULT 100,
                last_triggered_date date,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.commit()
    _comment_schedule_ready = True


async def list_comment_crawl_schedules() -> list[dict[str, Any]]:
    await _ensure_comment_crawl_schedules_table()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {COMMENT_SCHEDULE_COLUMNS} FROM comment_crawl_schedules ORDER BY platform")
        return await cur.fetchall()


async def ensure_default_comment_crawl_schedules(platforms: set[str]) -> None:
    """Same rationale as ensure_default_crawl_schedules above - a platform
    with no row here just never appears in _comments_tick's loop, with
    nothing surfacing the gap. ON CONFLICT DO NOTHING so a platform an
    operator has already configured (or deliberately disabled) is never
    touched."""
    await _ensure_comment_crawl_schedules_table()
    async with await _connect() as conn, conn.cursor() as cur:
        for platform in platforms:
            await cur.execute(
                "INSERT INTO comment_crawl_schedules (platform) VALUES (%s) ON CONFLICT (platform) DO NOTHING",
                (platform,),
            )
        await conn.commit()


async def upsert_comment_crawl_schedule(platform: str, *, run_time: str, enabled: bool, top_n: int) -> dict[str, Any]:
    """Dashboard-facing write - ON CONFLICT so the dashboard doesn't need to
    know whether this platform's row already exists yet."""
    await _ensure_comment_crawl_schedules_table()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO comment_crawl_schedules (platform, run_time, enabled, top_n)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (platform) DO UPDATE SET
                run_time = EXCLUDED.run_time, enabled = EXCLUDED.enabled, top_n = EXCLUDED.top_n, updated_at = now()
            RETURNING {COMMENT_SCHEDULE_COLUMNS}
            """,
            (platform, run_time, enabled, top_n),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def mark_comment_crawl_schedule_triggered(platform: str, triggered_date: str) -> None:
    """scheduler.py's own re-entrancy guard write - see
    comment_crawl_schedules.last_triggered_date's column comment above."""
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE comment_crawl_schedules SET last_triggered_date = %s WHERE platform = %s",
            (triggered_date, platform),
        )
        await conn.commit()


# --- AI settings -------------------------------------------------------
# Singleton row (id=1): model + per-task system prompts the dashboard
# Settings AI tab edits. Read on every Kira call (short-cached in
# app.kira.client) so a save applies without restarting ingest/spider-hub.

AI_SETTINGS_COLUMNS = "id, enabled, model, prompts, updated_at"

_ai_settings_ready = False


async def _ensure_ai_settings_table() -> None:
    global _ai_settings_ready
    if _ai_settings_ready:
        return
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                enabled boolean NOT NULL DEFAULT false,
                model text NOT NULL DEFAULT 'qwen3.8-flash',
                prompts jsonb NOT NULL DEFAULT '{}'::jsonb,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await cur.execute(
            """
            INSERT INTO ai_settings (id, enabled, model, prompts)
            VALUES (1, %s, %s, '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
            """,
            (bool(settings.kira_enabled), "qwen3.8-flash"),
        )
        await conn.commit()
    _ai_settings_ready = True


async def get_ai_settings() -> dict[str, Any]:
    await _ensure_ai_settings_table()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {AI_SETTINGS_COLUMNS} FROM ai_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {"id": 1, "enabled": False, "model": "qwen3.8-flash", "prompts": {}, "updated_at": None}


async def upsert_ai_settings(*, enabled: bool, prompts: dict[str, str]) -> dict[str, Any]:
    """Model is no longer written here - see ai_providers below, which owns
    base_url/api_key/model per provider. The ai_settings.model column is
    left alone (untouched on conflict) rather than dropped, so this isn't a
    destructive schema change; nothing reads it anymore."""
    from psycopg.types.json import Json

    await _ensure_ai_settings_table()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO ai_settings (id, enabled, prompts)
            VALUES (1, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                prompts = EXCLUDED.prompts,
                updated_at = now()
            RETURNING {AI_SETTINGS_COLUMNS}
            """,
            (enabled, Json(prompts)),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


# --- AI provider credentials --------------------------------------------
# One row per LLM provider (key = "kira", "bee", ... - add a new provider
# by inserting a new row, no schema change needed): base_url/api_key/model
# used to build that provider's OpenAI-compatible client (see
# app/ai_client.py). Used to be KIRA_API_KEY/KIRA_BASE_URL/
# BEEKNOEE_API_KEY/BEEKNOEE_BASE_URL env vars - moved here (see
# scripts/migrate_ai_provider_credentials.py) so a key can be rotated or a
# new provider added from the dashboard without a redeploy.

AI_PROVIDER_COLUMNS = "key, base_url, api_key, model, updated_at"

_ai_providers_ready = False


async def _ensure_ai_providers_table() -> None:
    global _ai_providers_ready
    if _ai_providers_ready:
        return
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_providers (
                key text PRIMARY KEY,
                base_url text NOT NULL DEFAULT '',
                api_key text NOT NULL DEFAULT '',
                model text NOT NULL DEFAULT '',
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.commit()
    _ai_providers_ready = True


async def list_ai_providers() -> list[dict[str, Any]]:
    await _ensure_ai_providers_table()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {AI_PROVIDER_COLUMNS} FROM ai_providers ORDER BY key")
        return await cur.fetchall()


async def get_ai_provider(key: str) -> dict[str, Any] | None:
    await _ensure_ai_providers_table()
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {AI_PROVIDER_COLUMNS} FROM ai_providers WHERE key = %s", (key,))
        return await cur.fetchone()


async def upsert_ai_provider(key: str, *, base_url: str, api_key: str | None, model: str) -> dict[str, Any]:
    """api_key=None keeps whatever secret is already stored - lets the
    dashboard change base_url/model without having to resend the secret
    every time."""
    await _ensure_ai_providers_table()
    async with await _connect() as conn, conn.cursor() as cur:
        if api_key is None:
            await cur.execute(
                f"""
                INSERT INTO ai_providers (key, base_url, model)
                VALUES (%s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    base_url = EXCLUDED.base_url,
                    model = EXCLUDED.model,
                    updated_at = now()
                RETURNING {AI_PROVIDER_COLUMNS}
                """,
                (key, base_url, model),
            )
        else:
            await cur.execute(
                f"""
                INSERT INTO ai_providers (key, base_url, api_key, model)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    base_url = EXCLUDED.base_url,
                    api_key = EXCLUDED.api_key,
                    model = EXCLUDED.model,
                    updated_at = now()
                RETURNING {AI_PROVIDER_COLUMNS}
                """,
                (key, base_url, api_key, model),
            )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]
