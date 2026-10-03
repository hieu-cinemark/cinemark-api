"""Hourly auto-login scheduler service.

Mirrors the shape of app/services/cleanup.py (same singleton jsonb
settings row + run_history table + resolve_settings helper): the
dashboard's PUT /settings/auto-login and POST /settings/auto-login/run
both flow through here, and the in-process scheduler loop in
app/services/scheduler.py calls run_auto_login_tick on every interval.

Why a scheduler inside cinemark-api at all, when spider-hub already has
its own auto_login/scheduler.py? Three reasons:

  1. ONE source of truth for "is auto-login on?". The dashboard
     settings row IS that source. spider-hub's AUTO_LOGIN_ENABLED env
     var was the original gate, but env vars + a separate daemon means
     the dashboard's "Enable auto-login" switch flipped a Postgres row
     that nothing on the spider-hub side ever checked, and the only way
     for an operator to know "is it actually on?" was to SSH into the
     spider-hub host and `cat /etc/spider-hub.env`. Having cinemark-api
     own the schedule means flipping the dashboard toggle reliably
     starts/stops the work on the next tick boundary, and a single
     `ps aux | grep cinemark-api` tells the operator what's running.

  2. RUN HISTORY needs to live somewhere the dashboard can read. Putting
     it in Supabase (auto_login_run_history table) and the run logic in
     cinemark-api means the dashboard's "last run" card pulls from the
     same Postgres the rest of settings already does - no new HTTP
     endpoint, no auth boundary, just a SELECT.

  3. KAFKA is the right transport to spider-hub. spider-hub is the
     process that owns Playwright, browser fingerprints, and the proxy
     pool. cinemark-api doesn't import patchright (and shouldn't - it's
     a FastAPI web server, not a headless browser host), so we publish
     one auto_login_requests Kafka message per account and let
     spider-hub's auto_login/consumer.py do the actual relogin.

What this module owns:

  * resolve_auto_login_settings(): read the singleton row, merge over
    AutoLoginSettings defaults, return a fully-validated object.
  * run_auto_login_tick(): one scheduler / manual-trigger tick. Reads
    settings, queries Supabase for accounts-needing-relogin, publishes
    one Kafka message per account, writes the run history row.
  * in_flight flag: like cleanup.py's purge_in_progress - the
    dashboard uses this to gray out its "Run now" button when a tick
    is mid-run.

Why we don't re-implement list_accounts_needing_relogin here: that
query lives in spider-hub's social_crawler/db/relogin.py because the
"needs relogin" predicate (dead cookies + not checkpointed + not
needs_manual) is the same predicate the spider-hub side already uses
for its own scheduler + consumer. We
read the same data via Supabase directly - same source of truth, no
duplicated business logic."""

from __future__ import annotations

import asyncio
from typing import Any

from app.clients.kafka import publish_auto_login_request
from app.core.logging import get_logger
from app.schemas.settings import (
    AutoLoginRunHistoryEntry,
    AutoLoginSettings,
    AutoLoginSettingsOut,
)
from app.services import platform_config_db as platform_cfg

logger = get_logger(__name__)

# Per the schema defaults - mirrors spider-hub's auto_login/scheduler.py
# so the dashboard's defaults match what a fresh
# AUTO_LOGIN_ENABLED=true env-only deploy would have produced.
DEFAULT_INTERVAL_SECONDS = 3600
DEFAULT_PLATFORMS = ("facebook", "threads")


# Tracks whether a tick is currently in flight. The dashboard's "Run
# now" button reads this to grey itself out + show "refreshing..."
# while a tick is running - same UX as the Cleanup schedule's
# `running: bool` field. Module-level + asyncio.Lock because two
# endpoints can both try to trigger (manual POST + scheduler tick
# firing simultaneously on the same interval boundary), and we want
# exactly one of them to actually start the work.
_in_flight = False
_in_flight_lock = asyncio.Lock()


def is_auto_login_in_flight() -> bool:
    """Used by the dashboard's "Run now" button - returns True while
    run_auto_login_tick is mid-execution so the dashboard can disable
    the trigger + show a spinner. False otherwise (including between
    scheduler ticks)."""
    return _in_flight


async def resolve_auto_login_settings() -> AutoLoginSettings:
    """Reads the singleton row from Supabase, merges the stored values
    over the AutoLoginSettings defaults (so a row from before a new
    field was added still gets the default for that field), and
    validates the result against the schema. Mirrors
    app/services/cleanup.py:resolve_cleanup_settings's shape exactly -
    same partial-update + over-defaults merge so the schema is the
    one source of truth for what each field's type/constraints are.

    The platforms field is normalized to a list (Postgres stores it
    as a comma-separated string for cross-DB compatibility, the
    schema accepts a list) and the sorted/alpha-set is what the
    spider-hub consumer reads in its key."""
    row = await platform_cfg.get_auto_login_settings()
    stored: dict[str, Any] = row.get("settings") or {}
    defaults = AutoLoginSettings()
    merged: dict[str, Any] = defaults.model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            # Re-validate via the schema for THIS key only so a single
            # bad stored value doesn't 500 the whole settings read.
            merged[key] = getattr(AutoLoginSettings.model_validate({key: value}), key)
        except Exception:
            logger.warning("auto_login_setting_invalid_stored_value", key=key)
    return AutoLoginSettings(**merged)


async def get_auto_login_settings_out() -> AutoLoginSettingsOut:
    """Convenience for the GET endpoint: wraps resolve + adds the
    metadata fields the schema exposes (`updated_at`). The defaults
    come from the schema class itself - no separate copy needed."""
    row = await platform_cfg.get_auto_login_settings()
    history = await platform_cfg.list_auto_login_run_history(limit=1)
    return AutoLoginSettingsOut(
        values=await resolve_auto_login_settings(),
        defaults=AutoLoginSettings(),
        running=is_auto_login_in_flight(),
        last_run_at=history[0]["started_at"] if history else None,
        updated_at=row.get("updated_at"),
    )


async def _list_accounts_needing_relogin(platform: str) -> list[dict[str, Any]]:
    """Replicates the SELECT in spider-hub's social_crawler/db/relogin.py
    (list_accounts_needing_relogin). Kept as an inline copy (not a
    shared module) because the two sides connect to Supabase with
    different driver libs (psycopg sync vs async) and the dashboard
    here never needs the password/totp_secret/cookie/token/email/
    email_password columns the spider-hub side does - we only need
    account_id (for the Kafka payload) and id (for the dashboard's
    "what would run" preview).

    Returns an empty list on a DB error (logs the failure) so a
    transient Supabase hiccup doesn't take down the whole scheduler
    tick - same defensive pattern as the spider-hub helper."""
    try:
        from app.services.platform_config_db import _connect

        async with await _connect() as conn, conn.cursor(row_factory=__import__("psycopg.rows", fromlist=["dict_row"]).dict_row) as cur:
            await cur.execute(
                """
                SELECT id, account_id, platform, last_check_status, last_checked_at
                FROM platform_accounts
                WHERE platform = %s AND enabled = true
                AND status != 'checkpoint'
                AND last_check_status = 'dead'
                AND needs_manual_login = false
                ORDER BY last_checked_at ASC NULLS LAST, id ASC
                """,
                (platform,),
            )
            rows = await cur.fetchall()
        return list(rows or [])
    except Exception as exc:
        logger.error("auto_login_list_accounts_failed", platform=platform, error=str(exc))
        return []


async def _tick_one_platform(
    platform: str, *, dry_run: bool
) -> tuple[dict[str, int], int, int]:
    """Runs the auto-login flow for one platform: query Supabase for
    candidates, publish one Kafka message per account, count outcomes.
    Returns (per_status_counters, kafka_published, kafka_publish_failed).

    Status counter keys:
      * attempted - how many accounts we tried to publish (== len(rows))
      * relogged_in - reserved for the spider-hub side's per-account
        outcome reporting (not populated here - spider-hub will write
        it to auto_login_run_history via a separate update path; this
        module writes zero for now and lets the history row's
        `kafka_published` column tell the dashboard "we DID dispatch
        these" vs the eventual post-tick summary. The full
        per-account relogged_in / needs_human / error breakdown
        lands in `auto_login_run_history.per_platform` via the
        spider-hub consumer's webhook - see its docstring.)
      * needs_human - same reservation as relogged_in
      * failed - same
      * error - same"""
    per_status: dict[str, int] = {
        "attempted": 0,
        "relogged_in": 0,
        "needs_human": 0,
        "failed": 0,
        "error": 0,
    }
    kafka_published = 0
    kafka_publish_failed = 0

    rows = await _list_accounts_needing_relogin(platform)
    per_status["attempted"] = len(rows)
    if not rows:
        return per_status, kafka_published, kafka_publish_failed

    logger.info(
        "auto_login_tick_platform_start",
        platform=platform,
        candidate_count=len(rows),
        dry_run=dry_run,
    )

    for row in rows:
        ok = await publish_auto_login_request(
            platform=platform,
            account_id=str(row["account_id"]),
            dry_run=dry_run,
        )
        if ok:
            kafka_published += 1
        else:
            kafka_publish_failed += 1
            # Don't bail on a single publish failure - log it and let
            # the next account try. A Kafka outage mid-tick is
            # expected to take out a chunk of one tick, not the whole
            # schedule. The dashboard's history table will show the
            # `kafka_publish_failed` count so an operator can spot a
            # partial outage after the fact.

    logger.info(
        "auto_login_tick_platform_end",
        platform=platform,
        attempted=per_status["attempted"],
        kafka_published=kafka_published,
        kafka_publish_failed=kafka_publish_failed,
        dry_run=dry_run,
    )
    return per_status, kafka_published, kafka_publish_failed


async def run_auto_login_tick(*, triggered_by: str = "schedule", force: bool = False) -> int:
    """Runs one tick of the auto-login scheduler. `triggered_by` is
    `"schedule"` for the in-process scheduler loop and `"manual"` for
    the dashboard's "Run now" button - recorded on the history row so
    the operator can tell the two apart later. `force=True` skips the
    enabled-gate (used by the scheduler loop's first call after startup
    so a freshly-restarted API doesn't silently drop the very first
    tick if the dashboard setting raced ahead of process start).

    Returns the auto_login_run_history.id of this run (or -1 if no run
    was recorded because the tick was a no-op due to enabled=false)."""
    settings = await resolve_auto_login_settings()
    if not settings.enabled and not force:
        logger.info(
            "auto_login_tick_skipped_disabled",
            triggered_by=triggered_by,
        )
        return -1

    async with _in_flight_lock:
        global _in_flight
        if _in_flight:
            # Two simultaneous triggers (manual POST + scheduler tick
            # firing at the same moment). The scheduler tick loses
            # silently here - the manual run already started, and the
            # dashboard's "Run now" button being grey is enough UX.
            logger.warning("auto_login_tick_already_in_flight", triggered_by=triggered_by)
            return -1
        _in_flight = True

    dry_run = settings.dry_run
    platforms = list(settings.platforms) or list(DEFAULT_PLATFORMS)
    interval_seconds = settings.interval_seconds or DEFAULT_INTERVAL_SECONDS

    run_id = -1
    error: str | None = None
    per_platform: dict[str, dict[str, int]] = {}
    kafka_published_total = 0
    kafka_publish_failed_total = 0
    try:
        run_id = await platform_cfg.record_auto_login_run_start(
            triggered_by=triggered_by,
            dry_run=dry_run,
            interval_seconds=interval_seconds,
            platforms=platforms,
        )
        logger.info(
            "auto_login_tick_start",
            run_id=run_id,
            triggered_by=triggered_by,
            dry_run=dry_run,
            platforms=platforms,
            interval_seconds=interval_seconds,
        )
        for platform in platforms:
            try:
                per_status, kafka_published, kafka_publish_failed = await _tick_one_platform(platform, dry_run=dry_run)
                per_platform[platform] = per_status
                kafka_published_total += kafka_published
                kafka_publish_failed_total += kafka_publish_failed
            except Exception as exc:
                # One platform's failure shouldn't kill the whole tick
                # (same defensive pattern as the cleanup tick's
                # try/except around purge_irrelevant_posts). Log the
                # error under the platform key so the dashboard can
                # show "facebook: <error>".
                logger.exception(
                    "auto_login_tick_platform_crashed",
                    platform=platform,
                    run_id=run_id,
                )
                per_platform[platform] = {
                    "attempted": 0,
                    "relogged_in": 0,
                    "needs_human": 0,
                    "failed": 0,
                    "error": 1,
                    "exception": str(exc),
                }
        logger.info(
            "auto_login_tick_end",
            run_id=run_id,
            triggered_by=triggered_by,
            total_attempted=sum(p.get("attempted", 0) for p in per_platform.values()),
            kafka_published=kafka_published_total,
            kafka_publish_failed=kafka_publish_failed_total,
            per_platform=per_platform,
        )
    except Exception as exc:
        logger.exception("auto_login_tick_crashed")
        error = str(exc)
    finally:
        if run_id > 0:
            try:
                await platform_cfg.record_auto_login_run_finish(
                    run_id,
                    per_platform=per_platform,
                    kafka_published=kafka_published_total,
                    kafka_publish_failed=kafka_publish_failed_total,
                    error=error,
                )
            except Exception:
                logger.exception("auto_login_history_persist_failed", run_id=run_id)
        async with _in_flight_lock:
            _in_flight = False

    return run_id


async def list_auto_login_history(*, limit: int = 20) -> list[AutoLoginRunHistoryEntry]:
    """Reads the most-recent-first list from auto_login_run_history
    and shapes each row into the pydantic schema the dashboard
    consumes. Same shape cleanup.py:list_cleanup_history exposes -
    both wrap platform_config_db.list_*_run_history and convert to
    the dashboard-friendly schema."""
    rows = await platform_cfg.list_auto_login_run_history(limit=limit)
    out: list[AutoLoginRunHistoryEntry] = []
    for row in rows:
        out.append(
            AutoLoginRunHistoryEntry(
                started_at=row["started_at"],
                finished_at=row.get("finished_at"),
                triggered_by=row.get("triggered_by") or "schedule",
                dry_run=bool(row.get("dry_run")),
                interval_seconds=int(row.get("interval_seconds") or DEFAULT_INTERVAL_SECONDS),
                platforms=[p.strip() for p in (row.get("platforms") or "").split(",") if p.strip()],
                per_platform=row.get("per_platform") or {},
                total_attempted=int(row.get("total_attempted") or 0),
                total_relogged_in=int(row.get("total_relogged_in") or 0),
                total_needs_human=int(row.get("total_needs_human") or 0),
                total_failed=int(row.get("total_failed") or 0),
                total_error=int(row.get("total_error") or 0),
                kafka_published=int(row.get("kafka_published") or 0),
                kafka_publish_failed=int(row.get("kafka_publish_failed") or 0),
                error=row.get("error"),
            )
        )
    return out