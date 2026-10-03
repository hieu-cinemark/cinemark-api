"""In-process daily crawl scheduler - the dashboard's own replacement for
both cinemark-api's OS-crontab-driven scripts/trigger_scheduled_crawl.sh
(a fixed "every 6h" cadence, only ever changeable by editing a crontab on
the host) and cinemark-scraper's Cloudflare Cron Triggers for Threads/
TikTok (wrangler.toml, needs a Worker redeploy to change). Going forward,
this is the one source of truth for when a platform's crawl runs - editable
from the dashboard's "Crawl schedule" card (see app/services/
platform_config_db.py's crawl_schedules CRUD), no crontab/Worker deploy
needed to change it.

One asyncio task for the process's lifetime (started/stopped from
app/main.py's startup/shutdown hooks), waking every _POLL_INTERVAL_SECONDS
to check every enabled crawl_schedules row's run_time ("HH:MM", a fixed
Asia/Ho_Chi_Minh clock - see that column's own comment in spider-hub's
scripts/dev_db_schema.sql) against the current wall-clock minute.
last_triggered_date is the re-entrancy guard: without it, a poll loop
checking every 30s would fire the same scheduled run maybe a dozen times
during the one minute its run_time matches.

Reuses exactly the same get_enabled_keywords + publish_crawl_request calls
app/api/routes/platform_scraper.py's POST /<platform>/run route makes for
"every enabled keyword" - a scheduled run and a manual "run everything"
button click are the same operation, just triggered differently.

_comments_tick below is the same shape for comment_crawl_schedules - a
second, independent per-platform daily time that sweeps every enabled
keyword's top-engagement posts for ones still missing comments (see
app/services/d1.py's list_posts_needing_comments) and queues a comments
crawl for each, the same selection scripts/trigger_recent_keyword_comments.py
already does by hand."""

from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from app.clients.kafka import publish_comments_crawl_request, publish_crawl_request, publish_nurture_request
from app.clients.redis import get_redis_client
from app.core.logging import get_logger
from app.services import platform_config_db as db
from app.services.auto_login import resolve_auto_login_settings, run_auto_login_tick
from app.services.cleanup import resolve_cleanup_settings, run_purge
from app.services.d1 import get_enabled_keywords, list_posts_needing_comments
from app.services.platforms import COMMENT_CRAWL_PLATFORMS

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
_POLL_INTERVAL_SECONDS = 30
# One tick must never be able to stall the loop: 2026-09-30 a Postgres
# socket opened on a previous network hung the tick (and every schedule
# after it) for a whole night with the API still answering /health.
_TICK_TIMEOUT_SECONDS = 25
# A run missed because the loop was stuck or the host was asleep still
# fires if the loop recovers within this window after its run_time.
_CATCH_UP_MINUTES = 30


def _is_due(run_time: str, now: datetime) -> bool:
    try:
        hour, minute = (int(part) for part in run_time.split(":", 1))
    except ValueError:
        return False
    late = (now.hour * 60 + now.minute) - (hour * 60 + minute)
    return 0 <= late <= _CATCH_UP_MINUTES

_task: asyncio.Task[None] | None = None
# Strong refs for fire-and-forget tasks - the event loop only keeps weak
# ones, so an unreferenced task can be garbage-collected mid-run.
_background_tasks: set[asyncio.Task] = set()


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


NURTURE_PLATFORMS = {"facebook", "threads", "tiktok"}


async def _trigger_platform(platform: str, *, nurture_before: bool = False, nurture_after: bool = False) -> None:
    # Same Kafka topic and per-platform consumer as crawls, so a nurture
    # published first actually runs to completion before the keyword
    # crawls behind it; published last, it runs after they drain.
    if nurture_before and platform in NURTURE_PLATFORMS:
        ok = await publish_nurture_request(platform)
        logger.info("scheduled_nurture_queued", platform=platform, when="before", ok=ok)
    elif nurture_before:
        logger.info("scheduled_nurture_skipped_platform", platform=platform, when="before")

    keywords = await get_enabled_keywords(platform=platform)
    if not keywords:
        logger.info("scheduled_crawl_no_keywords", platform=platform)
    else:
        published = 0
        for keyword in keywords:
            ok = await publish_crawl_request(platform=platform, keyword=keyword["keyword"], keyword_id=keyword["id"])
            if ok:
                published += 1
        logger.info(
            "scheduled_crawl_triggered",
            platform=platform,
            requested=len(keywords),
            published=published,
            telegram=True,
        )

    if nurture_after and platform in NURTURE_PLATFORMS:
        ok = await publish_nurture_request(platform)
        logger.info("scheduled_nurture_queued", platform=platform, when="after", ok=ok)
    elif nurture_after:
        logger.info("scheduled_nurture_skipped_platform", platform=platform, when="after")


async def _tick() -> None:
    now = datetime.now(TIMEZONE)
    current_hm = now.strftime("%H:%M")
    today = now.date().isoformat()

    schedules = await db.list_crawl_schedules()
    for sched in schedules:
        if not sched["enabled"]:
            continue
        if not _is_due(sched["run_time"], now):
            continue
        last = sched["last_triggered_date"]
        # dict_row from psycopg gives back a real date object, not a string
        # - compare against the same shape rather than the ISO string.
        if last is not None and last.isoformat() == today:
            continue
        platform = sched["platform"]
        await db.mark_crawl_schedule_triggered(platform, today)
        logger.info("scheduled_crawl_firing", platform=platform, run_time=sched["run_time"], fired_at=current_hm)
        # Don't await inline - a slow Kafka publish or D1 query for one
        # platform shouldn't delay checking (or firing) every other
        # platform's schedule in the same tick.
        _spawn(
            _trigger_platform(
                platform,
                nurture_before=bool(sched.get("nurture_before")),
                nurture_after=bool(sched.get("nurture_after")),
            )
        )


async def _trigger_comments_platform(platform: str, *, top_n: int) -> None:
    """For every enabled keyword on `platform`, queues a comments crawl for
    that keyword's top `top_n`-by-engagement posts that still have zero
    comments stored - same selection scripts/trigger_recent_keyword_comments.py
    already does by hand, just run on a schedule instead of manually."""
    keywords = await get_enabled_keywords(platform=platform)
    if not keywords:
        logger.info("scheduled_comments_no_keywords", platform=platform)
        return
    published = 0
    for keyword in keywords:
        posts = await list_posts_needing_comments(platform=platform, keyword_id=keyword["id"], top_n=top_n)
        for post in posts:
            if not post.get("url"):
                continue
            ok = await publish_comments_crawl_request(
                platform=platform, post_external_id=post["external_id"], post_url=post["url"], bypass_drain=False
            )
            if ok:
                published += 1
    logger.info("scheduled_comments_triggered", platform=platform, keywords=len(keywords), published=published, telegram=True)


async def _comments_tick() -> None:
    now = datetime.now(TIMEZONE)
    current_hm = now.strftime("%H:%M")
    today = now.date().isoformat()

    schedules = await db.list_comment_crawl_schedules()
    for sched in schedules:
        if not sched["enabled"]:
            continue
        if sched["platform"] not in COMMENT_CRAWL_PLATFORMS:
            continue
        if not _is_due(sched["run_time"], now):
            continue
        last = sched["last_triggered_date"]
        if last is not None and last.isoformat() == today:
            continue
        platform = sched["platform"]
        await db.mark_comment_crawl_schedule_triggered(platform, today)
        logger.info("scheduled_comments_firing", platform=platform, run_time=sched["run_time"], fired_at=current_hm)
        # Don't await inline - same reasoning as _tick below: a slow sweep
        # for one platform shouldn't delay checking every other platform's
        # schedule (posts or comments) in the same tick.
        _spawn(_trigger_comments_platform(platform, top_n=int(sched.get("top_n") or 100)))


_PURGE_KEY = "cinemark_api:cleanup:irrelevant_posts:last_run"
_purge_running = False


async def _run_purge(triggered_by: str = "schedule") -> None:
    """One purge run (cleanup.run_purge records the history row and holds
    the cross-process lock). The in-process `_purge_running` flag is what
    the dashboard's "running" indicator reads."""
    global _purge_running
    try:
        await run_purge(triggered_by=triggered_by)
    except Exception as exc:
        logger.error("irrelevant_purge_failed", error=str(exc), telegram=True)
    finally:
        _purge_running = False


async def _cleanup_tick() -> None:
    global _purge_running
    if _purge_running:
        return
    # Dashboard-stored knobs take precedence over the env defaults baked
    # into the Settings object; an unset/empty cleanup_settings row falls
    # back to those env values via resolve_cleanup_settings().
    cfg = await resolve_cleanup_settings()
    if not cfg["enabled"]:
        return
    now = datetime.now(TIMEZONE)
    if not _is_due(cfg["run_time"], now):
        return
    today = now.date().isoformat()
    redis = get_redis_client()
    if await redis.get(_PURGE_KEY) == today:
        return
    await redis.set(_PURGE_KEY, today, ex=3 * 24 * 60 * 60)
    _purge_running = True
    logger.info("irrelevant_purge_firing", run_time=cfg["run_time"])
    _spawn(_run_purge(triggered_by="schedule"))


async def run_purge_now() -> bool:
    """Manual trigger from the dashboard. Returns False if a run is already
    in flight (the dashboard should surface that to the user instead of
    spinning forever waiting for a result) - the schedule tick does the
    same _purge_running check above."""
    global _purge_running
    if _purge_running:
        return False
    _purge_running = True
    _spawn(_run_purge(triggered_by="manual"))
    return True


def purge_in_progress() -> bool:
    return _purge_running


# --- Auto-login scheduler tick ---
# Different cadence than crawl/cleanup/comments: hourly-by-default rather
# than daily. Rather than checking on every 30s poll whether an hour has
# elapsed (which would force the scheduler.py loop to track timestamps),
# we run an inner tick that:
#   * Reads the operator-configured interval_seconds from auto_login_settings
#     each time it wakes up, so changing the interval in the dashboard
#     takes effect on the next wake (no API restart needed).
#   * Compares an in-memory "last tick at" against the current time.
# Falls back to a default 3600s if the settings row is empty/missing
# (matches the env-var fallback auto_login_scheduler.py has used since
# day one). Errors inside run_auto_login_tick are caught + logged +
# persisted to auto_login_run_history by the service; this loop just
# sleeps and reschedules.
_auto_login_last_tick_at: float | None = None


async def _auto_login_tick() -> None:
    global _auto_login_last_tick_at
    try:
        cfg = await resolve_auto_login_settings()
    except Exception:
        # Settings table doesn't exist yet / Supabase hiccup / etc. -
        # log and skip this 30s window, the next tick will try again.
        logger.error("scheduler_auto_login_settings_read_failed")
        return
    if not cfg.enabled:
        # Don't even sleep - the operator wants auto-login off, we
        # just stop checking. Re-enabling the toggle reads the row
        # again on the next 30s poll, so flipping it in the
        # dashboard takes effect within 30s.
        _auto_login_last_tick_at = None
        return
    interval = int(cfg.interval_seconds or 3600)
    now = datetime.now(TIMEZONE).timestamp()
    if _auto_login_last_tick_at is None:
        # Seed from the last recorded run instead of firing right away - the
        # API restarts on every code save under `uvicorn --reload`, and each
        # restart would otherwise publish a fresh round of real logins.
        history = await db.list_auto_login_run_history(limit=1)
        if history:
            _auto_login_last_tick_at = history[0]["started_at"].timestamp()
    if _auto_login_last_tick_at is not None and (now - _auto_login_last_tick_at) < interval:
        return
    _auto_login_last_tick_at = now
    logger.info(
        "scheduler_auto_login_firing",
        interval_seconds=interval,
        platforms=list(cfg.platforms),
        dry_run=cfg.dry_run,
    )
    # Fire and forget - a slow per-account Kafka publish (one per
    # dead account, up to a few hundred per platform) shouldn't block
    # the scheduler loop's other ticks (crawl/comments/cleanup). The
    # service's _in_flight guard prevents a second scheduled tick
    # from racing the first even though we don't await here.
    _spawn(run_auto_login_tick(triggered_by="schedule", force=True))


async def _loop() -> None:
    while True:
        try:
            await asyncio.wait_for(_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            # A bad tick (D1/Kafka/DB hiccup) must not kill the loop - the
            # next scheduled run, possibly for a different platform, still
            # needs its chance to fire.
            logger.error("scheduler_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_cleanup_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_cleanup_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            logger.error("scheduler_cleanup_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_comments_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_comments_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            logger.error("scheduler_comments_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_auto_login_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_auto_login_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            # A bad auto_login_settings read or a thrown-from-create_task
            # error must not kill the loop.
            logger.error("scheduler_auto_login_tick_failed", error=str(exc))
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)


def start() -> None:
    global _task
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info("scheduler_started")


def stop() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        _task = None
