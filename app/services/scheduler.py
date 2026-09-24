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

from app.core.logging import get_logger
from app.services import platform_config_db as db
from app.services.d1 import get_enabled_keywords, list_posts_needing_comments
from app.services.kafka import publish_comments_crawl_request, publish_crawl_request, publish_nurture_request
from app.services.platforms import COMMENT_CRAWL_PLATFORMS

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
_POLL_INTERVAL_SECONDS = 30

_task: asyncio.Task[None] | None = None


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
        if sched["run_time"] != current_hm:
            continue
        last = sched["last_triggered_date"]
        # dict_row from psycopg gives back a real date object, not a string
        # - compare against the same shape rather than the ISO string.
        if last is not None and last.isoformat() == today:
            continue
        platform = sched["platform"]
        await db.mark_crawl_schedule_triggered(platform, today)
        logger.info("scheduled_crawl_firing", platform=platform, run_time=current_hm)
        # Don't await inline - a slow Kafka publish or D1 query for one
        # platform shouldn't delay checking (or firing) every other
        # platform's schedule in the same tick.
        asyncio.create_task(
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
                platform=platform, post_external_id=post["external_id"], post_url=post["url"]
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
        if sched["run_time"] != current_hm:
            continue
        last = sched["last_triggered_date"]
        if last is not None and last.isoformat() == today:
            continue
        platform = sched["platform"]
        await db.mark_comment_crawl_schedule_triggered(platform, today)
        logger.info("scheduled_comments_firing", platform=platform, run_time=current_hm)
        # Don't await inline - same reasoning as _tick below: a slow sweep
        # for one platform shouldn't delay checking every other platform's
        # schedule (posts or comments) in the same tick.
        asyncio.create_task(_trigger_comments_platform(platform, top_n=int(sched.get("top_n") or 100)))


async def _loop() -> None:
    while True:
        try:
            await _tick()
        except Exception as exc:
            # A bad tick (D1/Kafka/DB hiccup) must not kill the loop - the
            # next scheduled run, possibly for a different platform, still
            # needs its chance to fire.
            logger.error("scheduler_tick_failed", error=str(exc))
        try:
            await _comments_tick()
        except Exception as exc:
            logger.error("scheduler_comments_tick_failed", error=str(exc))
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
