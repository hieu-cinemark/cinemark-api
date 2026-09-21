"""Publishes crawl-trigger requests to Kafka - the producer side of
spider-hub's crawl_request_consumer.py, which listens for these and
launches the matching `scrapy crawl` subprocess. Both the per-platform
/<platform>/run routes (app/api/routes/platform_scraper.py, manual "run"
button) and the daily scheduler script publish through the same function,
so a manual trigger and a scheduled one are indistinguishable downstream -
one code path, one contract.

Mirrors spider-hub's own social_crawler/services/kafka.py: fire-and-forget,
never blocks/fails the request over a Kafka outage - a crawl trigger that
can't be published just doesn't run, logged, not a 500."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import date
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger
from app.services.crawl_jobs import clear_drain
from app.services.task_queue import enqueue_published

logger = get_logger(__name__)

CRAWL_REQUESTS_TOPIC = "crawl_requests"

_producer: AIOKafkaProducer | None = None

# producer.start() only raises KafkaError for a *refused* connection - a
# broker that's up but not responding (overloaded host, coordinator
# reload/election in progress - confirmed live 2026-09-21 against the local
# dev Kafka container) can leave it hanging with no exception at all. Since
# this runs first in app/main.py's on_startup, an unbounded hang here blocks
# every route (even /health) from ever becoming reachable, same failure
# shape ensure_default_crawl_schedules had before it got the same treatment.
_START_TIMEOUT_SECONDS = 10.0


async def start_kafka_producer() -> None:
    global _producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    try:
        await asyncio.wait_for(producer.start(), timeout=_START_TIMEOUT_SECONDS)
    except KafkaError as exc:
        logger.warning("kafka_unavailable", error=str(exc))
        await producer.stop()
        return
    except TimeoutError:
        logger.warning("kafka_unavailable", error=f"producer.start() did not finish within {_START_TIMEOUT_SECONDS}s")
        asyncio.create_task(producer.stop())
        return
    _producer = producer


async def stop_kafka_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def publish_crawl_request(
    *,
    platform: str,
    keyword: str,
    keyword_id: str,
    max_pages: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    bfs_depth: int | None = None,
) -> bool:
    """Returns whether the request was actually published - callers decide
    what to tell the user if Kafka is down (e.g. still return 202 since the
    trigger endpoint's job is just to ask, not to guarantee delivery, or
    surface a warning - see app/api/routes/platform_scraper.py). keyword_id is D1's
    own keywords.id (see app/services/d1.py) - threaded through unchanged so
    the raw_posts message this crawl eventually produces carries an id the
    ingest consumer can resolve straight back through D1."""
    if _producer is None:
        logger.warning("kafka_producer_not_started", keyword_id=keyword_id)
        return False
    if platform == "tiktok" and not keyword.startswith("#"):
        logger.info("crawl_request_skipped_tiktok_text", keyword=keyword, keyword_id=keyword_id)
        return False
    # A prior Stop arms platform_drain for ~15m so backlog is skipped. A new
    # intentional trigger must lift that, or the crawl is published then
    # immediately logged as request_skipped_drain.
    await clear_drain(platform)
    # Lets spider-hub's crawl_request_consumer.py track this specific
    # subprocess (crawl_job:<platform> in Redis) so the dashboard's Stop
    # button (see app/services/crawl_jobs.py) has something to cancel by -
    # see that module's own docstring for the full mechanism.
    run_id = str(uuid.uuid4())
    value: dict[str, Any] = {"platform": platform, "keyword": keyword, "keyword_id": keyword_id, "run_id": run_id}
    if max_pages is not None:
        value["max_pages"] = max_pages
    # Date windows only exist on Facebook search. Threads/TikTok ignore them.
    if platform == "facebook":
        if start_date is not None:
            value["start_date"] = start_date.isoformat()
        if end_date is not None:
            value["end_date"] = end_date.isoformat()
    elif start_date is not None or end_date is not None:
        logger.info(
            "crawl_request_dates_dropped",
            platform=platform,
            keyword=keyword,
            keyword_id=keyword_id,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
        )
    if bfs_depth:
        value["bfs_depth"] = int(bfs_depth)
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"{platform}:{keyword_id}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), keyword_id=keyword_id)
        return False
    await enqueue_published(value)
    return True

# Shared dashboard/API default for comments crawls. Threads root feeds on
# large posts often need 40+ pages before paging_tokens ends; Facebook
# Comet pages ~10 comments each so 10 pages only covers ~100 top-level.
# Callers can still pass a smaller max_pages for a quick sample.
DEFAULT_COMMENTS_MAX_PAGES = 80


async def publish_channel_videos_request(
    *, username: str, max_pages: int | None = None, keyword_id: str | None = None
) -> bool:
    """Queues type=channel_videos for TikTok's tiktok_channel_videos spider
    (one @username channel grid). Free-text handle - no D1 "tracked
    channel" table yet."""
    handle = username.lstrip("@").strip()
    if not handle:
        return False
    payload: dict[str, Any] = {"username": handle, "run_id": str(uuid.uuid4())}
    if max_pages is not None:
        payload["max_pages"] = max_pages
    if keyword_id:
        payload["keyword_id"] = keyword_id
    return await publish_action_request("tiktok", "channel_videos", payload)


async def publish_comments_crawl_request(
    *, platform: str, post_external_id: str, post_url: str, max_pages: int = DEFAULT_COMMENTS_MAX_PAGES
) -> bool:
    """Publishes a type="comments" request, tagged for crawl_request_consumer.py's
    _run_comments_spider (spider-hub) to run that platform's comments
    spider against one specific post - only platforms in spider-hub's own
    COMMENTS_SPIDER_BY_PLATFORM (facebook, threads, tiktok - see get_comment_mapper's
    docstring here for the matching cinemark-api-side registry) actually
    have one; publishing for any other platform just gets logged and
    dropped on the consumer side. post_url is needed too, not just
    post_external_id: spider-hub bootstraps its comments-query cache
    (shared across every post for that account) from a real post URL the
    first time it's missing/expired, not from a bare numeric id."""
    run_id = str(uuid.uuid4())
    return await publish_action_request(
        platform,
        "comments",
        {"post_id": post_external_id, "post_url": post_url, "max_pages": max_pages, "run_id": run_id},
    )


async def publish_action_request(platform: str, action: str, payload: dict[str, Any]) -> bool:
    """Publishes a generic action request to the crawl_requests topic, tagged
    with type=action so crawl_request_consumer.py can handle it. Used for
    account checks and token refreshes."""
    if _producer is None:
        logger.warning("kafka_producer_not_started", platform=platform)
        return False
    key = str(uuid.uuid4())
    value: dict[str, Any] = {"type": action, "platform": platform, **payload}
    # Unlike publish_crawl_request (a bulk platform crawl - the same kind of
    # work Stop blocks, so clearing drain there is correct), these actions
    # are one-off targeted requests. Calling clear_drain(platform) here would
    # wipe bfs_drain/comments_drain/platform_drain for the WHOLE platform,
    # silently un-blocking any still-queued backlog left over from a Stop.
    # Tag just this message to skip the drain check for itself instead - see
    # crawl_request_consumer.py's _handle_request on the spider-hub side.
    # refresh_token/cookie_import are already exempt there by type, so they
    # don't need the tag (and must keep drain armed for everything else -
    # Stop mid-refresh still uses run_id-scoped cancellation).
    if action not in ("refresh_token", "cookie_import"):
        value["bypass_drain"] = True
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"{action}:{platform}:{key}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), platform=platform)
        return False
    await enqueue_published(value)
    return True

async def publish_tiktok_identity_reset(account_id: int) -> bool:
    """TikTok has no browser-bootstrap query/password flow to re-run like
    Facebook/Threads (see spider-hub's tiktok/auth/bootstrap.py) - a "reset
    cookies" trigger names one specific platform_accounts row instead, whose
    device_id/odinId gets a fresh headless capture pass. Still tagged
    type="refresh_token" so crawl_request_consumer.py's existing dispatch/
    job-tracking (dashboard "refreshing..."/Stop button) just works, no
    separate action type needed."""
    return await publish_action_request("tiktok", "refresh_token", {"account_id": account_id, "run_id": str(uuid.uuid4())})


async def publish_nurture_request(
    platform: str,
    account: str | None = None,
    *,
    like: bool = True,
    comment: bool = True,
    visits: int = 3,
) -> bool:
    """Queues type=nurture so spider-hub's crawl_request_consumer runs
    `python -m social_crawler.nurture_accounts` for facebook, threads, or
    tiktok. TikTok's own warm-up (visits /tag/<hashtag> pages, not the home
    feed - see spider-hub's nurture_accounts.py module docstring) ignores
    `comment` and treats `visits` as the number of hashtag pages to visit;
    there's no separate param for it so this one call shape covers every
    platform without the caller needing to know which fields apply."""
    payload: dict[str, Any] = {
        "like": like,
        "comment": comment,
        "visits": visits,
        "run_id": str(uuid.uuid4()),
    }
    if account:
        payload["account"] = account
    return await publish_action_request(platform, "nurture", payload)


async def publish_cookie_import_request(
    platform: str, account_key: str, cookies: str, run_id: str | None = None
) -> str | None:
    """Publishes type="cookie_import" so crawl_request_consumer.py's
    _import_cookies runs `bootstrap.py --cookies-file ... --account
    <account_key>` - the same command a human would otherwise run at a
    terminal to hand over cookies exported from a real, non-automated
    browser session (see spider-hub's facebook/threads auth/cookies.py's
    import_cookies) - then chains straight into a normal token refresh, so
    one trigger both creates and refreshes the session. Still fully
    human-authenticated: this only automates the "get the cookies into
    Redis, then capture tokens" steps, never the login itself - see
    app/api/routes/token_refresh.py's own docstring for the full flow.

    Returns the generated run_id (or None if the publish itself failed) -
    the caller feeds it straight into refresh_tracker.start_refresh so the
    dashboard's live log panel/status badge track this run."""
    run_id = run_id or str(uuid.uuid4())
    ok = await publish_action_request(platform, "cookie_import", {"account_key": account_key, "cookies": cookies, "run_id": run_id})
    return run_id if ok else None


async def publish_restore_session_request(platform: str, account_key: str, run_id: str | None = None) -> str | None:
    """Queues type=refresh_token pinned to one account_key so spider-hub
    recaptures GraphQL tokens from Redis storage_state / the cookie column
    without a human paste and without rotating to a different pool row."""
    run_id = run_id or str(uuid.uuid4())
    ok = await publish_action_request(
        platform, "refresh_token", {"account_key": account_key, "run_id": run_id}
    )
    return run_id if ok else None
