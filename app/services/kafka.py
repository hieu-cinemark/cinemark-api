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

import json
import uuid
from datetime import date
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

CRAWL_REQUESTS_TOPIC = "crawl_requests"

_producer: AIOKafkaProducer | None = None


async def start_kafka_producer() -> None:
    global _producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    try:
        await producer.start()
    except KafkaError as exc:
        logger.warning("kafka_unavailable", error=str(exc))
        await producer.stop()
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
    # Lets spider-hub's crawl_request_consumer.py track this specific
    # subprocess (crawl_job:<platform> in Redis) so the dashboard's Stop
    # button (see app/services/crawl_jobs.py) has something to cancel by -
    # see that module's own docstring for the full mechanism.
    run_id = str(uuid.uuid4())
    value: dict[str, Any] = {"platform": platform, "keyword": keyword, "keyword_id": keyword_id, "run_id": run_id}
    if max_pages is not None:
        value["max_pages"] = max_pages
    if start_date is not None:
        value["start_date"] = start_date.isoformat()
    if end_date is not None:
        value["end_date"] = end_date.isoformat()
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"{platform}:{keyword_id}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), keyword_id=keyword_id)
        return False
    return True

# facebook_comments defaults to max_pages=1 (see spider-hub's
# comments.py), and its "root" query only returns ~2 comments per page (the
# small initial batch Facebook's own UI renders before you scroll) - a
# trigger that never overrides this always stops at ~2 comments regardless
# of how many the post actually has (confirmed happening for real - every
# comments/run trigger topped out at exactly 2). This is the default every
# call gets unless it asks for a different one.
DEFAULT_COMMENTS_MAX_PAGES = 10


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
    try:
        await _producer.send_and_wait(CRAWL_REQUESTS_TOPIC, key=f"{action}:{platform}:{key}", value=value)
    except KafkaError as exc:
        logger.warning("kafka_publish_failed", error=str(exc), platform=platform)
        return False
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


async def publish_token_refresh_request(platform: str) -> bool:
    """Publishes to the same crawl_requests topic as publish_crawl_request,
    tagged type="refresh_token" so crawl_request_consumer.py runs the given
    platform's auth bootstrap script (see spider-hub's
    social_crawler/spiders/<platform>/auth/bootstrap.py - facebook and
    threads both have one; scripts/refresh_token.sh already does the
    Facebook one on a 4h cron) instead of a scrapy spider."""
    return await publish_action_request(platform, "refresh_token", {"run_id": str(uuid.uuid4())})
