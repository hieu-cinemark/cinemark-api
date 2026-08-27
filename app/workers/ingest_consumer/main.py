"""Reads scraped posts off Kafka (published by spider-hub's spiders - see
social_crawler/services/kafka.py there) and mirrors them into D1 (see
app/services/d1.py). Runs as its own long-lived process, separate from the
FastAPI app:

    python -m app.workers.ingest_consumer.main

Each message is handled independently, and one bad message (unresolvable
keyword, D1 error, malformed payload) is logged and skipped rather than
killing the whole consumer - a poison-pill message must not take down
ingestion for every other message behind it.

Kafka messages are handled with bounded concurrency (message_semaphore in
run(), sized by _MESSAGE_CONCURRENCY) instead of one at a time, since each
message is now a couple of D1 HTTP round trips rather than a local DB write."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger
from app.services.d1 import get_keyword, persist_post
from app.services.platforms import get_post_mapper
from app.services.redis import REDIS_KEY_PREFIX, get_redis_client
from app.services.telegram import send_telegram_message

logger = get_logger(__name__)

RAW_POSTS_TOPIC = "raw_posts"
CONSUMER_GROUP = "cinemark-api.ingest"

_MESSAGE_CONCURRENCY = 8

# A burst of silent drops (unregistered platform mapper, malformed
# producer payload, a keyword that got disabled/deleted mid-flight) used to
# only ever show up as a WARNING log line nobody was watching - exactly how
# TikTok's missing mapper went unnoticed until someone happened to check
# the dashboard. This turns a *sustained* burst of one specific drop reason
# into a Telegram alert instead, without paging on every single message
# once a platform is already known to be broken.
DROP_ALERT_THRESHOLD = 10
# Rolling window, not a lifetime count - "10 drops in the last hour" is a
# meaningful signal; "10 drops since whenever this key first appeared,
# maybe weeks ago" isn't. Reset by re-arming the key's TTL each time a
# fresh window starts (the first increment after expiry/creation).
DROP_COUNTER_TTL_SECONDS = 3600

_DROP_ALERT_TEXT = {
    "mapper": "unregistered platform mapper (see app/services/platforms.py)",
    "missing_keyword_id": "posts arriving with no keyword_id (producer bug?)",
    "unknown_keyword_id": "posts referencing an unknown/disabled keyword_id",
}


async def _note_drop(platform: str, reason: str, **context: Any) -> None:
    """Atomically increments this (platform, reason)'s rolling-window
    counter and fires exactly one Telegram alert the instant it crosses
    DROP_ALERT_THRESHOLD - not once per message after that, so an ongoing
    outage doesn't spam the channel once it's already been reported."""
    client = get_redis_client()
    key = f"{REDIS_KEY_PREFIX}ingest_drop:{platform}:{reason}"
    count = await client.incr(key)
    if count == 1:
        await client.expire(key, DROP_COUNTER_TTL_SECONDS)
    if count == DROP_ALERT_THRESHOLD:
        details = " | ".join(f"{k}={v}" for k, v in context.items())
        await send_telegram_message(
            f"🚨 Ingest drop alert: {platform} - {_DROP_ALERT_TEXT[reason]}\n"
            f"{count} drops in the last {DROP_COUNTER_TTL_SECONDS // 60}m\n{details}"
        )


async def handle_post(payload: dict[str, Any]) -> None:
    platform = payload.get("platform")
    post_id = payload.get("post_id")

    mapper = get_post_mapper(platform)
    if mapper is None:
        logger.warning("post_unregistered_platform", platform=platform, post_id=post_id)
        await _note_drop(platform, "mapper", post_id=post_id)
        return

    keyword_id = payload.get("keyword_id")
    if not keyword_id:
        logger.warning("post_missing_keyword_id", platform=platform, post_id=post_id)
        await _note_drop(platform, "missing_keyword_id", post_id=post_id)
        return

    keyword = await get_keyword(keyword_id, platform=platform)
    if keyword is None:
        logger.warning("post_unknown_keyword_id", platform=platform, keyword_id=keyword_id, post_id=post_id)
        await _note_drop(platform, "unknown_keyword_id", post_id=post_id, keyword_id=keyword_id)
        return

    draft = mapper(payload)
    await persist_post(
        movie_id=keyword["movie_id"], keyword_id=keyword_id, keyword=keyword["keyword"], platform=platform, draft=draft
    )
    logger.info("post_persisted", platform=platform, post_id=draft.get("external_id"))


async def _process_message(message: Any, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        try:
            if message.topic == RAW_POSTS_TOPIC:
                await handle_post(message.value)
        except Exception as exc:
            logger.error("ingest_message_failed", topic=message.topic, error=str(exc))


async def run() -> None:
    consumer = AIOKafkaConsumer(
        RAW_POSTS_TOPIC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    try:
        await consumer.start()
    except KafkaError as exc:
        logger.error("kafka_connection_error", error=str(exc))
        return

    logger.info(
        "ingest_consumer_started", topic=RAW_POSTS_TOPIC, group=CONSUMER_GROUP, message_concurrency=_MESSAGE_CONCURRENCY
    )
    message_semaphore = asyncio.Semaphore(_MESSAGE_CONCURRENCY)
    pending: set[asyncio.Task[None]] = set()
    try:
        async for message in consumer:
            task = asyncio.create_task(_process_message(message, message_semaphore))
            pending.add(task)
            task.add_done_callback(pending.discard)
            # A cap well above _MESSAGE_CONCURRENCY (not equal to it) - the
            # semaphore already limits how many run *concurrently*; this
            # just stops `pending` itself from growing unbounded if the
            # Kafka read loop can enqueue faster than tasks finish.
            if len(pending) >= _MESSAGE_CONCURRENCY * 4:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
    except KafkaError as exc:
        logger.error("kafka_connection_error", error=str(exc))
    finally:
        if pending:
            await asyncio.wait(pending)
        await consumer.stop()
        logger.info("ingest_consumer_stopped")


if __name__ == "__main__":
    asyncio.run(run())
