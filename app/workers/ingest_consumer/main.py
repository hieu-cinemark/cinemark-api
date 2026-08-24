"""Reads scraped posts/comments off Kafka (published by spider-hub's
spiders - see social_crawler/services/kafka.py there) and persists them.
Runs as its own long-lived process, separate from the FastAPI app:

    python -m app.workers.ingest_consumer.main

Each message is committed independently in its own DB transaction, and one
bad message (unresolvable keyword, DB error, malformed payload) is logged
and skipped rather than killing the whole consumer - a poison-pill message
must not take down ingestion for every other message behind it.

Two throughput fixes over the original straight-line version, both driven
by the same observation: R2 media archival (download from Facebook's CDN,
re-upload to R2) is two sequential network round trips *per media file*,
which dominated ingest time far more than the DB writes did - spider-hub's
local JSON feed export finishes fast because it does no network I/O at all,
while this path used to block each message on archival before moving on.

1. Archival runs as bounded-concurrency background tasks (_archive_semaphore),
   fired after the DB commit instead of awaited inline in persist_post/
   persist_comment (see app/services/posts.py, app/services/comments.py).
2. Kafka messages themselves are handled with bounded concurrency
   (message_semaphore in run(), sized by _MESSAGE_CONCURRENCY) instead of
   one full message (DB write + scheduling archival) at a time.

Neither change strengthens delivery guarantees beyond what already existed
(auto-commit was already advancing offsets independently of whether a
handler's try/except swallowed a failure) - this only removes artificial
serialization, not anything that was providing real exactly-once behavior.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger
from app.db.session import SessionLocal
from app.models.comment import Comment
from app.models.post import Post
from app.services.comments import persist_comment
from app.services.keywords import KeywordRepository
from app.services.posts import PostRepository, persist_post
from app.services.storage import archive_media_url

logger = get_logger(__name__)

RAW_POSTS_TOPIC = "raw_posts"
RAW_COMMENTS_TOPIC = "raw_comments"
TOPICS = (RAW_POSTS_TOPIC, RAW_COMMENTS_TOPIC)
CONSUMER_GROUP = "cinemark-api.ingest"

# Bounded independently: message handling is DB-bound (fast, pooled
# connections), archival is network-bound (slow, external services) - each
# gets its own ceiling so a burst of media-heavy posts can't starve message
# throughput, and vice versa.
_MESSAGE_CONCURRENCY = 8
_ARCHIVE_CONCURRENCY = 8
_archive_semaphore = asyncio.Semaphore(_ARCHIVE_CONCURRENCY)


async def _archive_post_media(post_id: uuid.UUID, platform: str, media_url: str) -> None:
    async with _archive_semaphore:
        r2_key = await archive_media_url(media_url, prefix=f"posts/{platform}")
    if not r2_key:
        return
    async with SessionLocal() as session:
        post = await session.get(Post, post_id)
        if post is None:
            return
        media = dict(post.media or {})
        media["r2_key"] = r2_key
        post.media = media
        await session.commit()


async def _archive_comment_media(comment_id: uuid.UUID, media_urls: dict[str, str]) -> None:
    archived: dict[str, str] = {}
    for name, url in media_urls.items():
        async with _archive_semaphore:
            r2_key = await archive_media_url(url, prefix="comments")
        if r2_key:
            archived[name] = r2_key
    if not archived:
        return
    async with SessionLocal() as session:
        comment = await session.get(Comment, comment_id)
        if comment is None:
            return
        media_archive = dict(comment.media_archive or {})
        media_archive.update(archived)
        comment.media_archive = media_archive
        await session.commit()


async def handle_post(payload: dict[str, Any]) -> None:
    keyword_id = payload.get("keyword_id")
    if not keyword_id:
        logger.warning("post_missing_keyword_id", post_id=payload.get("post_id"))
        return

    async with SessionLocal() as session:
        try:
            keyword = await KeywordRepository(session).get(keyword_id)
            if keyword is None:
                logger.warning("post_unknown_keyword_id", keyword_id=keyword_id, post_id=payload.get("post_id"))
                return
            post, media_url = await persist_post(session, movie_id=keyword.movie_id, keyword_id=keyword.id, payload=payload)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    if media_url:
        asyncio.create_task(_archive_post_media(post.id, post.platform, media_url))

    logger.info("post_persisted", platform=payload.get("platform"), post_id=payload.get("post_id"), id=str(post.id))


async def handle_comment(payload: dict[str, Any]) -> None:
    post_external_id = payload.get("post_id")
    if not post_external_id:
        logger.warning("comment_missing_post_id", comment_id=payload.get("comment_id"))
        return

    async with SessionLocal() as session:
        try:
            post = await PostRepository(session).get_by_external_id(
                platform=payload["platform"], external_id=post_external_id
            )
            if post is None:
                # The post this comment belongs to hasn't been ingested yet
                # (raced ahead of it, or was never crawled) - nothing to
                # attach to, since comments.post_id is NOT NULL. Dropped,
                # not retried: the comments spider will surface it again on
                # its next run against the same post.
                logger.warning("comment_unknown_post", platform=payload.get("platform"), post_external_id=post_external_id)
                return
            comment, media_to_archive = await persist_comment(session, post=post, payload=payload)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    if media_to_archive:
        asyncio.create_task(_archive_comment_media(comment.id, media_to_archive))

    logger.info("comment_persisted", platform=payload.get("platform"), comment_id=payload.get("comment_id"), id=str(comment.id))


async def _process_message(message: Any, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        try:
            if message.topic == RAW_POSTS_TOPIC:
                await handle_post(message.value)
            elif message.topic == RAW_COMMENTS_TOPIC:
                await handle_comment(message.value)
        except Exception as exc:
            logger.error("ingest_message_failed", topic=message.topic, error=str(exc))


async def run() -> None:
    consumer = AIOKafkaConsumer(
        *TOPICS,
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

    logger.info("ingest_consumer_started", topics=TOPICS, group=CONSUMER_GROUP, message_concurrency=_MESSAGE_CONCURRENCY)
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
