"""Reads scraped posts and comments off Kafka (published by spider-hub's
spiders - see social_crawler/services/kafka.py there) and mirrors them into
D1 (see app/services/d1.py). Runs as its own long-lived process, separate
from the FastAPI app:

    python -m app.workers.ingest_consumer.main

Each message is handled independently, and one bad message (unresolvable
keyword, D1 error, malformed payload) is logged and skipped rather than
killing the whole consumer - a poison-pill message must not take down
ingestion for every other message behind it.

Two independent consumer loops, one per topic (see _run_topic_consumer),
not one consumer subscribed to both - split 2026-09-17 after confirming
live that raw_posts and raw_comments sharing one consumer group/semaphore
meant a comment-heavy post (hundreds of comments, each needing its own
message) could starve post ingestion of concurrency slots, and vice versa.
Each loop handles its own messages with bounded concurrency (sized by
_POST_MESSAGE_CONCURRENCY / _COMMENT_MESSAGE_CONCURRENCY) instead of one at
a time, since each message is now a couple of D1 HTTP round trips rather
than a local DB write.

Comment sentiment is classified out-of-band, not inline here anymore - see
handle_comment's own comment and scripts/backfill_comment_sentiment.py."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger
from app.services.d1 import (
    contains_keyword,
    get_keyword,
    get_post_by_external_id,
    persist_comment,
    persist_dropped_post,
    persist_post,
)
from app.services.platforms import get_comment_mapper, get_post_mapper
from app.services.redis import REDIS_KEY_PREFIX, get_redis_client
from app.services.relevance_phobert import classify_post_relevance
from app.services.telegram import send_telegram_message

logger = get_logger(__name__)

RAW_POSTS_TOPIC = "raw_posts"
RAW_COMMENTS_TOPIC = "raw_comments"
# Separate consumer groups (not one shared "cinemark-api.ingest" group
# subscribed to both topics, as this used to be) - see _run_topic_consumer's
# own docstring for why a comment flood and a post flood must not compete
# for the same concurrency budget.
CONSUMER_GROUP_POSTS = "cinemark-api.ingest.posts"
CONSUMER_GROUP_COMMENTS = "cinemark-api.ingest.comments"
# The single shared group these two replaced (see module docstring). Neither
# new group id has any committed offset on its first-ever run, so without
# migrating from here, auto_offset_reset="earliest" below would replay each
# topic's entire retained backlog through Kira/D1 the moment this ships.
_LEGACY_CONSUMER_GROUP = "cinemark-api.ingest"

_POST_MESSAGE_CONCURRENCY = 8
# Comments no longer call Kira inline (see handle_comment's own comment) -
# each one is now just a mapper call + one D1 write, cheap enough that a
# higher concurrency actually gets used instead of mostly waiting on Kira's
# global 2-slot semaphore the way it effectively did before.
_COMMENT_MESSAGE_CONCURRENCY = 24

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
    "d1_write_failed": "D1 write failed (see app/services/d1.py for details)",
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
        await persist_dropped_post(platform=platform, reason="mapper", payload=payload)
        return

    keyword_id = payload.get("keyword_id")
    if not keyword_id:
        logger.warning("post_missing_keyword_id", platform=platform, post_id=post_id)
        await _note_drop(platform, "missing_keyword_id", post_id=post_id)
        await persist_dropped_post(platform=platform, reason="missing_keyword_id", payload=payload)
        return

    keyword = await get_keyword(keyword_id, platform=platform)
    if keyword is None:
        logger.warning("post_unknown_keyword_id", platform=platform, keyword_id=keyword_id, post_id=post_id)
        await _note_drop(platform, "unknown_keyword_id", post_id=post_id, keyword_id=keyword_id)
        await persist_dropped_post(platform=platform, reason="unknown_keyword_id", payload=payload, keyword_id=keyword_id)
        return

    draft = mapper(payload)
    # The free substring check first - only spend a PhoBERT call on posts
    # it actually misses (no literal keyword/synonym/abbreviation match)
    # rather than classifying every single post regardless of whether the
    # cheap check already found one.
    ai_relevant = None
    relevance_label = None
    relevance_confidence = None
    if not contains_keyword(draft.get("content"), keyword["keyword"]):
        relevance = await classify_post_relevance(draft.get("content"), keyword.get("movie_title"), keyword["keyword"])
        if relevance:
            relevance_label = relevance["label"]
            relevance_confidence = relevance["confidence"]
            if relevance_label == "related":
                ai_relevant = True
            elif relevance_label == "not_related":
                # A confident (not "uncertain" - see
                # app/services/relevance_phobert.py's own confidence
                # threshold) verdict that this post isn't about the movie -
                # drop it instead of storing it with keyword_match=0 like
                # before, which never actually kept garbage out of the
                # posts table. A PhoBERT outage/misconfig (relevance is
                # None) must NOT drop anything - only a confident
                # "not_related" does, so ingestion never silently loses a
                # real post just because the local model had a bad moment.
                logger.info(
                    "post_dropped_irrelevant",
                    platform=platform,
                    post_id=post_id,
                    keyword_id=keyword_id,
                    confidence=relevance_confidence,
                )
                await persist_dropped_post(
                    platform=platform, reason="phobert_irrelevant", payload=payload, keyword_id=keyword_id
                )
                return
            # "uncertain": ai_relevant stays None (falls back to the
            # substring check below, which we already know is False here)
            # - relevance_label/confidence are still stored, so this post
            # doesn't need label_posts_relevance.py's batch sweep to pick
            # it up later; it's already been classified, just inconclusively.
    ok = await persist_post(
        movie_id=keyword["movie_id"],
        keyword_id=keyword_id,
        keyword=keyword["keyword"],
        platform=platform,
        draft=draft,
        ai_relevant=ai_relevant,
        relevance_label=relevance_label,
        relevance_confidence=relevance_confidence,
    )
    if not ok:
        await _note_drop(platform, "d1_write_failed", post_id=post_id)
        await persist_dropped_post(platform=platform, reason="d1_write_failed", payload=payload, keyword_id=keyword_id)
        return
    logger.info("post_persisted", platform=platform, post_id=draft.get("external_id"))


async def handle_comment(payload: dict[str, Any]) -> None:
    platform = payload.get("platform")
    external_post_id = payload.get("post_id")

    mapper = get_comment_mapper(platform)
    if mapper is None:
        # No persist_dropped_post-style archive for comments (unlike
        # handle_post) - comments are supplementary to the post they belong
        # to, which is already durably stored/re-fetchable by post_id, so
        # losing an unregistered-platform comment isn't the same kind of
        # unrecoverable loss a whole dropped post would be.
        logger.warning("comment_unregistered_platform", platform=platform, post_id=external_post_id)
        return

    post = await get_post_by_external_id(platform, external_post_id) if external_post_id else None
    if post is None:
        # The post this comment belongs to isn't in D1 yet (or never will
        # be - e.g. content_too_short skipped it, see persist_post) - a
        # comment can't exist without its parent row (post_id has a NOT
        # NULL FK, see cinemark-scraper's schema.ts), so there's nothing
        # to attach it to.
        logger.warning("comment_unknown_post", platform=platform, post_id=external_post_id)
        return

    draft = mapper(payload)
    # PhoBERT is local/fast (HTTP to phobert-classifier/serve.py), so
    # classifying inline is fine again - unlike the old Kira path that
    # shared a process-wide concurrency=2 semaphore with post relevance
    # and stalled ingest under comment floods. Fail-open: None on any
    # error, comment still persists; backfill_comment_sentiment.py can
    # fill gaps later.
    from app.kira.sentiment import classify_sentiment

    sentiment = await classify_sentiment(draft.get("message"))
    ok = await persist_comment(
        post_id=post["id"], platform=platform, draft=draft, sentiment=sentiment
    )
    if not ok:
        logger.warning("d1_comment_persist_failed", platform=platform, post_id=external_post_id)
        return
    logger.info(
        "comment_persisted",
        platform=platform,
        post_id=external_post_id,
        sentiment=sentiment,
    )


class _OffsetTracker:
    """Commits an offset only once every message up to and including it has
    actually finished processing - not on aiokafka's default timer, which
    commits based on how far the `async for` loop has iterated regardless
    of whether the concurrently-running task for that message is done.
    Without this, a message dispatched to a task that's still mid-flight
    (e.g. blocked in Kira's retry/backoff) can have its offset committed by
    the timer before the task finishes; a crash in that window loses the
    message for good, since Kafka won't redeliver an offset already
    committed past. Messages within one partition are dispatched in the
    order aiokafka yields them, so a per-partition queue of dispatched
    offsets plus a set of finished ones is enough to find the highest
    *contiguous* finished offset - the only one safe to commit."""

    def __init__(self) -> None:
        self._dispatched: dict[TopicPartition, deque[int]] = defaultdict(deque)
        self._finished: dict[TopicPartition, set[int]] = defaultdict(set)

    def dispatched(self, tp: TopicPartition, offset: int) -> None:
        self._dispatched[tp].append(offset)

    def finished(self, tp: TopicPartition, offset: int) -> dict[TopicPartition, int]:
        """Marks one offset done and returns any {partition: next_offset}
        entries that became safe to commit as a result."""
        self._finished[tp].add(offset)
        queue = self._dispatched[tp]
        done = self._finished[tp]
        advanced = None
        while queue and queue[0] in done:
            done.discard(queue[0])
            advanced = queue.popleft()
        if advanced is None:
            return {}
        return {tp: advanced + 1}


async def _process_message(message: Any, semaphore: asyncio.Semaphore, tracker: _OffsetTracker, consumer: AIOKafkaConsumer) -> None:
    tp = TopicPartition(message.topic, message.partition)
    async with semaphore:
        try:
            if message.topic == RAW_POSTS_TOPIC:
                await handle_post(message.value)
            elif message.topic == RAW_COMMENTS_TOPIC:
                await handle_comment(message.value)
        except Exception as exc:
            logger.error("ingest_message_failed", topic=message.topic, error=str(exc))
        finally:
            # Committed regardless of success/failure - a message this
            # process can't handle (bad payload, unresolvable keyword) is
            # deliberately skipped, not retried forever (see module
            # docstring); only a crash mid-processing should redeliver it.
            to_commit = tracker.finished(tp, message.offset)
            if to_commit:
                try:
                    await consumer.commit(to_commit)
                except KafkaError as exc:
                    logger.error("kafka_commit_failed", error=str(exc))


async def _seed_offsets_from_legacy_group(consumer: AIOKafkaConsumer, topic: str, group_id: str) -> None:
    """One-time migration for the group split described in the module
    docstring: for each partition this (new) consumer was just assigned,
    if the new group has no committed offset yet but the retired
    cinemark-api.ingest group does, seek there and commit it - so the split
    picks up where the old shared consumer left off instead of replaying
    the whole topic. A partition with no offset in the old group either
    (fresh cluster, or the old consumer never reached it) is left alone and
    falls through to auto_offset_reset as normal."""
    assignment = consumer.assignment()
    if not assignment:
        return
    unseeded = [tp for tp in assignment if await consumer.committed(tp) is None]
    if not unseeded:
        return

    legacy = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers, group_id=_LEGACY_CONSUMER_GROUP)
    await legacy.start()
    try:
        seeded: dict[TopicPartition, int] = {}
        for tp in unseeded:
            offset = await legacy.committed(tp)
            if offset is not None:
                consumer.seek(tp, offset)
                seeded[tp] = offset
    finally:
        await legacy.stop()

    if seeded:
        await consumer.commit(seeded)
        logger.info(
            "ingest_consumer_offsets_migrated",
            topic=topic,
            group=group_id,
            partitions={f"{tp.topic}-{tp.partition}": offset for tp, offset in seeded.items()},
        )


async def _run_topic_consumer(topic: str, group_id: str, concurrency: int) -> None:
    """One independent consumer loop for a single topic (posts or
    comments) - raw_posts and raw_comments used to share one consumer
    group and one concurrency budget, so a comment flood (one post can
    have hundreds) queued up behind the same semaphore slots a post
    needed, and vice versa. Separate consumer groups also mean each
    topic's own lag is independently visible via kafka-consumer-groups.sh,
    instead of one blended number that can't say which topic is actually
    behind."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        # Manual, per-message commit (see _OffsetTracker) instead of the
        # default timer-based auto-commit, which is decoupled from whether
        # a message's own concurrent task has actually finished.
        enable_auto_commit=False,
    )

    try:
        await consumer.start()
    except KafkaError as exc:
        logger.error("kafka_connection_error", topic=topic, error=str(exc))
        return
    try:
        await _seed_offsets_from_legacy_group(consumer, topic, group_id)
    except KafkaError as exc:
        logger.error("kafka_connection_error", topic=topic, error=str(exc))
        await consumer.stop()
        return

    logger.info("ingest_consumer_started", topic=topic, group=group_id, message_concurrency=concurrency)
    message_semaphore = asyncio.Semaphore(concurrency)
    tracker = _OffsetTracker()
    pending: set[asyncio.Task[None]] = set()
    try:
        async for message in consumer:
            tracker.dispatched(TopicPartition(message.topic, message.partition), message.offset)
            task = asyncio.create_task(_process_message(message, message_semaphore, tracker, consumer))
            pending.add(task)
            task.add_done_callback(pending.discard)
            # A cap well above concurrency (not equal to it) - the
            # semaphore already limits how many run *concurrently*; this
            # just stops `pending` itself from growing unbounded if the
            # Kafka read loop can enqueue faster than tasks finish.
            if len(pending) >= concurrency * 4:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
    except KafkaError as exc:
        logger.error("kafka_connection_error", topic=topic, error=str(exc))
    finally:
        if pending:
            await asyncio.wait(pending)
        await consumer.stop()
        logger.info("ingest_consumer_stopped", topic=topic)


async def run() -> None:
    """Runs both topics' consumer loops concurrently in this one process.
    Same "cancel the survivor and re-raise" shape as spider-hub's own
    crawl_request_consumer.py run() (see that module's own docstring for
    the full rationale) - if either loop exits unexpectedly, the other is
    cancelled and the process exits non-zero so systemd's Restart=on-
    failure brings both back, rather than leaving one topic's ingestion
    silently stopped forever while the process still looks "up"."""
    loops = {
        asyncio.create_task(_run_topic_consumer(RAW_POSTS_TOPIC, CONSUMER_GROUP_POSTS, _POST_MESSAGE_CONCURRENCY), name="posts"),
        asyncio.create_task(
            _run_topic_consumer(RAW_COMMENTS_TOPIC, CONSUMER_GROUP_COMMENTS, _COMMENT_MESSAGE_CONCURRENCY), name="comments"
        ),
    }
    try:
        done, pending = await asyncio.wait(loops, return_when=asyncio.FIRST_COMPLETED)
    except (asyncio.CancelledError, KeyboardInterrupt):
        for task in loops:
            task.cancel()
        await asyncio.gather(*loops, return_exceptions=True)
        logger.info("ingest_consumer_all_stopped")
        return

    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    finished = next(iter(done))
    exc = finished.exception()
    if exc is not None:
        logger.error("ingest_consumer_loop_crashed", loop=finished.get_name(), error=str(exc))
        raise exc
    logger.error("ingest_consumer_loop_exited_unexpectedly", loop=finished.get_name())


if __name__ == "__main__":
    asyncio.run(run())
