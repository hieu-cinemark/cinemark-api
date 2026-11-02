"""Ingest (review 02/11/2026): offset chỉ được commit khi message đã có kết cục - xử lý xong hoặc đã nằm trong dead letter."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from aiokafka.errors import KafkaError

from app.clients import kafka
from app.clients.d1 import D1Unavailable, d1_strict
from app.workers.ingest_consumer import main


class _Consumer:
    def __init__(self):
        self.commits = []

    async def commit(self, offsets):
        self.commits.append(offsets)


def _comment_message(offset=17):
    return SimpleNamespace(
        topic=main.RAW_COMMENTS_TOPIC,
        partition=0,
        offset=offset,
        value={"platform": "tiktok", "post_id": "p1", "comment_id": "c1"},
    )


@pytest.fixture
def ingest(monkeypatch):
    """Không ngủ thật, dead letter vào danh sách, không đụng Redis/Kafka."""
    env = SimpleNamespace(dead_letters=[], publish_results=[], drops=[])

    async def publish(key, record):
        if env.publish_results and not env.publish_results.pop(0):
            return False
        env.dead_letters.append(record)
        return True

    async def note_drop(platform, reason, **context):
        env.drops.append(reason)

    async def drop(**kwargs):
        env.drops.append(kwargs["reason"])

    monkeypatch.setattr(main, "publish_dead_letter", publish)
    monkeypatch.setattr(main, "_note_drop", note_drop)
    monkeypatch.setattr(main, "_drop", drop)
    monkeypatch.setattr(main, "_INFRA_RETRY_DELAYS", (0, 0))
    monkeypatch.setattr(main, "_PARENT_RETRY_DELAYS", (0, 0))
    monkeypatch.setattr(main, "_UNEXPECTED_RETRY_DELAYS", (0,))
    monkeypatch.setattr(main, "_DEAD_LETTER_RETRY_SECONDS", 0)
    monkeypatch.setattr(main, "_missing_parents", {})
    monkeypatch.setattr(main, "get_comment_mapper", lambda platform: lambda payload: {"external_id": "c1"})
    return env


def _process(message):
    tracker = main._OffsetTracker()
    tp = main.TopicPartition(message.topic, message.partition)
    tracker.dispatched(tp, message.offset)
    consumer = _Consumer()

    async def run():
        await main._process_message(message, asyncio.Semaphore(1), tracker, consumer)

    asyncio.run(run())
    return consumer


def _parent(monkeypatch, *results):
    calls = []
    queue = list(results)

    async def get_post(platform, post_id):
        calls.append(post_id)
        value = queue.pop(0) if queue else results[-1]
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(main, "get_post_by_external_id", get_post)
    return calls


def _persist(monkeypatch, *results):
    calls = []
    queue = list(results)

    async def persist(**kwargs):
        calls.append(kwargs)
        return queue.pop(0) if queue else results[-1]

    monkeypatch.setattr(main, "persist_comment", persist)
    return calls


def test_persist_failure_is_retried_before_commit(ingest, monkeypatch):
    _parent(monkeypatch, {"id": "post_1"})
    calls = _persist(monkeypatch, False, True)
    consumer = _process(_comment_message())
    assert len(calls) == 2
    assert ingest.dead_letters == []
    assert len(consumer.commits) == 1


def test_persistent_write_failure_goes_to_dead_letter_then_commits(ingest, monkeypatch):
    _parent(monkeypatch, {"id": "post_1"})
    calls = _persist(monkeypatch, False)
    consumer = _process(_comment_message())
    assert len(calls) == 3
    record = ingest.dead_letters[0]
    assert (record["reason"], record["offset"], record["attempts"]) == ("d1_write_failed", 17, 3)
    assert record["payload"]["post_id"] == "p1"
    assert len(consumer.commits) == 1


def test_comment_before_parent_waits_for_parent(ingest, monkeypatch):
    calls = _parent(monkeypatch, None, None, {"id": "post_1"})
    persisted = _persist(monkeypatch, True)
    consumer = _process(_comment_message())
    assert len(calls) == 3 and len(persisted) == 1
    assert ingest.dead_letters == [] and len(consumer.commits) == 1


def test_missing_parent_memo_skips_the_wait_for_siblings(ingest, monkeypatch):
    calls = _parent(monkeypatch, None)
    _process(_comment_message())
    assert len(calls) == 3
    assert ingest.dead_letters[0]["reason"] == "comment_parent_missing"
    calls.clear()
    _process(_comment_message(offset=18))
    assert len(calls) == 1  # comment khác của cùng bài: không chờ lại
    assert len(ingest.dead_letters) == 2


def test_d1_outage_is_not_mistaken_for_missing_parent(ingest, monkeypatch):
    _parent(monkeypatch, D1Unavailable("d1_http_503"))
    _process(_comment_message())
    assert ingest.dead_letters[0]["reason"] == "d1_write_failed"
    assert not main._missing_parents


def test_dead_letter_publish_failure_blocks_commit_until_delivered(ingest, monkeypatch):
    _parent(monkeypatch, None)
    ingest.publish_results = [False, False, True]
    consumer = _process(_comment_message())
    assert ingest.publish_results == []
    assert len(ingest.dead_letters) == 1 and len(consumer.commits) == 1


def test_cancel_during_retry_does_not_commit(ingest, monkeypatch):
    _parent(monkeypatch, {"id": "post_1"})
    _persist(monkeypatch, False)
    monkeypatch.setattr(main, "_INFRA_RETRY_DELAYS", (60,))
    tracker = main._OffsetTracker()
    message = _comment_message()
    tp = main.TopicPartition(message.topic, message.partition)
    tracker.dispatched(tp, message.offset)
    consumer = _Consumer()

    async def run():
        task = asyncio.create_task(main._process_message(message, asyncio.Semaphore(1), tracker, consumer))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert consumer.commits == [] and ingest.dead_letters == []


def test_poison_message_does_not_block_forever(ingest, monkeypatch):
    message = SimpleNamespace(topic=main.RAW_COMMENTS_TOPIC, partition=0, offset=1, value=["không phải dict"])
    consumer = _process(message)
    assert ingest.dead_letters[0]["reason"] == "handler_error"
    assert len(consumer.commits) == 1


def test_comment_without_post_id_is_rejected_without_retry(ingest, monkeypatch):
    message = SimpleNamespace(topic=main.RAW_COMMENTS_TOPIC, partition=0, offset=1, value={"platform": "tiktok"})
    consumer = _process(message)
    assert ingest.dead_letters == [] and len(consumer.commits) == 1


def test_tracked_movies_outage_raises_without_cache_and_uses_stale_cache(monkeypatch):
    async def down(sql, params=None, **kwargs):
        raise D1Unavailable("down")

    monkeypatch.setattr(main, "d1_query", down)
    monkeypatch.setattr(main, "_tracked_movies_cache", None)
    with pytest.raises(D1Unavailable):
        asyncio.run(main._tracked_movies())
    monkeypatch.setattr(main, "_tracked_movies_cache", (-1e9, {"m": {"title": "Phim"}}))
    assert asyncio.run(main._tracked_movies()) == {"m": {"title": "Phim"}}


def test_strict_scope_is_per_context():
    from app.clients import d1

    assert d1._strict.get() is False
    with d1_strict():
        assert d1._strict.get() is True
    assert d1._strict.get() is False


class _Producer:
    def __init__(self, *errors):
        self.errors = list(errors)
        self.sent = []

    async def send_and_wait(self, topic, key, value):
        if self.errors:
            raise self.errors.pop(0)
        self.sent.append((topic, key, value))


def test_publish_dead_letter_reports_kafka_failure(monkeypatch):
    monkeypatch.setattr(kafka, "_producer", _Producer(KafkaError()))
    assert asyncio.run(kafka.publish_dead_letter("k", {"payload": {}})) is False


def test_publish_dead_letter_drops_oversized_payload_but_keeps_coordinates(monkeypatch):
    producer = _Producer(kafka.MessageSizeTooLargeError())
    monkeypatch.setattr(kafka, "_producer", producer)
    assert asyncio.run(kafka.publish_dead_letter("k", {"offset": 5, "payload": {"big": "x"}})) is True
    assert producer.sent[0][2] == {"offset": 5, "payload": None, "payload_dropped": True}
