"""Kênh Threads trong source_pages: chuẩn hoá đường dẫn, lịch giãn cách, nạp hàng đợi spider-hub."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from app.core.errors import ValidationError
from app.services.crawl import source_pages, threads_channels

NOW = datetime(2026, 11, 13, 12, tzinfo=UTC)


def test_threads_url_normalised():
    assert source_pages.is_threads_url("https://www.threads.net/@Kenh.Phim/post/ABC?x=1")
    assert source_pages.normalize_threads_url("threads.com/@Kenh.Phim/post/ABC") == "https://www.threads.com/@kenh.phim"
    assert not source_pages.is_threads_url("https://www.facebook.com/kenh")
    with pytest.raises(ValidationError):
        source_pages.normalize_threads_url("https://www.threads.com/kenh")


def test_handle_from_post_url():
    assert threads_channels.handle_from_post_url("https://www.threads.com/@Ab.c/post/XYZ") == "ab.c"
    assert threads_channels.handle_from_post_url("https://www.threads.com/@abc") is None


def test_quiet_channels_back_off():
    film = {"movie_id": "m", "last_queued_at": (NOW - timedelta(hours=7)).isoformat(), "quiet_runs": 0}
    shared = {**film, "movie_id": None}
    assert threads_channels._due(film, NOW)
    assert not threads_channels._due(shared, NOW)  # kênh chung: gấp đôi
    assert not threads_channels._due({**film, "quiet_runs": 1}, NOW)
    assert threads_channels._due({"last_queued_at": None}, NOW)


class FakeRedis:
    def __init__(self, queued=0, last=None):
        self.list = ["x"] * queued
        self.last = last or {}

    async def llen(self, key):
        return len(self.list)

    async def lpush(self, key, value):
        self.list.insert(0, value)

    async def get(self, key):
        url = key.split("threads_channel_last:", 1)[1]
        return json.dumps(self.last[url]) if url in self.last else None


@pytest.fixture
def world(monkeypatch):
    redis = FakeRedis(
        last={
            "https://www.threads.com/@b": {
                "status": "ok",
                "stop_reason": "since",
                "at": NOW.timestamp() - 86400,
                "new": 3,
            }
        }
    )
    rows = [
        {"id": "a", "url": "https://www.threads.com/@a", "movie_id": None, "last_queued_at": None, "quiet_runs": 0},
        {"id": "b", "url": "https://www.threads.com/@b", "movie_id": None, "last_queued_at": None, "quiet_runs": 2},
        {
            "id": "c",
            "url": "https://www.threads.com/@c",
            "movie_id": None,
            "last_queued_at": NOW.isoformat(),
            "quiet_runs": 0,
        },
    ]
    updates: list[tuple] = []

    async def fake_exec(stmt, **_):
        return [dict(r) for r in rows]

    async def fake_set(source_id, *, quiet=False, **values):
        updates.append((source_id, values))

    async def noop(*a, **k):
        return None

    monkeypatch.setattr(threads_channels, "get_redis_client", lambda: redis)
    monkeypatch.setattr(threads_channels, "d1_exec", fake_exec)
    monkeypatch.setattr(threads_channels, "_set_source", fake_set)
    monkeypatch.setattr(threads_channels, "_ensure_table", noop)
    monkeypatch.setattr(threads_channels, "enqueue_published", noop)
    monkeypatch.setattr(threads_channels.settings, "threads_channels_enabled", True)

    async def target(name):
        return {"threads_channels_queue_target": 4}[name]

    monkeypatch.setattr(threads_channels, "crawl_setting", target)
    return redis, updates


def test_fill_queues_due_channels_with_narrow_window(world):
    redis, updates = world
    assert asyncio.run(threads_channels.fill_queue(NOW)) == 2
    requests = [json.loads(raw) for raw in redis.list]
    by_url = {r["page_url"]: r for r in requests}
    assert set(by_url) == {"https://www.threads.com/@a", "https://www.threads.com/@b"}
    assert by_url["https://www.threads.com/@a"]["since_days"] == threads_channels.SINCE_DAYS
    assert by_url["https://www.threads.com/@b"]["since_days"] == 3  # lần trước xong 1 ngày trước + 2
    assert all(r["type"] == "threads_channel" and r["keyword_id"] is None for r in requests)
    # Lần đọc trước của @b có bài mới -> quiet_runs về 0.
    assert ("b", {"quiet_runs": 0}) in updates


def test_full_queue_adds_nothing(world, monkeypatch):
    redis, _ = world
    redis.list = ["x"] * 4
    assert asyncio.run(threads_channels.fill_queue(NOW)) == 0
