"""channel_profiles: chọn kênh theo phim cho cả TikTok/Threads, hàng đợi Threads qua Redis cho spider-hub, gom kết quả."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from app.services.crawl import channel_profiles as cp


class FakeRedis:
    def __init__(self) -> None:
        self.sets: dict[str, set[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    async def sadd(self, key: str, *members: str) -> int:
        before = len(self.sets.setdefault(key, set()))
        self.sets[key].update(members)
        return len(self.sets[key]) - before

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def hdel(self, key: str, *fields: str) -> int:
        for field in fields:
            self.hashes.get(key, {}).pop(field, None)
        return len(fields)


def _patch(monkeypatch, redis: FakeRedis, calls: list[tuple[str, list[Any]]], candidates: list[dict] | None = None):
    async def fake_d1(sql: str, params: list[Any] | None = None, **_kw: Any):
        calls.append((sql, params or []))
        if "FROM posts p" in sql:
            return candidates or []
        return []

    published: list[int] = []

    async def fake_publish(max_profiles: int) -> bool:
        published.append(max_profiles)
        return True

    async def fake_ensure() -> None:
        return None

    monkeypatch.setattr(cp, "d1_query", fake_d1)
    monkeypatch.setattr(cp, "get_redis_client", lambda: redis)
    monkeypatch.setattr(cp, "publish_threads_profiles_request", fake_publish)
    monkeypatch.setattr(cp, "_ensure_table_once", fake_ensure)
    return published


def test_candidates_group_by_movie_and_platform(monkeypatch) -> None:
    calls: list[tuple[str, list[Any]]] = []
    _patch(monkeypatch, FakeRedis(), calls)
    asyncio.run(cp._candidates("threads", 50))
    sql, params = calls[0]
    assert "GROUP BY p.movie_id, handle HAVING COUNT(*) >= ?3" in sql
    assert "followers_source" in sql
    assert params[0] == "threads" and params[2] == cp.CHANNEL_MIN_POSTS and params[4] == 50


def test_enqueue_threads_adds_handles_and_publishes(monkeypatch) -> None:
    redis = FakeRedis()
    calls: list[tuple[str, list[Any]]] = []
    published = _patch(monkeypatch, redis, calls, [{"handle": "a", "posts": 5}, {"handle": "b", "posts": 2}])
    result = asyncio.run(cp.enqueue_threads_profiles(True, 200))
    assert result == {"pending": 0, "added": 2, "published": 1}
    assert redis.sets[cp.THREADS_QUEUE_KEY] == {"a", "b"}
    assert published == [200]


def test_enqueue_threads_batch_without_fill_only_retriggers(monkeypatch) -> None:
    redis = FakeRedis()
    redis.sets[cp.THREADS_QUEUE_KEY] = {"left"}
    calls: list[tuple[str, list[Any]]] = []
    published = _patch(monkeypatch, redis, calls, [{"handle": "new", "posts": 9}])
    result = asyncio.run(cp.enqueue_threads_profiles(False, 200))
    assert result["added"] == 0 and result["pending"] == 1 and published == [200]
    assert not calls  # không chọn kênh mới
    assert redis.sets[cp.THREADS_QUEUE_KEY] == {"left"}


def test_enqueue_threads_nothing_to_do_does_not_publish(monkeypatch) -> None:
    redis = FakeRedis()
    published = _patch(monkeypatch, redis, [], [])
    assert asyncio.run(cp.enqueue_threads_profiles(True, 200))["published"] == 0
    assert published == []


def test_enqueue_threads_daily_fill_adds_to_pending_queue(monkeypatch) -> None:
    redis = FakeRedis()
    redis.sets[cp.THREADS_QUEUE_KEY] = {"left"}
    calls: list[tuple[str, list[Any]]] = []
    published = _patch(monkeypatch, redis, calls, [{"handle": "new", "posts": 9}, {"handle": "left", "posts": 3}])
    result = asyncio.run(cp.enqueue_threads_profiles(True, 200))
    assert result["added"] == 1 and published == [200]
    assert calls[0][1][4] == cp.THREADS_FILL_LIMIT
    assert redis.sets[cp.THREADS_QUEUE_KEY] == {"left", "new"}


def test_collect_threads_results_upserts_and_clears(monkeypatch) -> None:
    redis = FakeRedis()
    redis.hashes[cp.THREADS_RESULTS_KEY] = {
        "_ph.hp04_": json.dumps(
            {"handle": "_ph.hp04_", "found": True, "full_name": "Hoàng Phúc", "follower_count": 80, "fetched_at": "x"}
        ),
        "ghost": json.dumps({"handle": "ghost", "found": False, "fetched_at": "x"}),
    }
    calls: list[tuple[str, list[Any]]] = []
    _patch(monkeypatch, redis, calls)
    assert asyncio.run(cp.collect_threads_results()) == {"collected": 2, "found": 1}
    sql, params = calls[0]
    assert "INSERT INTO channel_profiles" in sql and "!= 'manual'" in sql
    assert params[2] == "threads"
    rows = {row["handle"]: row for row in json.loads(params[0])}
    assert rows["_ph.hp04_"] == {"handle": "_ph.hp04_", "followers": 80, "name": "Hoàng Phúc"}
    assert rows["ghost"] == {"handle": "ghost", "followers": None, "name": None}
    assert redis.hashes[cp.THREADS_RESULTS_KEY] == {}


def test_collect_threads_results_empty_is_noop(monkeypatch) -> None:
    calls: list[tuple[str, list[Any]]] = []
    _patch(monkeypatch, FakeRedis(), calls)
    assert asyncio.run(cp.collect_threads_results()) == {"collected": 0}
    assert not calls

