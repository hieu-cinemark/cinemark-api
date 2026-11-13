"""Xếp page không đăng nhập: nạp đủ hàng đợi khi vơi, tôn trọng tạm dừng / khoảng cách tối thiểu, và comment Facebook không còn phụ thuộc
công tắc lịch cào."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from app.repositories.d1 import sql as d1_sql
from app.services.crawl import scheduler
from app.services.crawl import source_pages as sp

NOW = datetime(2026, 11, 3, 8, 0, tzinfo=UTC)


class Redis:
    def __init__(self, waiting=0, paused=False, last=None):
        self.pushed = []
        self.waiting = waiting
        self.paused = paused
        self.last = last or {}

    async def exists(self, key):
        return self.paused

    async def llen(self, key):
        return self.waiting + len(self.pushed)

    async def lpush(self, key, value):
        self.pushed.append((key, json.loads(value)))

    async def get(self, key):
        return json.dumps(self.last[key]) if key in self.last else None


ROWS = [
    {"id": "shared", "url": "https://www.facebook.com/shared", "movie_id": None},
    {"id": "film", "url": "https://www.facebook.com/film", "movie_id": "m1"},
    {
        "id": "recent",
        "url": "https://www.facebook.com/recent",
        "last_queued_at": (NOW - timedelta(minutes=5)).isoformat(),
    },
]


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setattr(sp, "_public_plan", [])
    monkeypatch.setattr(sp, "_public_plan_at", None)
    monkeypatch.setattr(sp, "_ensure_table", AsyncMock())
    monkeypatch.setattr(sp, "_update_quiet_runs", AsyncMock())
    monkeypatch.setattr(sp, "enqueue_published", AsyncMock())
    monkeypatch.setattr(sp, "_due", lambda row, now: row["id"] != "recent")
    monkeypatch.setattr(sp.settings, "facebook_public_pages_enabled", True)
    monkeypatch.setattr(sp.settings, "facebook_public_pages_queue_target", 2)
    monkeypatch.setattr(sp.settings, "facebook_public_pages_min_gap_minutes", 60)
    calls = []

    async def d1(sql, params=None, **kwargs):
        calls.append(sql)
        if sql.startswith("SELECT") and "FROM source_pages" in sql:
            return [dict(row) for row in ROWS]
        if sql.startswith("SELECT movies.title"):
            return [{"title": "Phim"}]
        return []

    monkeypatch.setattr(sp, "d1_query", d1)
    monkeypatch.setattr(d1_sql, "d1_query", d1)
    monkeypatch.setattr(sp, "get_or_create_keyword", AsyncMock(return_value={"id": "kw"}))
    redis = Redis()
    monkeypatch.setattr(sp, "get_redis_client", lambda: redis)
    return redis, calls


def test_fills_queue_to_target_film_first_and_skips_recent_page(env):
    redis, calls = env
    assert asyncio.run(sp.fill_public_pages_queue(NOW)) == 2
    assert all(key.endswith("crawl_public_pages:facebook") for key, _ in redis.pushed)
    film, shared = (request for _, request in redis.pushed)
    assert film["type"] == "public_page" and film["page_url"].endswith("/film") and film["keyword_id"] == "kw"
    assert shared["page_url"].endswith("/shared") and shared["keyword_id"] is None
    assert sum(sql.startswith("UPDATE source_pages") for sql in calls) == 2
    assert asyncio.run(sp.fill_public_pages_queue(NOW)) == 0  # đã đủ target


def test_refills_as_soon_as_queue_has_room(env):
    redis, _ = env
    asyncio.run(sp.fill_public_pages_queue(NOW))
    redis.pushed.pop()
    assert asyncio.run(sp.fill_public_pages_queue(NOW)) == 0  # hết page trong plan, chưa tới lượt đọc lại D1
    assert asyncio.run(sp.fill_public_pages_queue(NOW + timedelta(minutes=11))) == 1


def test_page_past_min_gap_is_requeued_even_when_not_due(env, monkeypatch):
    redis, _ = env
    monkeypatch.setattr(sp, "_due", lambda row, now: False)
    assert asyncio.run(sp.fill_public_pages_queue(NOW)) == 2
    assert {request["page_url"].rsplit("/", 1)[1] for _, request in redis.pushed} == {"film", "shared"}


@pytest.mark.parametrize("redis", [Redis(waiting=2), Redis(paused=True)])
def test_full_queue_or_pause_does_not_mark_page_as_read(env, monkeypatch, redis):
    _, calls = env
    monkeypatch.setattr(sp, "get_redis_client", lambda: redis)
    assert asyncio.run(sp.fill_public_pages_queue(NOW)) == 0
    assert redis.pushed == [] and calls == []


def test_disabled_setting_queues_nothing(env, monkeypatch):
    redis, _ = env
    monkeypatch.setattr(sp.settings, "facebook_public_pages_enabled", False)
    assert asyncio.run(sp.fill_public_pages_queue(NOW)) == 0
    assert redis.pushed == []


def test_plan_is_not_reread_while_nothing_is_ready(env, monkeypatch):
    _, calls = env

    async def empty(sql, params=None, **kwargs):
        calls.append(sql)
        return []

    monkeypatch.setattr(sp, "d1_query", empty)
    monkeypatch.setattr(d1_sql, "d1_query", empty)
    asyncio.run(sp.fill_public_pages_queue(NOW))
    asyncio.run(sp.fill_public_pages_queue(NOW + timedelta(minutes=1)))
    assert len(calls) == 1


@pytest.mark.parametrize(
    "last,expected",
    [
        ({"stop_reason": "exhausted", "at": NOW.timestamp() - 3600}, 2),
        ({"stop_reason": "since_days", "at": NOW.timestamp() - 3 * 86400}, 5),
        ({"stop_reason": "exhausted", "at": NOW.timestamp() - 200 * 86400}, sp.SINCE_DAYS),
        ({"stop_reason": "exhausted", "at": NOW.timestamp(), "pagination_status": "login_redirect"}, None),
        ({"stop_reason": "cancelled", "at": NOW.timestamp()}, None),
        ({"new": 1, "at": NOW.timestamp()}, None),  # lần đọc của spider page_feed có tài khoản
        ({}, None),
    ],
)
def test_since_days_only_shrinks_after_a_complete_read(monkeypatch, last, expected):
    url = "https://www.facebook.com/x"
    redis = Redis(last={f"{sp.REDIS_KEY_PREFIX}page_feed_last:{url}": last} if last else {})
    monkeypatch.setattr(sp, "get_redis_client", lambda: redis)
    assert asyncio.run(sp._public_since_days(url, NOW)) == expected


def test_facebook_comment_round_ignores_crawl_schedule_switch(monkeypatch):
    monkeypatch.setattr(scheduler, "_comment_round_done", {})
    monkeypatch.setattr(
        scheduler.db,
        "list_comment_crawl_schedules",
        AsyncMock(return_value=[{"platform": "facebook", "enabled": True, "top_n": 15, "run_time": "08:00"}]),
    )
    monkeypatch.setattr(scheduler, "_facebook_crawl_enabled", AsyncMock(return_value=False))
    spawned = []
    monkeypatch.setattr(scheduler, "_spawn", lambda coro: (spawned.append(coro), coro.close()))
    monkeypatch.setattr(scheduler, "datetime", _At)
    asyncio.run(scheduler._comments_tick())
    assert len(spawned) == 1


class _At(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 11, 3, 15, 10, tzinfo=tz)


def test_queued_page_is_listed_on_the_queue_page(env):
    asyncio.run(sp.fill_public_pages_queue(NOW))
    listed = sp.enqueue_published.await_args_list
    assert [call.args[0]["page_url"].rsplit("/", 1)[1] for call in listed] == ["film", "shared"]
    assert listed[0].args[0]["published_at"] == NOW.timestamp()


def test_queue_label_and_search_first_ordering(monkeypatch):
    from app.services.crawl import crawl_jobs, task_queue

    assert task_queue.task_label({"type": "public_page", "page_url": "https://www.facebook.com/x"}).endswith("/x")
    pending = {
        "facebook": [
            {"id": "p", "type": "public_page"},
            {"id": "s1", "type": "search"},
            {"id": "c", "type": "comments"},
        ],
        "threads": [{"id": "s2", "type": "search"}],
        "tiktok": [],
        "youtube": [],
    }
    monkeypatch.setattr(task_queue, "list_pending", AsyncMock(side_effect=lambda platform: pending[platform]))
    monkeypatch.setattr(task_queue, "list_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(crawl_jobs, "get_running_jobs", AsyncMock(return_value=[]))
    monkeypatch.setattr(crawl_jobs, "is_platform_draining", AsyncMock(return_value=False))
    assert [item["id"] for item in asyncio.run(task_queue.snapshot())["queued"]] == ["s1", "s2", "p", "c"]


def test_only_facebook_scans_for_running_keys_and_uses_a_large_count():
    from app.services.crawl import crawl_jobs

    calls = []

    class Client:
        def scan_iter(self, match, count):
            calls.append((match, count))

            async def keys():
                for key in ("b:lane1", "a:public_page:15"):
                    yield key

            return keys()

    assert len(asyncio.run(crawl_jobs._running_keys(Client(), "tiktok"))) == 1
    assert calls == []
    keys = asyncio.run(crawl_jobs._running_keys(Client(), "facebook"))
    assert keys[1:] == ["a:public_page:15", "b:lane1"]
    assert len(calls) == 1 and calls[0][1] >= 1000
