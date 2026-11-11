import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from app.repositories.d1 import sql as d1_sql
from app.services.crawl import watch_pages

NOON_VN = datetime(2026, 11, 5, 5, 0, tzinfo=UTC)
MIDNIGHT_VN = datetime(2026, 11, 5, 17, 30, tzinfo=UTC)


def test_pick_pages_prefers_showing_film_pages_then_active_shared_pages():
    rows = [
        {"id": "shared-quiet", "movie_id": None, "quiet_runs": 3},
        {"id": "other-film", "movie_id": "old", "quiet_runs": 0},
        {"id": "shared-active", "movie_id": None, "quiet_runs": 0},
        {"id": "film", "movie_id": "now", "quiet_runs": 5},
    ]
    picked = watch_pages.pick_pages(rows, {"now"}, 2)
    assert [row["id"] for row in picked] == ["film", "shared-active"]


class Redis:
    def __init__(self, pending=0):
        self.pending = pending
        self.pushed = []

    async def llen(self, key):
        return self.pending

    async def lpush(self, key, value):
        self.pushed.append((key, json.loads(value)))


def _install(monkeypatch, pending=0, showing=("now",)):
    redis = Redis(pending)
    monkeypatch.setattr(watch_pages, "get_redis_client", lambda: redis)
    monkeypatch.setattr(watch_pages, "enqueue_published", AsyncMock())
    monkeypatch.setattr(watch_pages, "get_or_create_keyword", AsyncMock(return_value={"id": "kw"}))
    monkeypatch.setattr(watch_pages, "showing_movie_ids", AsyncMock(return_value=set(showing)))

    async def query(sql, params=None, **_):
        if "FROM movies" in sql:
            return [{"title": "Phim"}]
        return [{"id": "film", "movie_id": "now", "url": "https://www.facebook.com/phim"}]

    monkeypatch.setattr(d1_sql, "d1_query", query)
    return redis


def test_round_queues_logged_out_page_reads_that_resend_seen_posts(monkeypatch):
    redis = _install(monkeypatch)
    monkeypatch.setattr(watch_pages.settings, "facebook_watch_pages_max_pages", 1)
    assert asyncio.run(watch_pages.queue_round(NOON_VN)) == 1
    key, request = redis.pushed[0]
    assert key == watch_pages.PUBLIC_QUEUE_KEY
    assert request["type"] == "public_page" and request["page_url"] == "https://www.facebook.com/phim"
    assert request["max_pages"] == 1 and request["dedupe"] == "false" and request["keyword_id"] == "kw"


def test_round_runs_at_night_without_accounts(monkeypatch):
    redis = _install(monkeypatch)
    assert asyncio.run(watch_pages.queue_round(MIDNIGHT_VN)) == 1
    assert redis.pushed


def test_round_skips_when_public_page_jobs_are_backing_up(monkeypatch):
    redis = _install(monkeypatch, pending=watch_pages.MAX_PENDING_PUBLIC_PAGES)
    assert asyncio.run(watch_pages.queue_round(NOON_VN)) == 0
    assert not redis.pushed


def test_round_queues_nothing_when_no_film_is_showing(monkeypatch):
    redis = _install(monkeypatch, showing=())
    assert asyncio.run(watch_pages.queue_round(NOON_VN)) == 0
    assert not redis.pushed
