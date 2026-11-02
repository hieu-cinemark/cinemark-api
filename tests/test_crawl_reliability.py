"""Lịch crawl, mốc Dừng và lease comment (review 02/11/2026): trạng thái chỉ đổi khi việc đã thực sự xong."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from app.clients import d1, kafka
from app.clients.redis import REDIS_KEY_PREFIX
from app.services.crawl import comment_planner as cp
from app.services.crawl import crawl_jobs, scheduler

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


class _Redis:
    def __init__(self):
        self.values = {}
        self.sets = {}
        self.lists = {}

    async def exists(self, key):
        return int(key in self.values)

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
            self.sets.pop(key, None)

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def sadd(self, key, item):
        self.sets.setdefault(key, set()).add(item)

    async def expire(self, key, ttl):
        pass

    async def lindex(self, key, index):
        items = self.lists.get(key, [])
        return items[index] if items else None

    async def lrem(self, key, count, raw):
        items = self.lists[key]
        items.reverse()
        items.remove(raw)
        items.reverse()


@pytest.fixture
def slots(monkeypatch):
    redis = _Redis()
    published = []
    results = {}
    monkeypatch.setattr(scheduler, "get_redis_client", lambda: redis)
    monkeypatch.setattr(scheduler, "_crawl_fired", set())
    monkeypatch.setattr(scheduler, "_crawl_running", set())

    async def publish(**kwargs):
        ok = results.get(kwargs["keyword_id"], True)
        if ok:
            published.append(kwargs)
        return ok

    async def keywords(platform):
        return [{"id": "k1", "keyword": "phim a"}, {"id": "k2", "keyword": "phim b"}]

    async def plan(platform, kws):
        return kws, {}

    async def mark(platform, today):
        pass

    monkeypatch.setattr(scheduler, "publish_crawl_request", publish)
    monkeypatch.setattr(scheduler, "get_enabled_keywords", keywords)
    monkeypatch.setattr(scheduler, "plan_keyword_crawl", plan)
    monkeypatch.setattr(scheduler.db, "mark_crawl_schedule_triggered", mark)
    return redis, published, results


def _slot():
    return asyncio.run(scheduler._run_crawl_slot("threads", "2026-11-02", "08:00"))


def _claim():
    return asyncio.run(scheduler._claim_crawl_slot("threads", "2026-11-02", "08:00"))


def test_slot_complete_is_never_refired(slots):
    _, published, _ = slots
    assert _claim() is True
    _slot()
    assert len(published) == 2
    assert _claim() is False
    scheduler._crawl_fired.clear()  # restart: cờ Redis vẫn chặn
    assert _claim() is False


def test_running_slot_is_not_claimed_twice(slots):
    assert _claim() is True
    assert _claim() is False


def test_failed_keyword_is_retried_alone_with_same_run_id(slots):
    redis, published, results = slots
    results["k2"] = False
    assert _claim() is True
    _slot()
    assert [p["keyword_id"] for p in published] == ["k1"]
    assert _claim() is False  # lease đóng vai khoảng nghỉ trước lần thử lại
    redis.values.pop(f"{scheduler._CRAWL_LEASE_PREFIX}threads:2026-11-02:08:00")
    results["k2"] = True
    assert _claim() is True
    _slot()
    assert [p["keyword_id"] for p in published] == ["k1", "k2"]
    assert _claim() is False
    assert published[1]["run_id"] == scheduler._SlotProgress("threads:2026-11-02:08:00").run_id("k2")


def test_d1_outage_while_loading_keywords_is_not_a_finished_slot(slots, monkeypatch):
    redis, _, _ = slots

    async def empty_on_failure(platform):
        # Như get_enabled_keywords thật: d1_query lỗi -> rows or [] - chỉ strict mới phân biệt được với "không có từ khoá".
        d1._unavailable("down")
        return []

    monkeypatch.setattr(scheduler, "get_enabled_keywords", empty_on_failure)
    assert _claim() is True
    _slot()
    assert not redis.values.get(f"{scheduler._CRAWL_FIRED_PREFIX}threads:2026-11-02:08:00")
    assert not scheduler._crawl_fired


def test_threads_hashtag_does_not_keep_slot_incomplete(slots, monkeypatch):
    redis, _, results = slots

    async def keywords(platform):
        return [{"id": "k1", "keyword": "#tag"}]

    results["k1"] = False  # publish thật luôn từ chối hashtag Threads
    monkeypatch.setattr(scheduler, "get_enabled_keywords", keywords)
    assert _claim() is True
    _slot()
    assert redis.values.get(f"{scheduler._CRAWL_FIRED_PREFIX}threads:2026-11-02:08:00")


def test_scheduler_publish_keeps_stop_but_manual_run_clears_it(monkeypatch):
    cleared = []

    class Producer:
        async def send_and_wait(self, topic, key, value):
            self.value = value

    async def clear(platform):
        cleared.append(platform)

    async def nothing(*args, **kwargs):
        return None

    producer = Producer()
    monkeypatch.setattr(kafka, "_producer", producer)
    monkeypatch.setattr(kafka, "clear_drain", clear)
    monkeypatch.setattr(kafka, "enqueue_published", nothing)

    async def no_terms(keyword_id):
        return []

    monkeypatch.setattr(kafka, "_match_terms_for_keyword", no_terms)
    assert asyncio.run(kafka.publish_crawl_request(platform="threads", keyword="k", keyword_id="1", run_id="fixed"))
    assert cleared == []
    assert producer.value["run_id"] == "fixed"
    assert producer.value["published_at"] > 0
    assert asyncio.run(kafka.publish_crawl_request(platform="threads", keyword="k", keyword_id="1", clear_stop=True))
    assert cleared == ["threads"]


def test_stop_cutoff_survives_clear_drain(monkeypatch):
    redis = _Redis()

    async def nothing(platform):
        return None

    async def no_jobs(platform):
        return []

    monkeypatch.setattr(crawl_jobs, "get_redis_client", lambda: redis)
    monkeypatch.setattr(crawl_jobs, "clear_pending", nothing)
    monkeypatch.setattr(crawl_jobs, "get_running_jobs", no_jobs)
    asyncio.run(crawl_jobs.request_stop("threads"))
    cutoff = f"{REDIS_KEY_PREFIX}platform_stop_cutoff:threads"
    assert float(redis.values[cutoff]) > 0
    asyncio.run(crawl_jobs.clear_drain("threads"))
    assert cutoff in redis.values
    assert f"{REDIS_KEY_PREFIX}platform_drain:threads" not in redis.values


def _row(**extra):
    return {"id": "p1", "reply_count": 10, "posted_at": (NOW - timedelta(hours=2)).isoformat(), **extra}


def test_queue_lease_blocks_only_while_fresh():
    assert not cp.queued_recently(_row(), NOW)
    assert cp.queued_recently(_row(comments_queued_at=(NOW - timedelta(hours=1)).isoformat()), NOW)
    assert not cp.queued_recently(_row(comments_queued_at=(NOW - cp.QUEUE_LEASE).isoformat()), NOW)


def test_publish_marks_queue_lease_not_crawled_at(monkeypatch):
    queries = []

    async def fake_d1(sql, params=None, **kwargs):
        queries.append((sql, params))
        return []

    monkeypatch.setattr(cp, "d1_query", fake_d1)
    pick = cp.CrawlPick("p1", "x1", "u", "tiktok", "m", 2, "hot", 7)
    assert asyncio.run(cp._mark_queued(pick, NOW, "r1")) is True
    assert "comments_queued_at" in queries[0][0] and "comments_crawled_at" not in queries[0][0]
    assert queries[0][1] == [NOW.isoformat(), 7, "r1", "hot", "p1"]


def _completions(monkeypatch, *items, d1_result=lambda sql: []):
    redis = _Redis()
    redis.lists[cp.COMMENT_COMPLETIONS_KEY] = [json.dumps(i) if isinstance(i, dict) else i for i in items]
    queries = []

    async def fake_d1(sql, params=None, **kwargs):
        queries.append((sql, params))
        return d1_result(sql)

    async def ready():
        return None

    monkeypatch.setattr(cp, "get_redis_client", lambda: redis)
    monkeypatch.setattr(cp, "d1_query", fake_d1)
    monkeypatch.setattr(cp, "ensure_post_voice_columns", ready)
    return redis, queries


@pytest.fixture
def posts_db(monkeypatch):
    """Bảng posts thật trên SQLite: các câu UPDATE của planner chạy đúng như trên D1."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE posts (id TEXT, platform TEXT, external_id TEXT, reply_count INT, comments_crawled_at TEXT, "
        "comments_crawled_replies INT, comments_crawl_reason TEXT, comments_queued_at TEXT, comments_queued_replies INT, "
        "comments_queued_run_id TEXT)"
    )
    conn.execute("INSERT INTO posts (id, platform, external_id, reply_count) VALUES ('p1', 'tiktok', 'x1', 9)")

    async def fake_d1(sql, params=None, **kwargs):
        return [dict(row) for row in conn.execute(sql, params or [])]

    monkeypatch.setattr(cp, "d1_query", fake_d1)
    return conn


def _row_of(conn):
    return dict(conn.execute("SELECT * FROM posts WHERE id = 'p1'").fetchone())


PICK = cp.CrawlPick("p1", "x1", "u", "tiktok", "m", 2, "hot", 7)


def _completion(run_id, status="done"):
    return {"platform": "tiktok", "post_id": "x1", "run_id": run_id, "status": status, "finished_at": 100}


def test_done_sets_crawled_at_and_releases_matching_lease(posts_db):
    asyncio.run(cp._mark_queued(PICK, NOW, "r1"))
    asyncio.run(cp._apply_completion(_completion("r1")))
    row = _row_of(posts_db)
    assert row["comments_crawled_at"] and row["comments_crawled_replies"] == 7
    assert row["comments_queued_at"] is None and row["comments_queued_run_id"] is None


def test_failed_releases_lease_without_crawled_at(posts_db):
    asyncio.run(cp._mark_queued(PICK, NOW, "r1"))
    asyncio.run(cp._apply_completion(_completion("r1", "skipped")))
    row = _row_of(posts_db)
    assert row["comments_crawled_at"] is None and row["comments_queued_at"] is None


def test_other_run_completion_keeps_the_lease(posts_db):
    asyncio.run(cp._mark_queued(PICK, NOW, "new"))
    asyncio.run(cp._apply_completion(_completion("old", "failed")))
    assert _row_of(posts_db)["comments_queued_run_id"] == "new"
    asyncio.run(cp._apply_completion(_completion("manual-run")))  # bấm tay: đã quét thật, nhưng lease không phải của nó
    row = _row_of(posts_db)
    assert row["comments_crawled_at"] and row["comments_queued_run_id"] == "new"
    assert row["comments_crawled_replies"] == 9


def _round(monkeypatch, publish):
    async def candidates(platform, window, now):
        return [
            {
                "id": "p1",
                "external_id": "x1",
                "url": "u",
                "platform": "tiktok",
                "movie_id": "m",
                "posted_at": datetime.now(tz=UTC).isoformat(),
                "reply_count": 7,
                "like_count": 1,
            }
        ]

    async def no_scores(*args):
        return {}

    monkeypatch.setattr(cp, "_candidates", candidates)
    monkeypatch.setattr(cp, "load_scores", no_scores)
    monkeypatch.setattr(cp, "judge_new_posts", no_scores)
    monkeypatch.setattr(cp, "publish_comments_crawl_request", publish)
    return asyncio.run(cp.run_comment_round("tiktok", hot_per_movie=5, with_sample=False))


def test_job_finishing_before_publish_returns_is_not_overwritten(posts_db, monkeypatch):
    """Job xong ngay khi publish chưa trả về: bài không được mang lại lease 24 giờ."""

    async def publish(**kwargs):
        await cp._apply_completion(_completion(kwargs["run_id"]))
        return True

    assert _round(monkeypatch, publish)["published"] == 1
    row = _row_of(posts_db)
    assert row["comments_crawled_at"] and row["comments_queued_at"] is None


def test_failed_publish_releases_the_lease(posts_db, monkeypatch):
    async def publish(**kwargs):
        return False

    assert _round(monkeypatch, publish)["published"] == 0
    assert _row_of(posts_db)["comments_queued_at"] is None


def test_no_lease_means_no_publish(monkeypatch):
    async def d1_down(sql, params=None, **kwargs):
        return None

    async def publish(**kwargs):
        raise AssertionError("không có lease thì không được publish")

    monkeypatch.setattr(cp, "d1_query", d1_down)
    assert _round(monkeypatch, publish)["published"] == 0


def test_auto_login_blocked_even_when_forced(monkeypatch):
    from app.core.config import settings
    from app.services.accounts import auto_login

    async def must_not_run():
        raise AssertionError("cổng cứng phải chặn trước khi đọc cấu hình")

    monkeypatch.setattr(settings, "auto_login_allowed", False)
    monkeypatch.setattr(auto_login, "resolve_auto_login_settings", must_not_run)
    assert asyncio.run(auto_login.run_auto_login_tick(triggered_by="manual", force=True)) == -1


def test_completion_stays_queued_when_d1_fails(monkeypatch):
    item = {"platform": "tiktok", "post_id": "x1", "status": "done", "finished_at": 100}
    redis, _ = _completions(monkeypatch, item, d1_result=lambda sql: None)
    assert asyncio.run(cp.apply_comment_completions()) == 0
    assert len(redis.lists[cp.COMMENT_COMPLETIONS_KEY]) == 1


def test_malformed_completion_is_discarded(monkeypatch):
    redis, queries = _completions(monkeypatch, "not json")
    assert asyncio.run(cp.apply_comment_completions()) == 0
    assert redis.lists[cp.COMMENT_COMPLETIONS_KEY] == [] and queries == []
