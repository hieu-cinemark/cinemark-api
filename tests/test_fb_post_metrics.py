"""Cập nhật số bài Facebook không đăng nhập (job post_metrics của spider-hub): tracked_posts gửi job đúng dạng, ingest chỉ
cập nhật số của bài đã có, giữ số cũ khi trang không có (reel không có share), ghi snapshot khi số đổi."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

from app.core.config import settings
from app.repositories.d1 import posts as posts_repo
from app.services.crawl import tracked_posts
from app.workers.ingest_consumer import main as ingest


async def test_metrics_message_updates_engagement() -> None:
    update = AsyncMock(return_value=True)
    views = AsyncMock()
    with patch.object(ingest, "update_post_engagement", update), patch.object(ingest, "update_post_views", views):
        await ingest.handle_post(
            {
                "platform": "facebook",
                "kind": "metrics",
                "source": "logged_out",
                "post_id": "123",
                "reactions_count": 80,
                "comments_count": 4,
            }
        )
    update.assert_awaited_once_with("facebook", "123", like_count=80, reply_count=4, repost_count=None)
    views.assert_not_awaited()


async def test_view_only_metrics_message_does_not_touch_engagement() -> None:
    update = AsyncMock()
    with patch.object(ingest, "update_post_engagement", update), patch.object(ingest, "update_post_views", AsyncMock()):
        await ingest.handle_post({"platform": "facebook", "kind": "metrics", "post_id": "123", "view_count": 6500})
    update.assert_not_awaited()


class _FakeD1:
    def __init__(self, existing: dict | None):
        self.existing = existing
        self.calls: list[tuple[str, list]] = []

    async def __call__(self, sql: str, params=None, **_):
        self.calls.append((sql, params or []))
        if sql.startswith("SELECT"):
            return [self.existing] if self.existing else []
        return []


def _existing(**counts):
    base = {
        "like_count": 10,
        "reply_count": 2,
        "repost_count": 5,
        "quote_count": 0,
        "reshare_count": 0,
        "view_count": 900,
    }
    return {"id": "p1", "keyword_id": "k1", **base, **counts}


async def _update(fake: _FakeD1, **counts) -> bool:
    with (
        patch.object(posts_repo, "d1_query", fake),
        patch.object(posts_repo, "_ensure_post_indexes", AsyncMock()),
        patch("app.services.reports.stats_summary.record_post", AsyncMock()),
    ):
        return await posts_repo.update_post_engagement("facebook", "e1", **counts)


async def test_engagement_update_keeps_missing_counts_and_snapshots_change() -> None:
    fake = _FakeD1(_existing())
    assert await _update(fake, like_count=12, reply_count=None, repost_count=None)
    update_sql, update_params = fake.calls[1]
    assert update_sql.startswith("UPDATE posts SET like_count")
    assert update_params[:3] == [12, 2, 5]  # reply/repost không có -> giữ số cũ
    snapshot_sql, snapshot_params = fake.calls[2]
    assert "post_engagement_snapshots" in snapshot_sql
    assert snapshot_params[3:] == [12, 2, 5, 0, 0, 900]  # view_count giữ nguyên


async def test_engagement_unchanged_writes_no_snapshot_and_unknown_post_is_ignored() -> None:
    fake = _FakeD1(_existing())
    assert await _update(fake, like_count=10, reply_count=2, repost_count=5)
    assert not any("post_engagement_snapshots" in sql for sql, _ in fake.calls)

    fake = _FakeD1(None)
    assert not await _update(fake, like_count=1)
    assert len(fake.calls) == 1  # chỉ SELECT


class _FakeRedis:
    def __init__(self):
        self.pushed: list[dict] = []

    async def lpush(self, key, value):
        self.pushed.insert(0, {**json.loads(value), "_queue": key.rsplit(":", 1)[0].rsplit(":", 1)[-1]})


async def _queue(rows, matched, *, logged_out=True):
    redis = _FakeRedis()
    stats: dict = {}

    async def fake_d1(sql, params=None, **_):
        return [{"external_id": e, "url": matched[e]} for e in params or [] if e in matched]

    with (
        patch.object(tracked_posts, "d1_query", fake_d1),
        patch.object(tracked_posts, "get_redis_client", lambda: redis),
        patch.object(settings, "tracked_facebook_logged_out", logged_out),
    ):
        await tracked_posts._queue_facebook(rows, stats)
    return list(reversed(redis.pushed)), stats  # thứ tự spider-hub lấy ra (RPOP)


ROWS = [
    {
        "url": "https://www.facebook.com/a23/posts/pfbid0abc",
        "external_id": "e1",
        "page": "A23",
        "posted_date": "2026-10-20",
    },
    {"url": "https://fb.watch/xyz", "external_id": "e2", "page": "A23"},  # reel, không có slug từ link
    {"url": "https://www.facebook.com/galaxy/posts/pfbid0new", "external_id": None, "posted_date": "2026-10-22"},
]
MATCHED = {"e1": "https://www.facebook.com/a23/posts/pfbid0abc", "e2": "https://www.facebook.com/reel/988965410142997/"}


async def test_known_posts_go_to_post_metrics_with_page_fallback() -> None:
    pushed, stats = await _queue(ROWS, MATCHED)
    # post_metrics vào hàng đợi rảnh (không giữ tài khoản), page_feed vào làn ưu tiên
    assert [(r["type"], r["_queue"]) for r in pushed] == [
        ("post_metrics", "crawl_idle"),
        ("page_feed", "crawl_priority"),
    ]
    metrics = pushed[0]
    assert [(p["external_id"], p["url"], p["page_url"]) for p in metrics["posts"]] == [
        ("e1", MATCHED["e1"], "https://www.facebook.com/a23"),
        ("e2", MATCHED["e2"], "https://www.facebook.com/a23"),  # slug lấy qua tên page của dòng khác
    ]
    assert list(metrics["fallback_page_feeds"]) == ["https://www.facebook.com/a23"]
    assert metrics["fallback_page_feeds"]["https://www.facebook.com/a23"]["type"] == "page_feed"
    assert pushed[1]["page_url"] == "https://www.facebook.com/galaxy"  # bài chưa có trong D1 -> page_feed ngay
    assert stats == {"facebook_metrics_posts": 2, "facebook_pages_queued": 1}


async def test_switch_off_falls_back_to_page_feed_only() -> None:
    pushed, _ = await _queue(ROWS, MATCHED, logged_out=False)
    assert sorted(r["page_url"] for r in pushed) == ["https://www.facebook.com/a23", "https://www.facebook.com/galaxy"]
    assert all(r["type"] == "page_feed" for r in pushed)
