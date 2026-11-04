import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.crawl import scheduler, tracked_posts


@pytest.mark.parametrize(
    "background,enabled,expected",
    [(False, True, False), (False, False, False), (True, False, False), (True, True, True)],
)
def test_background_work_needs_both_switches(monkeypatch, background, enabled, expected):
    monkeypatch.setattr(scheduler.settings, "facebook_background_crawl_enabled", background)
    schedules = AsyncMock(return_value=[{"platform": "facebook", "enabled": enabled}])
    monkeypatch.setattr(scheduler.db, "list_crawl_schedules", schedules)
    assert asyncio.run(scheduler._facebook_crawl_enabled()) is expected
    assert schedules.call_count == int(background)


def test_disabled_background_does_not_disable_the_explicit_daily_slot(monkeypatch):
    monkeypatch.setattr(scheduler.settings, "facebook_background_crawl_enabled", False)
    monkeypatch.setattr(scheduler, "datetime", AtSlot)
    monkeypatch.setattr(
        scheduler.db,
        "list_crawl_schedules",
        AsyncMock(
            return_value=[
                {
                    "platform": "facebook",
                    "enabled": True,
                    "run_time": "19:30",
                    "nurture_before": False,
                    "nurture_after": False,
                }
            ]
        ),
    )
    claim = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(scheduler, "_claim_crawl_slot", claim)
    jobs = []
    monkeypatch.setattr(scheduler, "_spawn", lambda coro: (jobs.append(coro), coro.close()))
    asyncio.run(scheduler._tick())
    asyncio.run(scheduler._tick())
    assert len(jobs) == 1
    assert claim.call_args.args == ("facebook", "2026-11-04", "19:30")


def test_tracked_posts_keep_logged_out_metrics_without_background_accounts(monkeypatch):
    rows = [
        {
            "id": "known",
            "platform": "facebook",
            "crawlable": 1,
            "external_id": "e1",
            "page": "A23",
            "url": "https://www.facebook.com/a23/posts/1",
        },
        {
            "id": "new",
            "platform": "facebook",
            "crawlable": 1,
            "external_id": None,
            "page": "B",
            "url": "https://www.facebook.com/b/posts/2",
        },
    ]
    updates = []

    async def d1(sql, params=None, **kwargs):
        if sql.startswith("SELECT external_id"):
            return [{"external_id": "e1", "url": "https://www.facebook.com/a23/posts/1"}]
        updates.append((sql, params))
        return []

    redis = SimpleNamespace(lpush=AsyncMock())
    monkeypatch.setattr(tracked_posts, "_active_rows", AsyncMock(return_value=rows))
    monkeypatch.setattr(tracked_posts, "d1_query", d1)
    monkeypatch.setattr(tracked_posts, "get_redis_client", lambda: redis)
    monkeypatch.setattr(tracked_posts.settings, "tracked_facebook_logged_out", True)

    stats = asyncio.run(tracked_posts.run_daily(include_authenticated_facebook=False))

    redis.lpush.assert_awaited_once()
    queue, payload = redis.lpush.await_args.args
    job = json.loads(payload)
    assert queue == tracked_posts.IDLE_QUEUE_KEY
    assert job["type"] == "post_metrics"
    assert job["fallback_page_feeds"] == {}  # page_feed dự phòng sẽ dùng tài khoản
    assert stats["facebook_deferred"] == 1
    assert len(updates) == 1
    assert updates[0][1][1:] == ["known"]  # bài chưa được xếp lượt nào không ghi last_queued_at


class AtSlot(scheduler.datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 4, 19, 30, tzinfo=tz)


def test_facebook_slot_crawls_without_nurture_even_with_old_flags_enabled(monkeypatch):
    keyword = {"id": "movie", "keyword": "Test movie"}
    monkeypatch.setattr(scheduler.settings, "facebook_crawl_lanes", False)
    monkeypatch.setattr(scheduler, "get_enabled_keywords", AsyncMock(return_value=[keyword]))
    monkeypatch.setattr(scheduler, "plan_keyword_crawl", AsyncMock(return_value=([keyword], {})))
    monkeypatch.setattr(scheduler, "queue_source_pages", AsyncMock())
    publish = AsyncMock(return_value=True)
    nurture = AsyncMock(side_effect=AssertionError("Facebook no longer queues nurture jobs"))
    monkeypatch.setattr(scheduler, "publish_crawl_request", publish)
    monkeypatch.setattr(scheduler, "publish_nurture_request", nurture)

    class Progress(set):
        def run_id(self, identifier):
            return f"test-{identifier}"

        async def add(self, value):
            super().add(value)

    assert asyncio.run(
        scheduler._trigger_platform(
            "facebook",
            progress=Progress(),
            nurture_before=True,
            nurture_after=True,
        )
    )
    publish.assert_awaited_once()
    assert publish.await_args.kwargs["keyword"] == "Test movie"
    nurture.assert_not_awaited()


def test_nurture_all_only_queues_threads(monkeypatch):
    from app.api.routes import settings as routes

    publish = AsyncMock(return_value=True)
    monkeypatch.setattr(routes, "publish_nurture_request", publish)
    asyncio.run(routes.nurture_accounts(routes.NurtureRequest()))
    publish.assert_awaited_once()
    assert publish.await_args.args[0] == "threads"


def test_nurture_cannot_target_a_facebook_account(monkeypatch):
    from app.api.routes import settings as routes

    monkeypatch.setattr(routes.db, "get_account", AsyncMock(return_value={"id": 1, "platform": "facebook"}))
    publish = AsyncMock()
    monkeypatch.setattr(routes, "publish_nurture_request", publish)
    with pytest.raises(routes.ValidationError):
        asyncio.run(routes.nurture_accounts(routes.NurtureRequest(account_id=1)))
    publish.assert_not_awaited()


def test_saving_facebook_schedule_discards_old_nurture_flags(monkeypatch):
    from app.api.routes import settings as routes

    save = AsyncMock(
        return_value={
            "platform": "facebook",
            "run_time": "19:30",
            "enabled": True,
            "updated_at": "2026-11-04T12:30:00Z",
        }
    )
    monkeypatch.setattr(routes.db, "upsert_crawl_schedule", save)
    asyncio.run(
        routes.set_crawl_schedule(
            "facebook",
            routes.CrawlScheduleUpdate(
                run_times=["19:30"],
                enabled=True,
                nurture_before=True,
                nurture_after=True,
            ),
        )
    )
    assert save.await_args.kwargs["nurture_before"] is False
    assert save.await_args.kwargs["nurture_after"] is False
