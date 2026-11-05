import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from app.services.crawl import watch_pages

NOON_VN = datetime(2026, 11, 5, 5, 0, tzinfo=UTC)
MIDNIGHT_VN = datetime(2026, 11, 5, 17, 30, tzinfo=UTC)


def test_quiet_hours_follow_vietnam_time():
    assert not watch_pages.in_quiet_hours(NOON_VN)
    assert watch_pages.in_quiet_hours(MIDNIGHT_VN)


def test_pick_pages_prefers_showing_film_pages_then_active_shared_pages():
    rows = [
        {"id": "shared-quiet", "movie_id": None, "quiet_runs": 3},
        {"id": "other-film", "movie_id": "old", "quiet_runs": 0},
        {"id": "shared-active", "movie_id": None, "quiet_runs": 0},
        {"id": "film", "movie_id": "now", "quiet_runs": 5},
    ]
    picked = watch_pages.pick_pages(rows, {"now"}, 2)
    assert [row["id"] for row in picked] == ["film", "shared-active"]


def _install(monkeypatch, accounts=1, pending=(), showing=("now",)):
    queue = AsyncMock(return_value=True)
    monkeypatch.setattr(watch_pages, "queue_source", queue)
    monkeypatch.setattr(watch_pages.platform_config_db, "count_crawl_ready_accounts", AsyncMock(return_value=accounts))
    monkeypatch.setattr(watch_pages, "list_pending", AsyncMock(return_value=list(pending)))
    monkeypatch.setattr(watch_pages, "showing_movie_ids", AsyncMock(return_value=set(showing)))
    monkeypatch.setattr(watch_pages, "d1_query", AsyncMock(return_value=[{"id": "film", "movie_id": "now"}]))
    return queue


def test_round_reads_one_feed_page_and_resends_seen_posts(monkeypatch):
    queue = _install(monkeypatch)
    monkeypatch.setattr(watch_pages.settings, "facebook_watch_pages_max_pages", 1)
    assert asyncio.run(watch_pages.queue_round(NOON_VN)) == 1
    options = queue.call_args.kwargs["overrides"]
    assert options["max_pages"] == 1
    assert options["dedupe"] == "false"
    assert options["reels"] == "false"


def test_round_queues_nothing_without_an_account(monkeypatch):
    queue = _install(monkeypatch, accounts=0)
    assert asyncio.run(watch_pages.queue_round(NOON_VN)) == 0
    queue.assert_not_called()


def test_round_queues_nothing_in_quiet_hours(monkeypatch):
    queue = _install(monkeypatch)
    assert asyncio.run(watch_pages.queue_round(MIDNIGHT_VN)) == 0
    queue.assert_not_called()


def test_round_skips_when_page_jobs_are_backing_up(monkeypatch):
    backlog = [{"type": "page_feed"}] * watch_pages.MAX_PENDING_PAGE_FEED
    queue = _install(monkeypatch, pending=backlog)
    assert asyncio.run(watch_pages.queue_round(NOON_VN)) == 0
    queue.assert_not_called()


def test_round_queues_nothing_when_no_film_is_showing(monkeypatch):
    queue = _install(monkeypatch, showing=())
    assert asyncio.run(watch_pages.queue_round(NOON_VN)) == 0
    queue.assert_not_called()
