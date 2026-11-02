import json
import sqlite3
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from app.core.errors import ConflictError, UpstreamError, ValidationError
from app.repositories.d1 import press_reviews
from app.services import post_release_news as news


class Cache:
    def __init__(self):
        self.data = {}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return False
        self.data[key] = value
        return True

    async def delete(self, key):
        self.data.pop(key, None)

    async def eval(self, script, count, key, value):
        if self.data.get(key) == value:
            self.data.pop(key)


@pytest.fixture
def setup(monkeypatch):
    cache = Cache()
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    clock = {"now": datetime(2026, 11, 2, 10, tzinfo=UTC)}
    movie = {"id": "film", "title": "Út Lan 2", "released_at": "2026-10-30", "enabled": 1}

    async def query(sql, params=None, **kwargs):
        if "FROM movies" in sql:
            return [movie]
        cursor = db.execute(sql, params or [])
        db.commit()
        return [dict(row) for row in cursor.fetchall()] if cursor.description else []

    monkeypatch.setattr(news, "now", lambda: clock["now"])
    monkeypatch.setattr(news, "get_redis_client", lambda: cache)
    monkeypatch.setattr(news, "d1_query", query)
    monkeypatch.setattr(press_reviews, "d1_query", query)
    search = AsyncMock(
        return_value={
            "organic_results": [
                {
                    "title": "Review",
                    "link": "https://example.com",
                    "snippet": "Kịch bản tốt",
                    "source": "Báo",
                    "date": "1 day ago",
                }
            ]
        }
    )
    monkeypatch.setattr(news.google_news, "search_web", search)
    search.return_value["organic_results"] *= 5
    search.return_value["organic_results"] = [
        {**item, "link": f"https://example.com/{i}"} for i, item in enumerate(search.return_value["organic_results"])
    ]
    monkeypatch.setattr(news.news_articles, "enrich", AsyncMock())
    yield cache, clock, movie, search, db
    db.close()


@pytest.mark.parametrize("value", [None, "", "2026-11-03", "invalid", "2026-02-30"])
async def test_unreleased_or_unknown_date_cannot_fetch_or_read(setup, value):
    setup[2]["released_at"] = value
    with pytest.raises(ValidationError):
        await news.refresh_news("film")
    with pytest.raises(ValidationError):
        await news.read_news("film")
    setup[3].assert_not_called()


async def test_reuses_news_for_72_hours_then_invalidates_analysis(setup):
    first = await news.refresh_news("film")
    assert setup[3].call_args.args[0].q == "phim Út Lan 2 đánh giá"
    setup[4].execute(
        "UPDATE movie_press_reviews SET analysis_text = 'Khen [1]', analysis_at = '2026-11-02', analysis_model = 'bee-model'"
    )
    setup[4].commit()
    setup[1]["now"] += timedelta(hours=71, minutes=59)
    assert (await news.refresh_news("film"))["analysis"]["text"] == "Khen [1]"
    assert setup[3].await_count == 1
    setup[1]["now"] += timedelta(minutes=1)
    new = await news.refresh_news("film")
    assert new["snapshotId"] != first["snapshotId"]
    assert (await news.read_news("film"))["analysis"] is None
    assert setup[3].await_count == 2


async def test_empty_news_is_persisted_and_not_retried_early(setup):
    setup[3].return_value = {"organic_results": []}
    await news.refresh_news("film")
    assert (await news.refresh_news("film"))["data"]["news_results"] == []
    assert setup[3].await_count == 2


async def test_failed_refresh_preserves_old_result_and_backs_off(setup):
    old = await news.refresh_news("film")
    setup[1]["now"] += timedelta(days=3)
    setup[3].side_effect = UpstreamError("quota")
    assert await news.refresh_news("film") == old
    assert await news.refresh_news("film") == old
    assert setup[3].await_count == 2
    assert await setup[0].get(news.PREFIX + "film:lock") is None


async def test_busy_initial_fetch_does_not_spend_quota(setup):
    await setup[0].set(news.PREFIX + "film:lock", "other")
    with pytest.raises(ConflictError):
        await news.refresh_news("film")
    setup[3].assert_not_called()


async def test_read_does_not_fetch_even_without_serpapi_key(setup, monkeypatch):
    monkeypatch.setattr(news.settings, "serpapi_api_key", None)
    assert await news.read_news("film") is None
    setup[3].assert_not_called()


async def test_news_survives_redis_loss_and_exposes_no_ai_credentials(setup):
    old = await news.refresh_news("film")
    setup[0].data.clear()
    assert await news.read_news("film") == old
    assert await news.refresh_news("film") == old
    assert setup[3].await_count == 1
    assert "api_key" not in json.dumps(await news.read_news("film"))


async def test_older_snapshot_cannot_overwrite_newer_snapshot(setup):
    first = await news.refresh_news("film")
    setup[1]["now"] += timedelta(days=3)
    latest = await news.refresh_news("film")
    await press_reviews.save(first)
    assert await news.read_news("film") == latest


async def test_database_write_failure_keeps_old_news(setup, monkeypatch):
    old = await news.refresh_news("film")
    setup[1]["now"] += timedelta(days=3)
    monkeypatch.setattr(press_reviews, "save", AsyncMock(side_effect=RuntimeError("database failed")))
    assert await news.refresh_news("film") == old
    assert await news.read_news("film") == old


async def test_tick_only_released_movies_and_not_more_than_once_per_hour(setup, monkeypatch):
    monkeypatch.setattr(news.settings, "serpapi_api_key", "test")
    monkeypatch.setattr(
        news,
        "d1_query",
        AsyncMock(
            return_value=[
                setup[2],
                {"id": "future", "released_at": "2099-01-01"},
                {"id": "unknown", "released_at": None},
            ]
        ),
    )
    refresh = AsyncMock()
    monkeypatch.setattr(news, "refresh_news", refresh)
    await news.tick()
    await news.tick()
    refresh.assert_awaited_once_with("film")


async def test_sparse_results_use_one_broader_query_and_deduplicate(setup):
    setup[3].side_effect = [
        {"organic_results": [{"title": "One", "link": "https://example.com/one"}]},
        {
            "organic_results": [
                {"title": "Duplicate", "link": "https://example.com/one#section"},
                {"title": "Two", "link": "https://example.com/two"},
            ]
        },
    ]
    result = await news.refresh_news("film")
    assert len(result["data"]["news_results"]) == 2
    assert [call.args[0].q for call in setup[3].call_args_list] == ["phim Út Lan 2 đánh giá", "phim Út Lan 2 review"]


async def test_force_bypasses_snapshot_and_provider_cache(setup):
    old = await news.refresh_news("film")
    setup[1]["now"] += timedelta(minutes=1)
    result = await news.refresh_news("film", force=True)
    assert result["snapshotId"] != old["snapshotId"]
    assert setup[3].call_args.args[0].no_cache is True


async def test_failed_extra_query_keeps_first_results(setup):
    setup[3].side_effect = [
        {"organic_results": [{"title": "One", "link": "https://example.com/one"}]},
        UpstreamError("unavailable"),
    ]
    assert len((await news.refresh_news("film"))["data"]["news_results"]) == 1


async def test_existing_snapshot_is_enriched_without_search_or_schedule_change(setup):
    old = await news.refresh_news("film")
    data = old["data"]
    data.pop("extraction_version")
    setup[4].execute("UPDATE movie_press_reviews SET response_json = ?", [json.dumps(data)])
    setup[4].commit()
    updated = await news.refresh_news("film")
    assert setup[3].await_count == 1
    assert updated["snapshotId"] != old["snapshotId"]
    assert updated["nextRefreshAt"] == old["nextRefreshAt"]
    assert updated["data"]["extraction_version"] == 1
