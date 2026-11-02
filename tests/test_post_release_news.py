import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from app.core.errors import ConflictError, UpstreamError, ValidationError
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
    clock = {"now": datetime(2026, 11, 2, 10, tzinfo=UTC)}
    movie = {"id": "film", "title": "Út Lan 2", "released_at": "2026-10-30", "enabled": 1}
    monkeypatch.setattr(news, "now", lambda: clock["now"])
    monkeypatch.setattr(news, "get_redis_client", lambda: cache)
    monkeypatch.setattr(news, "d1_query", AsyncMock(return_value=[movie]))
    search = AsyncMock(
        return_value={
            "news_results": [
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
    monkeypatch.setattr(news.google_news, "search", search)
    bee = AsyncMock(return_value="Điểm được khen: kịch bản [1].")
    monkeypatch.setattr(news, "call_ai", bee)
    return cache, clock, movie, search, bee


@pytest.mark.parametrize("value", [None, "", "2026-11-03", "invalid", "2026-02-30"])
async def test_unreleased_or_unknown_date_cannot_fetch_or_analyze(setup, value):
    setup[2]["released_at"] = value
    with pytest.raises(ValidationError):
        await news.get_news("film")
    with pytest.raises(ValidationError):
        await news.analyze("film")
    setup[3].assert_not_called()
    setup[4].assert_not_called()


async def test_reuses_news_for_72_hours_then_invalidates_analysis(setup):
    first = await news.get_news("film")
    assert setup[3].call_args.args[0].q == 'phim "Út Lan 2" đánh giá'
    analyzed = await news.analyze("film")
    assert analyzed["analysis"]["provider"] == "Bee"
    setup[1]["now"] += timedelta(hours=71, minutes=59)
    assert (await news.get_news("film"))["generatedAt"] == first["generatedAt"]
    assert setup[3].await_count == 1
    setup[1]["now"] += timedelta(minutes=1)
    new = await news.get_news("film")
    assert new["generatedAt"] != first["generatedAt"]
    assert new["analysis"] is None
    assert setup[3].await_count == 2


async def test_empty_news_is_cached_without_calling_bee(setup):
    setup[3].return_value = {"news_results": []}
    await news.get_news("film")
    await news.get_news("film")
    assert setup[3].await_count == 1
    with pytest.raises(ValidationError):
        await news.analyze("film")
    setup[4].assert_not_called()


async def test_failed_refresh_preserves_old_result_and_backs_off(setup):
    old = await news.get_news("film")
    setup[1]["now"] += timedelta(days=3)
    setup[3].side_effect = UpstreamError("quota")
    assert await news.get_news("film") == old
    assert await news.get_news("film") == old
    assert setup[3].await_count == 2
    assert await setup[0].get(news.PREFIX + "film:lock") is None


async def test_busy_initial_fetch_does_not_spend_quota(setup):
    await setup[0].set(news.PREFIX + "film:lock", "other")
    with pytest.raises(ConflictError):
        await news.get_news("film")
    setup[3].assert_not_called()


async def test_explicit_bee_uses_beeknoee_and_reuses_analysis(setup):
    result = await news.analyze("film")
    kwargs = setup[4].call_args.kwargs
    assert kwargs["provider"] == "beeknoee"
    context = json.loads(kwargs["user_prompt"])
    assert context[0]["source"] == "Báo" and context[0]["link"] == "https://example.com"
    assert await news.analyze("film") == result
    assert setup[4].await_count == 1


async def test_bee_failure_keeps_news(setup):
    setup[4].side_effect = RuntimeError("secret")
    with pytest.raises(UpstreamError, match="Bee") as error:
        await news.analyze("film")
    assert "secret" not in str(error.value)
    saved = await news.cached("film")
    assert saved["data"]["news_results"] and saved["analysis"] is None


async def test_tick_only_released_movies_and_not_more_than_once_per_hour(setup, monkeypatch):
    monkeypatch.setattr(news.settings, "serpapi_api_key", "test")
    news.d1_query.return_value = [
        setup[2],
        {"id": "future", "released_at": "2099-01-01"},
        {"id": "unknown", "released_at": None},
    ]
    get = AsyncMock()
    monkeypatch.setattr(news, "get_news", get)
    await news.tick()
    await news.tick()
    get.assert_awaited_once_with("film")
