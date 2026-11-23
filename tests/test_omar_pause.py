import asyncio
import json
from datetime import UTC, datetime, timedelta

from app.core.config import settings
from app.services.omar import omar_hashtag as oh
from app.services.omar import omar_pause as pause_mod
from app.services.omar import omar_tiktok as ot


class _FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, **_):
        self.values[key] = value
        return True

    async def delete(self, key):
        self.values.pop(key, None)


def _setup(monkeypatch, *, usage=None, key="key-a"):
    redis = _FakeRedis()
    monkeypatch.setattr(pause_mod, "get_redis_client", lambda: redis)

    async def fake_usage(*_args, **_kwargs):
        return usage

    async def fake_settings():
        return {"api_key": key}

    monkeypatch.setattr(ot, "fetch_usage", fake_usage)
    monkeypatch.setattr(pause_mod, "get_omar_api_settings", fake_settings)
    return redis


def _pause(redis):
    raw = redis.values.get(pause_mod._PAUSE_KEY)
    return json.loads(raw) if raw else None


def test_quota_stop_pauses_until_reset(monkeypatch):
    redis = _setup(monkeypatch, usage={"day": {"remaining": 0, "resets_in": 3600}})
    asyncio.run(ot._pause_after_round({"stopped": "daily_limit: Daily limit reached"}, "key-a", None, False))
    pause = _pause(redis)
    assert pause is not None
    assert pause["reason"] == "quota"
    until = datetime.fromisoformat(pause["until"])
    assert timedelta(minutes=59) < until - datetime.now(tz=UTC) < timedelta(minutes=62)
    assert asyncio.run(pause_mod.pause_over(pause)) is False


def test_exhausted_budget_pauses_until_reset(monkeypatch):
    redis = _setup(monkeypatch, usage={"day": {"remaining": settings.omar_daily_reserve, "resets_in": 600}})
    asyncio.run(ot._pause_after_round({}, "key-a", None, True))
    assert _pause(redis)["reason"] == "quota"


def test_no_pause_when_quota_left_or_work_done(monkeypatch):
    redis = _setup(monkeypatch, usage={"day": {"remaining": 900, "resets_in": 600}})
    asyncio.run(ot._pause_after_round({}, "key-a", None, True))  # gói vừa được nâng: còn hạn mức
    asyncio.run(ot._pause_after_round({}, "key-a", None, False))  # hết việc trước khi hết ngân sách
    assert _pause(redis) is None


def test_key_error_pause_ends_when_key_changes(monkeypatch):
    redis = _setup(monkeypatch, key="key-a")
    asyncio.run(ot._pause_after_round({"stopped": "http_401: invalid key"}, "key-a", None, False))
    pause = _pause(redis)
    assert pause is not None
    assert pause["reason"] == "key"
    assert asyncio.run(pause_mod.pause_over(pause)) is False
    _setup(monkeypatch, key="key-b")
    assert asyncio.run(pause_mod.pause_over(pause)) is True


def test_pause_over_after_until():
    past = {"reason": "quota", "until": (datetime.now(tz=UTC) - timedelta(seconds=1)).isoformat()}
    assert asyncio.run(pause_mod.pause_over(past)) is True
    assert asyncio.run(pause_mod.pause_over({"reason": "quota"})) is True  # dữ liệu hỏng: không kẹt mãi


class _StateRedis:
    def __init__(self, entry):
        self.state = {"kw1": json.dumps(entry)} if entry is not None else {}

    async def hget(self, _key, field):
        return self.state.get(field)

    async def hset(self, _key, field, value):
        self.state[field] = value


def test_recrawl_clears_cursor_but_keeps_hashtag_info(monkeypatch):
    redis = _StateRedis(
        {
            "cursor": "abc",
            "done_at": "2026-11-18T00:00:00+00:00",
            "pages": 255,
            "video_count": 5100,
            "hashtag_id": "h1",
            "info_at": "x",
        }
    )
    monkeypatch.setattr(oh, "get_redis_client", lambda: redis)
    started = []
    monkeypatch.setattr(oh, "crawl_hashtag_now", lambda keyword: started.append(keyword["id"]) or True)
    assert asyncio.run(oh.recrawl_hashtag_now({"id": "kw1", "keyword": "#x"})) is True
    assert json.loads(redis.state["kw1"]) == {"video_count": 5100, "info_at": "x", "hashtag_id": "h1"}
    assert started == ["kw1"]


def test_recrawl_refuses_while_hashtag_is_being_crawled(monkeypatch):
    redis = _StateRedis({"cursor": "abc", "pages": 3})
    monkeypatch.setattr(oh, "get_redis_client", lambda: redis)
    monkeypatch.setattr(oh, "_manual_running", {"kw1"})
    monkeypatch.setattr(
        oh, "crawl_hashtag_now", lambda keyword: (_ for _ in ()).throw(AssertionError("must not start"))
    )
    assert asyncio.run(oh.recrawl_hashtag_now({"id": "kw1", "keyword": "#x"})) is False
    assert json.loads(redis.state["kw1"])["cursor"] == "abc"
