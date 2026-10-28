import asyncio
import json
from datetime import UTC, datetime, timedelta

from app.core.config import settings
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
    monkeypatch.setattr(ot, "get_redis_client", lambda: redis)

    async def fake_usage(*_args, **_kwargs):
        return usage

    async def fake_settings():
        return {"api_key": key}

    monkeypatch.setattr(ot, "fetch_usage", fake_usage)
    monkeypatch.setattr(ot, "get_omar_api_settings", fake_settings)
    return redis


def _pause(redis):
    raw = redis.values.get(ot._PAUSE_KEY)
    return json.loads(raw) if raw else None


def test_quota_stop_pauses_until_reset(monkeypatch):
    redis = _setup(monkeypatch, usage={"day": {"remaining": 0, "resets_in": 3600}})
    asyncio.run(ot._pause_after_round({"stopped": "daily_limit: Daily limit reached"}, "key-a", None, False))
    pause = _pause(redis)
    assert pause is not None
    assert pause["reason"] == "quota"
    until = datetime.fromisoformat(pause["until"])
    assert timedelta(minutes=59) < until - datetime.now(tz=UTC) < timedelta(minutes=62)
    assert asyncio.run(ot.pause_over(pause)) is False


def test_exhausted_budget_pauses_only_in_continuous_mode(monkeypatch):
    redis = _setup(monkeypatch, usage={"day": {"remaining": settings.omar_daily_reserve, "resets_in": 600}})
    monkeypatch.setattr(settings, "omar_spread_daily", True)
    asyncio.run(ot._pause_after_round({}, "key-a", None, True))
    assert _pause(redis) is None  # rải đều theo giờ: hết ngân sách giờ là bình thường
    monkeypatch.setattr(settings, "omar_spread_daily", False)
    asyncio.run(ot._pause_after_round({}, "key-a", None, True))
    assert _pause(redis)["reason"] == "quota"


def test_no_pause_when_quota_left_or_work_done(monkeypatch):
    redis = _setup(monkeypatch, usage={"day": {"remaining": 900, "resets_in": 600}})
    monkeypatch.setattr(settings, "omar_spread_daily", False)
    asyncio.run(ot._pause_after_round({}, "key-a", None, True))  # gói vừa được nâng: còn hạn mức
    asyncio.run(ot._pause_after_round({}, "key-a", None, False))  # hết việc trước khi hết ngân sách
    assert _pause(redis) is None


def test_key_error_pause_ends_when_key_changes(monkeypatch):
    redis = _setup(monkeypatch, key="key-a")
    asyncio.run(ot._pause_after_round({"stopped": "http_401: invalid key"}, "key-a", None, False))
    pause = _pause(redis)
    assert pause is not None
    assert pause["reason"] == "key"
    assert asyncio.run(ot.pause_over(pause)) is False
    _setup(monkeypatch, key="key-b")
    assert asyncio.run(ot.pause_over(pause)) is True


def test_pause_over_after_until():
    past = {"reason": "quota", "until": (datetime.now(tz=UTC) - timedelta(seconds=1)).isoformat()}
    assert asyncio.run(ot.pause_over(past)) is True
    assert asyncio.run(ot.pause_over({"reason": "quota"})) is True  # dữ liệu hỏng: không kẹt mãi
