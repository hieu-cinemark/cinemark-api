import asyncio
import json
import time

from app.api.routes import stats as stats_routes
from app.services.reports import stats_summary


class _FakeRedis:
    def __init__(self):
        self.kv: dict[str, str] = {}

    async def get(self, key):
        return self.kv.get(key)

    async def set(self, key, value, ex=None, nx=False):
        self.kv[key] = value
        return True


def _setup(monkeypatch, redis):
    monkeypatch.setattr(stats_routes, "get_redis_client", lambda: redis)
    monkeypatch.setattr(stats_routes, "_QUERY_TIMEOUT_SECONDS", 0.05)


def test_fresh_cache_skips_d1(monkeypatch):
    redis = _FakeRedis()
    _setup(monkeypatch, redis)
    redis.kv[stats_routes._CACHE_KEY.format(name="x")] = json.dumps({"at": time.time(), "rows": [{"n": 1}]})
    calls = []

    async def compute():
        calls.append(1)
        return [{"n": 2}]

    assert asyncio.run(stats_routes._cached_rows("x", compute)) == [{"n": 1}]
    assert calls == []


def test_slow_or_empty_d1_serves_last_good_rows(monkeypatch):
    redis = _FakeRedis()
    _setup(monkeypatch, redis)
    redis.kv[stats_routes._CACHE_KEY.format(name="x")] = json.dumps({"at": time.time() - 3600, "rows": [{"n": 1}]})

    async def slow():
        await asyncio.sleep(1)
        return [{"n": 2}]

    async def empty():
        return []

    assert asyncio.run(stats_routes._cached_rows("x", slow)) == [{"n": 1}]
    assert asyncio.run(stats_routes._cached_rows("x", empty)) == [{"n": 1}]


def test_fresh_result_is_cached(monkeypatch):
    redis = _FakeRedis()
    _setup(monkeypatch, redis)

    async def compute():
        return [{"n": 3}]

    assert asyncio.run(stats_routes._cached_rows("x", compute)) == [{"n": 3}]
    assert json.loads(redis.kv[stats_routes._CACHE_KEY.format(name="x")])["rows"] == [{"n": 3}]


def test_reconcile_recent_days_overwrites_without_deleting(monkeypatch):
    executed: list[tuple[str, list]] = []

    async def fake_q(sql, params=None, **_):
        executed.append((sql, params))
        return []

    async def no_tables():
        return None

    monkeypatch.setattr(stats_summary, "_q", fake_q)
    monkeypatch.setattr(stats_summary, "ensure_stats_tables", no_tables)
    assert asyncio.run(stats_summary.reconcile_recent_days(2))
    assert len(executed) == 4
    assert not any("DELETE" in sql for sql, _ in executed)
    assert all("DO UPDATE SET" in sql and "+ excluded" not in sql for sql, _ in executed)
    assert len({tuple(params) for _, params in executed}) == 1  # cùng một mốc ngày


def test_reconcile_stops_on_failed_step(monkeypatch):
    async def failing_q(sql, params=None, **_):
        return None

    async def no_tables():
        return None

    monkeypatch.setattr(stats_summary, "_q", failing_q)
    monkeypatch.setattr(stats_summary, "ensure_stats_tables", no_tables)
    assert asyncio.run(stats_summary.reconcile_recent_days(2)) is False
