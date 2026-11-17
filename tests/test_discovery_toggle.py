import asyncio

from app.services.crawl.scheduler import discovery as scheduler


def test_discovery_enabled_reads_the_toggle(monkeypatch):
    async def off():
        return {"enabled": False}

    async def on():
        return {"enabled": True}

    async def broken():
        raise RuntimeError("db down")

    monkeypatch.setattr(scheduler.db, "get_discovery_settings", off)
    assert asyncio.run(scheduler.discovery_enabled()) is False
    monkeypatch.setattr(scheduler.db, "get_discovery_settings", on)
    assert asyncio.run(scheduler.discovery_enabled()) is True
    # Không đọc được: giữ hành vi cũ (bật) thay vì lặng lẽ tắt.
    monkeypatch.setattr(scheduler.db, "get_discovery_settings", broken)
    assert asyncio.run(scheduler.discovery_enabled()) is True


class _FakeRedis:
    def __init__(self):
        self.kv = {}

    async def get(self, key):
        return self.kv.get(key)

    async def set(self, key, value, ex=None):
        self.kv[key] = value


def _tick(monkeypatch, *, enabled):
    ran, spawned = [], []

    async def settings_row():
        return {"enabled": enabled}

    async def job(name):
        ran.append(name)

    monkeypatch.setattr(scheduler, "get_redis_client", lambda: _FakeRedis())
    monkeypatch.setattr(scheduler, "is_due", lambda run_time, now: True)
    monkeypatch.setattr(scheduler.db, "get_discovery_settings", settings_row)
    monkeypatch.setattr(scheduler.threads_channels, "auto_add_authors", lambda: job("threads"))
    monkeypatch.setattr(scheduler, "run_query_discovery", lambda: job("queries"))
    monkeypatch.setattr(scheduler, "review_source_pages", lambda: job("sources"))
    monkeypatch.setattr(scheduler, "spawn", lambda coro: spawned.append(coro))
    asyncio.run(scheduler.hashtag_tick())
    for coro in spawned:
        asyncio.run(coro)
    return ran


def test_daily_tick_runs_everything_when_the_switch_is_on(monkeypatch):
    assert _tick(monkeypatch, enabled=True) == ["threads", "queries", "sources"]


def test_daily_tick_only_adds_threads_channels_when_the_switch_is_off(monkeypatch):
    # Kênh Threads tự thêm theo số bài liên quan (không dùng AI) nên vẫn chạy; câu tìm kiếm và trang nguồn theo công tắc chung.
    assert _tick(monkeypatch, enabled=False) == ["threads"]
