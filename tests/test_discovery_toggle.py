import asyncio

from app.services.crawl import scheduler


def test_discovery_enabled_reads_the_toggle(monkeypatch):
    async def off():
        return {"enabled": False}

    async def on():
        return {"enabled": True}

    async def broken():
        raise RuntimeError("db down")

    monkeypatch.setattr(scheduler.db, "get_discovery_settings", off)
    assert asyncio.run(scheduler._discovery_enabled()) is False
    monkeypatch.setattr(scheduler.db, "get_discovery_settings", on)
    assert asyncio.run(scheduler._discovery_enabled()) is True
    # Không đọc được: giữ hành vi cũ (bật) thay vì lặng lẽ tắt.
    monkeypatch.setattr(scheduler.db, "get_discovery_settings", broken)
    assert asyncio.run(scheduler._discovery_enabled()) is True


def test_hashtag_tick_skips_when_disabled(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.kv = {}

        async def get(self, key):
            return self.kv.get(key)

        async def set(self, key, value, ex=None):
            self.kv[key] = value

    spawned = []

    async def off():
        return {"enabled": False}

    monkeypatch.setattr(scheduler, "get_redis_client", lambda: FakeRedis())
    monkeypatch.setattr(scheduler, "_is_due", lambda run_time, now: True)
    monkeypatch.setattr(scheduler.db, "get_discovery_settings", off)
    monkeypatch.setattr(scheduler, "_spawn", lambda coro: spawned.append(coro.close()))
    asyncio.run(scheduler._hashtag_tick())
    assert spawned == []
