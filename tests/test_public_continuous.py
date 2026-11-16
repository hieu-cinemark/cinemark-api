import asyncio
from datetime import UTC, datetime

from app.services.crawl import public_continuous as pc

NOW = datetime(2026, 11, 16, 8, 0, tzinfo=UTC)


class _Published(list):
    switch: dict


class _Redis:
    def __init__(self, lens: dict[str, int] | None = None, last: dict[str, str] | None = None):
        self.lens = lens or {}
        self.last = dict(last or {})

    async def llen(self, key):
        return self.lens.get(key, 0)

    async def hgetall(self, key):
        return dict(self.last)

    async def hset(self, key, field, value):
        self.last[field] = value


def _setup(monkeypatch, redis, keywords):
    published: _Published = _Published()

    async def publish(**kwargs):
        published.append(kwargs["keyword_id"])
        return True

    async def get_keywords(platform):
        assert platform == "youtube"
        return keywords

    async def ensure():
        return 0

    monkeypatch.setattr(pc, "get_redis_client", lambda: redis)
    monkeypatch.setattr(pc, "publish_crawl_request", publish)
    monkeypatch.setattr(pc, "get_enabled_keywords", get_keywords)
    monkeypatch.setattr("app.services.crawl.scheduler.crawl_slots.ensure_youtube_keywords", ensure)
    switch = {"on": True}

    async def setting(name):
        assert name == "youtube_continuous"
        return switch["on"]

    monkeypatch.setattr(pc, "crawl_setting", setting)
    monkeypatch.setattr(pc.settings, "youtube_continuous_batch", 2)
    monkeypatch.setattr(pc.settings, "youtube_continuous_min_gap_hours", 3)
    published.switch = switch
    return published


KEYWORDS = [{"id": f"k{i}", "keyword": f"Phim {i}"} for i in range(4)]


def test_queues_oldest_films_when_youtube_lanes_are_empty(monkeypatch):
    now = NOW.timestamp()
    # k0 vừa chạy 1 giờ trước (chưa đủ 3 giờ), k1 chạy 5 giờ trước, k2/k3 chưa từng chạy.
    redis = _Redis(last={"k0": str(now - 3600), "k1": str(now - 5 * 3600)})
    published = _setup(monkeypatch, redis, KEYWORDS)
    assert asyncio.run(pc.youtube_idle_tick(NOW)) == 2
    assert published == ["k2", "k3"]  # chưa từng chạy đi trước
    assert float(redis.last["k2"]) == now


def test_does_nothing_while_youtube_queue_has_work(monkeypatch):
    redis = _Redis(lens={pc.YOUTUBE_QUEUE_KEYS[1]: 5})
    published = _setup(monkeypatch, redis, KEYWORDS)
    assert asyncio.run(pc.youtube_idle_tick(NOW)) == 0
    assert published == []


def test_respects_switch_and_min_gap(monkeypatch):
    now = NOW.timestamp()
    redis = _Redis(last={k["id"]: str(now - 600) for k in KEYWORDS})
    published = _setup(monkeypatch, redis, KEYWORDS)
    assert asyncio.run(pc.youtube_idle_tick(NOW)) == 0  # mọi phim vừa chạy 10 phút trước
    published.switch["on"] = False
    redis.last = {}
    assert asyncio.run(pc.youtube_idle_tick(NOW)) == 0
    assert published == []
