import asyncio

from app.services import omar_search as os_
from app.services import omar_tiktok as ot


def test_round_robin_interleaves_items():
    calls: list[str] = []
    left = {"a": 3, "b": 3}

    async def step(item):
        calls.append(item)
        left[item] -= 1
        return 0.0 if left[item] else None

    asyncio.run(ot.run_round_robin(["a", "b"], step, workers=1, should_stop=lambda: False))
    assert calls == ["a", "b", "a", "b", "a", "b"]


class _FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    async def hdel(self, key, field):
        self.hashes.get(key, {}).pop(field, None)


def test_search_retries_empty_mid_page_same_cursor(monkeypatch):
    redis = _FakeRedis()
    monkeypatch.setattr(os_, "get_redis_client", lambda: redis)
    monkeypatch.setattr(os_, "search_modes", lambda: [{}])
    monkeypatch.setattr(os_.settings, "omar_search_pages", 10)

    async def no_yield(*_a, **_k):
        return None

    async def no_publish(*_a, **_k):
        return None

    monkeypatch.setattr(os_.omar_yield, "add", no_yield)
    monkeypatch.setattr(ot, "publish_videos", no_publish)
    monkeypatch.setattr(ot, "map_web_item", lambda raw: {"video_id": raw["id"]})
    pages = iter(
        [
            {"results": [{"id": "1"}], "has_more": True, "next_cursor": "20"},
            {"results": [], "has_more": True, "next_cursor": "40"},  # giới hạn tạm thời
            {"results": [{"id": "2"}], "has_more": False},
        ]
    )
    cursors: list[str] = []

    async def fake_get(_client, _pacer, _key, path, params):
        if path == "search_videos":
            cursors.append(params["cursor"])
            return next(pages)
        return {"results": [], "has_more": False}

    monkeypatch.setattr(ot, "omar_get", fake_get)
    search = os_._QuerySearch({"query": "q", "key": "q", "keyword_id": "k"})
    delays = []

    async def run():
        while (delay := await search.step(None, None, "key", lambda: True, {})) is not None:
            delays.append(delay)

    asyncio.run(run())
    assert cursors == ["0", "20", "20"]
    assert ot._EMPTY_RETRY_SECONDS in delays
    assert search.seen_ids == {"1", "2"}
