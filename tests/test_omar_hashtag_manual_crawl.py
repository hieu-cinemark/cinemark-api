"""Đặc tả _crawl_hashtag_now (lượt cào riêng một hashtag - nút "Thêm hashtag"/"Cào lại" và các script thủ công dùng chung
đường này): trang rỗng (0 video) phải coi như đã hết và đánh dấu done_at, kể cả khi fetch_page vẫn trả về một next_cursor
(Omar có lúc báo has_more sai cho một trang thật sự rỗng) - khác lượt cào liên tục (run_omar_round) vốn có thử lại tạm
thời (_handle_empty_page) mà lượt cào riêng này không có, nên không được đi theo cursor của một trang rỗng mãi mãi."""

import asyncio
import json

from app.services.omar import omar_hashtag as oh

RAW = {"id": "v"}


class FakeRedis:
    def __init__(self):
        self.state: dict[str, str] = {}

    async def hget(self, key, field):
        return self.state.get(field)

    async def hset(self, key, field, value):
        self.state[field] = value


def _setup(monkeypatch, *, pages, api_key="key"):
    redis = FakeRedis()
    calls: list[str] = []

    async def fetch_page(client, pacer, key, tag, cursor, hashtag_id=None):
        calls.append(cursor)
        return pages[cursor]

    async def fake_settings():
        return {"api_key": api_key}

    async def noop_cooccur(*_a, **_k):
        return None

    async def noop_publish(*_a, **_k):
        return None

    monkeypatch.setattr(oh, "get_redis_client", lambda: redis)
    monkeypatch.setattr(oh, "get_omar_api_settings", fake_settings)
    monkeypatch.setattr(oh, "fetch_page", fetch_page)
    monkeypatch.setattr(oh, "count_cooccur", noop_cooccur)
    monkeypatch.setattr(oh, "publish_videos", noop_publish)
    return redis, calls


def _entry(redis, kw_id):
    return json.loads(redis.state[kw_id])


def test_empty_page_with_a_cursor_still_marks_done_and_stops(monkeypatch):
    """Omar trả has_more=true (next_cursor="c1") cho một trang 0 video: trước đây vòng lặp đi theo cursor đó mãi mãi,
    không bao giờ xong. Giờ trang rỗng tự coi là hết, kể cả có cursor kế tiếp."""
    redis, calls = _setup(monkeypatch, pages={"0": ([RAW], "c1", "h1"), "c1": ([], "c2", "h1")})
    keyword = {"id": "kw1", "keyword": "#alpha"}
    asyncio.run(oh._crawl_hashtag_now(keyword))
    assert calls == ["0", "c1"]  # không gọi "c2" - trang rỗng đã dừng vòng lặp
    entry = _entry(redis, "kw1")
    assert entry["done_at"] and entry["cursor"] is None and entry["pages"] == 2


def test_empty_first_page_marks_done_immediately(monkeypatch):
    """Hashtag không tồn tại (Omar 404) -> fetch_page trả ([], None, ...) ngay ở trang đầu: vẫn phải xong, không loại trừ."""
    redis, calls = _setup(monkeypatch, pages={"0": ([], None, None)})
    asyncio.run(oh._crawl_hashtag_now({"id": "kw1", "keyword": "#alpha"}))
    assert calls == ["0"]
    assert _entry(redis, "kw1")["done_at"]


def test_page_with_videos_and_no_next_cursor_marks_done(monkeypatch):
    redis, calls = _setup(monkeypatch, pages={"0": ([RAW], None, "h1")})
    asyncio.run(oh._crawl_hashtag_now({"id": "kw1", "keyword": "#alpha"}))
    assert calls == ["0"]
    assert _entry(redis, "kw1")["done_at"]


def test_page_with_videos_and_a_cursor_keeps_going(monkeypatch):
    redis, calls = _setup(monkeypatch, pages={"0": ([RAW], "c1", "h1"), "c1": ([RAW], None, "h1")})
    asyncio.run(oh._crawl_hashtag_now({"id": "kw1", "keyword": "#alpha"}))
    assert calls == ["0", "c1"]
    entry = _entry(redis, "kw1")
    assert entry["done_at"] and entry["pages"] == 2
