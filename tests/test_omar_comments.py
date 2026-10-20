from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.omar_comments import iso_week, map_comment, weekly_due
from app.services.omar_tiktok import Budget, next_cursor

VN = ZoneInfo("Asia/Ho_Chi_Minh")

RAW = {
    "cid": 7693842856536425236,
    "text": "phim hay",
    "create_time": 1791362402,
    "digg_count": 46,
    "reply_comment_total": 22,
    "reply_id": "0",
    "user": {"uid": "1", "unique_id": "abc", "nickname": "Abc", "avatar_thumb": {"url_list": ["https://a/1.jpg"]}},
}


def test_map_comment_top_level():
    comment = map_comment(RAW)
    assert comment == {
        "comment_id": "7693842856536425236",
        "message": "phim hay",
        "timestamp": 1791362402,
        "like_count": 46,
        "reply_count": 22,
        "author_id": "1",
        "author_username": "abc",
        "author_name": "Abc",
        "author_avatar_url": "https://a/1.jpg",
        "parent_comment_id": None,
    }


def test_map_comment_reply_and_missing_id():
    assert map_comment({**RAW, "reply_id": "99"})["parent_comment_id"] == "99"
    assert map_comment(RAW, parent_comment_id="7")["parent_comment_id"] == "7"
    assert map_comment({**RAW, "user": {}})["author_avatar_url"] is None
    assert map_comment({"text": "x"}) is None


def test_daily_due(monkeypatch):
    monkeypatch.setattr(settings, "omar_comments_cadence", "daily")
    monkeypatch.setattr(settings, "omar_comments_time", "01:00")
    assert not weekly_due(datetime(2026, 10, 20, 0, 59, tzinfo=VN))
    assert weekly_due(datetime(2026, 10, 20, 1, 0, tzinfo=VN))
    assert weekly_due(datetime(2026, 10, 21, 1, 0, tzinfo=VN))  # thứ nào cũng chạy
    assert iso_week(datetime(2026, 10, 20, 23, tzinfo=VN)) == "2026-10-20"


def test_weekly_due(monkeypatch):
    monkeypatch.setattr(settings, "omar_comments_cadence", "weekly")
    monkeypatch.setattr(settings, "omar_comments_weekday", 0)
    monkeypatch.setattr(settings, "omar_comments_time", "01:00")
    assert not weekly_due(datetime(2026, 10, 12, 0, 59, tzinfo=VN))  # thứ Hai, trước giờ
    assert weekly_due(datetime(2026, 10, 12, 1, 0, tzinfo=VN))
    assert weekly_due(datetime(2026, 10, 18, 23, 0, tzinfo=VN))  # chủ nhật, chạy bù
    assert iso_week(datetime(2026, 10, 12, tzinfo=VN)) == iso_week(datetime(2026, 10, 18, tzinfo=VN)) == "2026-W42"


def test_budget_and_cursor():
    budget = Budget(2)
    assert budget.take() and budget.take() and not budget.take()
    assert budget.exhausted
    assert next_cursor({"has_more": True, "next_cursor": 30}) == "30"
    assert next_cursor({"has_more": "false", "next_cursor": 30}) is None
    assert next_cursor({"has_more": True}) is None


class _FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}
        self.values: dict[str, str] = {}

    async def hkeys(self, key):
        return list(self.hashes.get(key, {}))

    async def hset(self, key, field=None, value=None, mapping=None):
        self.hashes.setdefault(key, {}).update(mapping or {field: value})

    async def set(self, key, value, **_):
        self.values[key] = value


def test_enqueue_all_videos_only_for_selected_keywords(monkeypatch):
    import asyncio
    import json

    from app.services import omar_comments as oc

    redis = _FakeRedis()
    keywords = [{"id": "k1", "keyword": "#phim"}]
    top = {"k1": [{"id": "p1", "external_id": "v1", "reply_count": 9}]}
    rest = {"k1": [{"id": "p1", "external_id": "v1", "reply_count": 9}, {"id": "p2", "external_id": "v2", "reply_count": 3}]}

    async def fake_keywords():
        return keywords

    async def fake_top(*_):
        return top

    async def fake_rest(_ids):
        return rest

    monkeypatch.setattr(oc, "get_redis_client", lambda: redis)
    monkeypatch.setattr(oc, "comment_keywords", fake_keywords)
    monkeypatch.setattr(oc, "_top_videos", fake_top)
    monkeypatch.setattr(oc, "_all_videos", fake_rest)

    def run(selected: list[str], enabled: bool):
        redis.hashes.clear()

        async def fake_selected():
            return selected

        async def fake_enabled():
            return enabled

        monkeypatch.setattr(oc, "selected_keyword_ids", fake_selected)
        monkeypatch.setattr(oc, "all_videos_enabled", fake_enabled)
        info = asyncio.run(oc.enqueue_weekly_job(week="2026-W42"))
        jobs = {k: json.loads(v) for k, v in redis.hashes.get(oc.JOBS_KEY, {}).items()}
        return info, jobs

    info, jobs = run(["k1"], True)
    assert info["all_videos"] is True and set(jobs) == {"v1", "v2"}
    assert jobs["v1"]["rank"] == 0 and jobs["v2"]["rank"] == 2  # top tuần trước, video trùng chỉ xếp một lần
    _, jobs = run([], True)  # không chọn tay -> bỏ qua tuỳ chọn
    assert set(jobs) == {"v1"}
    _, jobs = run(["k1"], False)
    assert set(jobs) == {"v1"}
