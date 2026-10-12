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


def test_weekly_due(monkeypatch):
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
