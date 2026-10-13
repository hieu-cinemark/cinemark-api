from app.core.config import settings
from app.services.omar_yield import share_for, totals


def _m(requests, new):
    return {"requests": requests, "sent": new, "new": new}


def test_totals_new_per_100():
    days = [
        {
            "day": "2026-10-13",
            "sources": {"hashtag": _m(1000, 50), "search": _m(200, 40), "channel": _m(0, 0), "comments": _m(0, 0)},
        },
        {
            "day": "2026-10-12",
            "sources": {"hashtag": _m(1000, 30), "search": _m(100, 5), "channel": _m(0, 0), "comments": _m(0, 0)},
        },
    ]
    out = totals(days)
    assert out["hashtag"]["requests"] == 2000 and out["hashtag"]["new_per_100"] == 4.0
    assert out["search"]["new_per_100"] == 15.0
    assert out["channel"]["new_per_100"] is None


def test_share_for_scales_and_clamps(monkeypatch):
    monkeypatch.setattr(settings, "omar_adaptive_min_share", 0.05)
    monkeypatch.setattr(settings, "omar_adaptive_max_share", 0.45)
    measured = {"hashtag": _m(1000, 40), "search": _m(500, 60), "channel": _m(500, 2)}
    assert share_for("search", 0.2, measured) == 0.45  # 3x hashtag -> 2.5x (kẹp) -> 0.5 -> kẹp 0.45
    assert share_for("channel", 0.2, measured) == 0.05  # 0.1x -> 0.25x (kẹp) -> 0.05
    assert share_for("search", 0.2, {"hashtag": _m(100, 4), "search": _m(500, 60)}) == 0.2  # chưa đủ số liệu


def test_keep_suggestion():
    from app.services.omar_search import keep_suggestion

    title = "trai buon nguoi"
    assert keep_suggestion("trai buon nguoi trailer", title)
    assert keep_suggestion("trai buon nguoi đạo diễn cast", title)
    assert not keep_suggestion("Trai Buon Nguoi", title)  # trùng tên phim
    assert not keep_suggestion("trai buon nguoi netflix español", title)
    assert not keep_suggestion("trai buon nguoi netflix de que trata en español", title)
    assert not keep_suggestion("phim hay tháng 10", title)
