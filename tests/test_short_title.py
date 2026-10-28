"""Tên rút gọn quanh ngày công chiếu (short_title): "Phim Scotty đang chiếu" là bài về "SCOTTY: GIẢI CỨU HOÀNG THƯỢNG"."""

from __future__ import annotations

from app.services.relevance.relevance_rules import mentions_short_title, resolve_relevance, short_title_names

SCOTTY = {"title": "SCOTTY: GIẢI CỨU HOÀNG THƯỢNG", "released_at": "2026-10-02"}


def test_short_title_names() -> None:
    assert short_title_names(SCOTTY["title"]) == ["scotty", "giai cuu hoang thuong"]
    assert short_title_names("Trại Buôn Người") == []  # không có phần tách - đã có cổng tên đầy đủ
    assert short_title_names("Mai: Ngày Xưa") == []  # tên ngắn / cụm thường ngày


def test_mentions_short_title_window() -> None:
    assert mentions_short_title("Phim Scotty đang chiếu tại rạp", SCOTTY, "2026-09-28")
    assert mentions_short_title("#scotty #fanxinema", SCOTTY, "2026-09-25")
    assert mentions_short_title("Giải Cứu Hoàng Thượng đang chiếu", SCOTTY, "2026-10-03")
    assert not mentions_short_title("Phim Scotty", SCOTTY, "2026-12-30")  # quá xa ngày chiếu
    assert not mentions_short_title("Scottyyy", SCOTTY, "2026-09-28")
    assert not mentions_short_title("Phim Scotty", {"title": SCOTTY["title"]}, "2026-09-28")  # không có ngày chiếu


def test_resolve_relevance_short_title() -> None:
    relevant, label, context = resolve_relevance(
        None, "Phim Scotty đang chiếu", SCOTTY, has_keyword=False, strict=False, posted_at="2026-09-28"
    )
    assert (relevant, label, context) == (True, "related", "short_title")
