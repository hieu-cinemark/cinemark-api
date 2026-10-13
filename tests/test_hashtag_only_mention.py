"""Quy tắc "gắn hashtag ké" (relevance_rules.hashtag_only_mention) và cách resolve_relevance xử lý nó."""

from __future__ import annotations

import pytest

from app.services.relevance_rules import hashtag_only_mention, resolve_relevance

TRAI_BUON_NGUOI = {
    "title": "Trại Buôn Người",
    "keywords": ["#traibuonnguoi"],
    "director": "Ngô Thanh Vân",
    "cast": "Steven Nguyễn, Kaity Nguyễn",
    "distributor": "Galaxy Studio",
}
HOANG_HAU = {"title": "Hoàng Hậu Cuối Cùng", "keywords": ["hoàng hậu cuối cùng"]}


@pytest.mark.parametrize(
    ("content", "movie"),
    [
        ("Khi có đủ yêu thương thì người bình thường cũng hoá anh hùng💕 #traibuonnguoi #fyp #trend", TRAI_BUON_NGUOI),
        ("U50 mà cô nét cỡ này đó #cookingshowbiz #HoangHauCuoiCung #tiktokgiaitri", HOANG_HAU),
        # Caption chỉ toàn hashtag / chỉ còn emoji cũng là hashtag-only.
        ("#traibuonnguoi #fyp #xuhuong", TRAI_BUON_NGUOI),
        ("😭😭 #TrạiBuônNgười", TRAI_BUON_NGUOI),
        # Hashtag tên diễn viên quá ngắn (< 6 ký tự viết liền) không tính.
        ("Hôm nay trời đẹp ghê #anhvu #traibuonnguoi", {**TRAI_BUON_NGUOI, "cast": "Anh Vũ"}),
        # Hashtag chung chung (#tiktokgiaitri, #showbiz) không phải ngữ cảnh phim.
        ("Ăn sáng thôi #traibuonnguoi #tiktokgiaitri #showbiz #xuhuong", TRAI_BUON_NGUOI),
        # Viết tắt tên phim trong hashtag.
        ("Đi chơi cuối tuần nè #tbn #trend", TRAI_BUON_NGUOI),
        # "về"/"điện" không phải "vé"/"diễn".
        ("Về quê ăn Tết, điện thoại hết pin #traibuonnguoi", TRAI_BUON_NGUOI),
    ],
)
def test_hashtag_only(content: str, movie: dict) -> None:
    assert hashtag_only_mention(content, movie) is True


@pytest.mark.parametrize(
    "content",
    [
        "Review phim Trại Buôn Người hay quá #traibuonnguoi",
        "Steven Nguyễn diễn đỉnh #traibuonnguoi",
        "Trại Buôn Người hay quá trời #traibuonnguoi",  # tên phim nằm trong phần chữ
        "trai buon nguoi coi cuon ghe #fyp",  # không dấu
        "TBN 2 tiếng không chớp mắt #traibuonnguoi",  # viết tắt trong phần chữ
        "Ra rạp liền mấy bà ơi #traibuonnguoi",
        "Mua vé xem rồi nha #traibuonnguoi",
        "Cảnh cuối nổi da gà #traibuonnguoi",
        "Ngô Thanh Vân làm tới luôn #traibuonnguoi",  # đạo diễn
        "Galaxy Studio chơi lớn #traibuonnguoi",  # nhà phát hành
        "Trailer đây #traibuonnguoi",
        "best movie of the year #traibuonnguoi",
        # Hashtag ngữ cảnh phim / tên diễn viên, đạo diễn.
        "#reviewphim #tiktoksoiphim #traibuonnguoi",
        "#PhimTraiBuonNguoi 🥰",
        "Hôm nay trời đẹp ghê #StevenNguyen #traibuonnguoi",
        "Ủa gì vậy trời #ngothanhvan #traibuonnguoi",
        "Nổi da gà #traibuonnguoi #phimchieurap #fyp",
        "Quá dữ #traibuonnguoi #trailer",
        "Hồi hộp ghê #CinestarMoviesStation #traibuonnguoi",
        # Không nhắc phim ở đâu cả: không phải việc của quy tắc này.
        "Hôm nay ăn phở #fyp",
        "",
    ],
)
def test_not_hashtag_only(content: str) -> None:
    assert hashtag_only_mention(content, TRAI_BUON_NGUOI) is False


def test_film_title_hashtag_with_phim_prefix() -> None:
    movie = {"title": "Quý Tử Vượt Giàu", "keywords": ["#quytuvuotgiau"]}
    assert hashtag_only_mention("#phimquytuvuotgiau 🥰", movie) is False
    assert hashtag_only_mention("#quytuvuotgiau 🥰", movie) is True


def test_keywords_argument_is_used() -> None:
    movie = {"title": "Phim Gì Đó Rất Dài"}
    assert hashtag_only_mention("Vui ghê #tbnxyz", movie, ["#tbnxyz"]) is True
    assert hashtag_only_mention("tbnxyz vui ghê", movie, ["#tbnxyz"]) is False


HASHTAG_ONLY = "Khi có đủ yêu thương thì người bình thường cũng hoá anh hùng💕 #traibuonnguoi #fyp #trend"


@pytest.mark.parametrize(("label", "confidence"), [("related", 0.85), ("uncertain", 0.95), (None, None)])
def test_resolve_hides_hashtag_only(label: str | None, confidence: float | None) -> None:
    assert resolve_relevance(
        label, HASHTAG_ONLY, TRAI_BUON_NGUOI, has_keyword=True, strict=False, hashtag_only=True, confidence=confidence
    ) == (False, "not_related", "hashtag_only")


def test_resolve_trusts_very_confident_related() -> None:
    assert resolve_relevance(
        "related", HASHTAG_ONLY, TRAI_BUON_NGUOI, has_keyword=True, strict=False, hashtag_only=True, confidence=0.93
    ) == (True, "related", None)


def test_resolve_without_flag_unchanged() -> None:
    # Không có cờ: hashtag tên phim vẫn nâng "uncertain" lên như trước.
    assert resolve_relevance("uncertain", HASHTAG_ONLY, TRAI_BUON_NGUOI, has_keyword=True, strict=False) == (
        True,
        "related",
        "title_hashtag",
    )
