"""Lọc bài đúng phim, dùng chính các tiêu đề đã lẫn vào dữ liệu thật ngày 28/11/2026."""

import pytest

from app.services import press_relevance as rel


def test_aliases_cover_original_title_and_main_part():
    aliases = rel.title_aliases("QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)")
    for headline in ["'Quỷ ăn tạng 4' chưa đủ sức lật đổ", "Review phim Quỷ Ăn Tạng 4: Hổ Tinh"]:
        assert rel.mention_count(headline, aliases) > 0, headline
    assert rel.mention_count("Review Quỷ Ăn Tạng 3", aliases) == 0
    assert rel.search_title("QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)") == "QUỶ ĂN TẠNG 4: HỔ TINH"


@pytest.mark.parametrize(
    ("movie", "headline"),
    [
        ("Mẹ Mìn", 'Review phim "Làm Giàu Với Ma": Thực hư chuyện Tuấn Trần'),
        ("Mẹ Mìn", '"Mang Mẹ Đi Bỏ": Góc nhìn khác về tình mẫu tử | ELLE'),
        ("Huyết Thống", "(Review) Final Destination: Bloodlines"),
        ("Người Được Chọn", "10 phim điện ảnh Châu Á không thể không xem"),
        ("Chị Chị Em Em 3", "Review Chị Chị Em Em 2: drama nữ quyền"),
    ],
)
def test_other_films_are_rejected(movie, headline):
    assert not rel.is_relevant({"title": headline}, rel.title_aliases(movie))


def test_matching_is_whole_words_with_vietnamese_diacritics():
    aliases = rel.title_aliases("Mẹ Mìn")
    assert rel.is_relevant({"title": "Khán giả khen phim MẸ MÌN: căng"}, aliases)
    assert rel.is_relevant({"title": "Hậu trường", "snippet": "đoàn phim Mẹ Mìn chia sẻ"}, aliases)
    assert not rel.is_relevant({"title": "Me Min review"}, aliases)
    assert not rel.is_relevant({"title": "Mẹ mìnhh"}, aliases)


async def test_keep_relevant_reads_content_and_needs_two_mentions():
    items = [
        {"title": "Review Mẹ Mìn", "link": "a"},
        {"title": "Phim tuần này", "link": "b"},
        {"title": "Phòng vé", "link": "c"},
        {"title": "Không tải được", "link": "d"},
    ]
    content = {"b": "Phim Mẹ Mìn mở màn tốt. Diễn xuất trong Mẹ Mìn được khen.", "c": "Phim khác thu lớn, vượt cả Mẹ Mìn."}
    read = []

    async def enrich(articles):
        read.extend(a["link"] for a in articles)
        for article in articles:
            if article["link"] in content:
                article["content"] = content[article["link"]]

    kept = await rel.keep_relevant("Mẹ Mìn", items, enrich)
    assert [i["link"] for i in kept] == ["a", "b"]
    assert read == ["b", "c", "d"]  # bài đã khớp ở tiêu đề không cần tải nội dung


def test_franchise_name_alone_does_not_match_a_numbered_part():
    aliases = rel.title_aliases("QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)")
    assert not rel.is_relevant({"title": "(Review) 'Tee Yod: Quỷ ăn tạng 2'"}, aliases)
    assert rel.is_relevant({"title": "Linh Miêu (Quỷ Nhập Tràng)"}, rel.title_aliases("Linh Miêu (Quỷ Nhập Tràng)"))


async def test_articles_long_before_release_are_other_works():
    since = rel.published_since("2026-10-30")
    assert since == "2025-10-30"
    assert rel.published_since(None) is None
    items = [
        {"title": "Huyết Thống cũ", "iso_date": "2025-05-16T00:00:00"},
        {"title": "Huyết Thống mới", "iso_date": "2026-10-01T00:00:00"},
        {"title": "Huyết Thống không rõ ngày", "date": "1 day ago"},
    ]
    kept = await rel.keep_relevant("Huyết Thống", items, since=since)
    assert [i["title"] for i in kept] == ["Huyết Thống mới", "Huyết Thống không rõ ngày"]


def test_short_titles_in_content_must_read_like_a_film_title():
    aliases = rel.title_aliases("Huyết Thống")
    drama = "Tình thân không huyết thống. Họ không cùng huyết thống nhưng thương nhau."
    assert not rel.is_relevant({"title": "Phim truyền hình mới", "content": drama}, aliases)
    assert rel.is_relevant({"title": "Phòng vé", "content": "Phim Huyết Thống ra rạp. Huyết thống thu tốt."}, aliases)
    assert rel.is_relevant({"title": "Phòng vé", "content": "“Huyết Thống” dẫn đầu, huyết thống vượt mốc."}, aliases)
    long_title = rel.title_aliases("Trại Giam Hạnh Phúc")
    assert rel.is_relevant({"title": "x", "content": "trại giam hạnh phúc hay. Trại Giam Hạnh Phúc cảm động."}, long_title)
