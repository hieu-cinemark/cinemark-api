"""Khớp tên phim / từ khoá / hashtag trong nội dung bài (hàm thuần, không chạm D1).

Tách từ posts.py (17/11/2026); posts.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

import re
import unicodedata

from app.core.logging import get_logger

logger = get_logger(__name__)

_HASHTAG_STRIP = re.compile(r"[\s._-]+")


def post_mentions_movie(content: str | None, title: str | None) -> bool:
    """True khi nội dung bài chứa đầy đủ tên phim, hoặc hashtag của tên đó đã bỏ khoảng
    trắng (#AnhHùng cho "Anh Hùng")."""
    if not content or not title:
        return False
    text = content.casefold()
    name = title.strip().casefold()
    if len(name) < 2:
        return False
    if name in text:
        return True
    compact = _HASHTAG_STRIP.sub("", name)
    if len(compact) < 2:
        return False
    compact_text = _HASHTAG_STRIP.sub("", text)
    return f"#{compact}" in compact_text


def _fold_for_keyword_match(text: str) -> str:
    """Chuyển chữ thường, bỏ dấu tiếng Việt, bỏ khoảng trắng - chuyển từ
    foldForKeywordMatch() trong src/lib/keyword-match.ts của cinemark-scraper để bài
    ingest ở đây tính keyword_match giống với các bài khác trong cùng bảng, bất kể nền
    tảng nào crawl."""
    decomposed = unicodedata.normalize("NFD", text)
    without_marks = "".join(c for c in decomposed if not unicodedata.combining(c))
    without_dd = without_marks.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"\s+", "", without_dd.lower())


def _keyword_match_parts(keyword: str) -> list[str]:
    """Từ khoá thường -> một cụm phải xuất hiện đầy đủ; từ khoá nối bằng `+` -> mọi phần
    đều phải xuất hiện (thứ tự nào cũng được) - giống keywordMatchParts() trong
    keyword-match.ts."""
    if "+" in keyword:
        return [part.strip() for part in keyword.split("+") if part.strip()]
    trimmed = keyword.strip()
    return [trimmed] if trimmed else []


def contains_keyword(content: str | None, keyword: str | None) -> bool:
    """Kiểm tra keyword_match theo chuỗi con chính xác. Để public (không chỉ là phương án
    dự phòng riêng của persist_post) để chỗ gọi - xem handle_post trong
    app/workers/ingest_consumer/main.py - có thể kiểm tra lối khớp rẻ/miễn phí này trước
    và chỉ tốn một lời gọi AI (xem app/ai/tasks/post_relevance.py) cho những bài nó
    thực sự bỏ sót, thay vì phân loại mọi bài bất kể kiểm tra miễn phí đã khớp hay chưa."""
    if not content or not keyword:
        return False
    haystack = _fold_for_keyword_match(content)
    parts = _keyword_match_parts(keyword)
    if not parts:
        return False
    for part in parts:
        needle = _fold_for_keyword_match(part)
        if not needle or needle not in haystack:
            return False
    return True


_HASHTAG_TOKEN_RE = re.compile(r"#(\w+)", re.UNICODE)
_VIETNAMESE_COMBINING_MARKS = ("̛", "̣", "̉")  # dấu móc (ư/ơ), dấu nặng, dấu hỏi


def _tag_form(text: str) -> str:
    """Dạng đã fold (xem _fold_for_keyword_match) và bỏ mọi ký tự không phải chữ/số - đúng
    dạng của một token hashtag."""
    return re.sub(r"[\W_]+", "", _fold_for_keyword_match(text))


def _looks_vietnamese(text: str) -> bool:
    """Tín hiệu ngôn ngữ rẻ tiền, không phải bộ nhận diện thật: đ, cộng với dấu móc kết hợp
    (ư/ơ) và dấu nặng/dấu hỏi (đã tách theo NFD), trên thực tế gần như chỉ có ở tiếng
    Việt trong các ngôn ngữ thực sự xuất hiện trong dữ liệu crawl này - dấu huyền/sắc/ngã
    đứng riêng thì trùng với tiếng Tây Ban Nha/Pháp/Bồ Đào Nha nên không dùng riêng ở
    đây. Chỉ dùng làm tiêu chí phân xử cho tín hiệu yếu hơn của movie_hashtag_present
    (token hashtag khớp chính xác nhưng ngắn/mơ hồ, không có tên phim nguyên văn ở đâu
    cả) - một câu tiếng Việt thật có độ dài bình thường gần như luôn có ít nhất một dấu
    này; nội dung tiếng nước ngoài thật sự không liên quan mà tình cờ dùng chung hashtag
    ngắn đó thì không."""
    if "đ" in text.lower():
        return True
    decomposed = unicodedata.normalize("NFD", text)
    return any(mark in decomposed for mark in _VIETNAMESE_COMBINING_MARKS)


def movie_hashtag_present(
    content: str | None, movie_title: str | None, keyword: str | None, *, is_reputable_author: bool = False
) -> bool:
    """Bản chặt hơn của post_mentions_movie/contains_keyword, cho các chỗ gọi chỉ muốn tin
    relevance_label='related' (một phán quyết của AI hoặc chuỗi con - xem docstring của
    persist_post: keyword_match chỉ là bản sao của relevance_label mỗi khi AI thực sự
    được gọi, KHÔNG phải bằng chứng củng cố độc lập) khi chính nội dung bài cũng nêu tên
    phim này một cách độc lập. Ba tín hiệu, mạnh nhất trước:

    1. Dạng hashtag bỏ khoảng trắng của tên phim (ví dụ "#HoangHauCuoiCung" cho "Hoàng
       Hậu Cuối Cùng") xuất hiện nguyên văn - hashtag có chủ đích TOÀN BỘ tên phim, được
       tin bất kể ai đăng.

    2. Một TOKEN hashtag nguyên vẹn có dạng fold (xem _fold_for_keyword_match - không
       phân biệt dấu/hoa thường/khoảng trắng) bằng đúng tên phim hoặc từ khoá đã cấu hình
       (cũng ở dạng fold). Cố ý so sánh nguyên token, không chứa chuỗi con như
       contains_keyword: một từ khoá ngắn/chung chung sau khi fold (ví dụ "Mẹ Mìn" thành
       "memin") có thể tình cờ là CHUỖI CON của một hashtag dài hơn hoàn toàn không liên
       quan (đã xác nhận thực tế các bài
       "#botanasmemin"/"#echatelabotanaconbotanasmemin" của một hãng snack Mexico đạt
       99%+ "related" chỉ vì trùng hợp đó). Vẫn cần thêm _looks_vietnamese(content):
       "#memin" CŨNG là một biệt danh/hình ảnh văn hoá có sẵn trong tiếng Tây Ban Nha
       (một nhân vật truyện tranh Mexico kinh điển) dùng đúng token đó, chứ không chỉ là
       chuỗi con - xem docstring của hàm đó.

    3. Tên phim nguyên văn xuất hiện ở bất cứ đâu trong văn bản tự do, không có hashtag
       nào hỗ trợ - đáng tin với tên phim tự đặt (gần như không bao giờ tình cờ xuất
       hiện), không đáng tin khi tên phim cũng là từ vựng thông thường. Đã xác nhận thực
       tế "Huyết Thống" - nghĩa đen là quan hệ máu mủ - khớp với một bài về
       mâu thuẫn gia đình không liên quan của một người lạ trên Threads với độ tin
       "related" 99,9% từ CẢ kiểm tra chuỗi con lẫn bộ phân loại AI, vì cụm từ trơ trọi
       đó tự nó không mang tín hiệu riêng nào về phim. Chỉ được tin khi
       is_reputable_author là True (xem reputable_authors) - tài khoản có thành tích độc
       lập về nội dung phim thật trên cả *các* phim khác, qua tín hiệu 1/2 ở trên, không
       bao giờ qua chính tín hiệu yếu này (không có vòng lặp). Một tài khoản lần đầu/chỉ
       đăng một lần mà có đúng khẳng định này thì tự nó chưa đủ bằng chứng.

    Đánh đổi, chấp nhận có chủ đích (xem docstring của các chỗ gọi): một bài thật sự
    liên quan mà không dùng tên phim nguyên văn, không có hashtag bằng tên phim, cũng
    không đến từ tài khoản có uy tín - chỉ có tên diễn viên, một tag ghép như
    "#PhimMeMin", hoặc biệt danh - cũng sẽ không qua được. Đó chính là mục đích với các
    chỗ gọi dùng hàm này (danh sách "top 100", lượt quét comment hằng ngày và tạo
    report) - ít kết quả hơn nhưng đúng chủ đề hơn, thay vì cố lấy cho đủ."""
    if not content:
        return False
    text = content.casefold()
    name = (movie_title or "").strip().casefold()

    if len(name) >= 2:
        compact = _HASHTAG_STRIP.sub("", name)
        if len(compact) >= 2 and f"#{compact}" in _HASHTAG_STRIP.sub("", text):
            return True

    # Dùng _tag_form, không dùng _fold_for_keyword_match trần: token hashtag được tách ra
    # không có "#" và không chứa dấu câu, nên đích so sánh cũng phải bỏ cả hai - nếu
    # không, từ khoá dạng hashtag ("#ScottyGiaiCuuHoangThuong" -> "#scotty...") hoặc tên
    # phim có dấu câu ("SCOTTY: GIẢI CỨU..." -> "scotty:...") sẽ không bao giờ khớp.
    folded_targets = {_tag_form(t) for t in (movie_title, keyword) if t}
    folded_targets.discard("")
    if folded_targets:
        tags = {_tag_form(tag) for tag in _HASHTAG_TOKEN_RE.findall(content)}
        if (tags & folded_targets) and _looks_vietnamese(content):
            return True

    if len(name) >= 2 and name in text:
        return is_reputable_author

    return False
