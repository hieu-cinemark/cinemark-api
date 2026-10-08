"""Mọi thứ đọc/ghi bảng `posts` của D1 (+ bảng anh em post_engagement_snapshots) -
chính các dòng mà Worker của cinemark-scraper sở hữu (xem api/schema/scraper.ts bên
đó). Tách ra từ khối d1.py lớn cũ trong app/services để các mẫu query, phân trang và
index của bảng này nằm một chỗ thay vì trộn lẫn với movies/keywords/comments.

app/services/d1.py vẫn re-export mọi tên bên dưới (persist_post, list_posts, ...)
để các chỗ gọi `from app.services.d1 import persist_post` hiện có (ingest_consumer,
scripts, tests) không phải sửa - chỉ code mới mới cần dùng thẳng `post_repo`.

Bảng `dropped_posts` cũ từng nằm trong repo này (xem lịch sử git) để lưu payload
Kafka thô bị loại lúc ingest trước khi tới persist_post (mapper chưa đăng ký,
keyword_id thiếu/không biết). Giờ ingest_consumer ghi các bài bị loại đó lên topic
Kafka `ingest_decisions` (xem app/clients/kafka.py:publish_ingest_decision), và lake
writer (app/workers/lake_writer/main.py) lưu toàn bộ luồng đó lên R2 dưới
bronze/entity=decisions/. Muốn phát lại một bài bị loại trong quá khứ thì lấy từ đó,
không phải từ D1."""

from __future__ import annotations

import asyncio
import json
import re
import time
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

from app.clients.d1 import _configured, d1_query
from app.core.logging import get_logger
from app.services.platforms import PostDraft, registered_platforms

logger = get_logger(__name__)

ENGAGEMENT_FIELDS = ("like_count", "reply_count", "repost_count", "quote_count", "reshare_count", "view_count")

# Dưới số ký tự này (sau khi trim), nội dung bài bị coi là rác - một reaction/emoji/
# comment một chữ trơ trọi, không có gì để phân tích. Chỉnh thoải mái; đây là con số
# cảm tính, không suy ra từ đâu cả.
MIN_CONTENT_LENGTH = 10

# Chỉ tính tương tác, cố ý bỏ view_count: lượt xem là hiển thị thụ động, không phải
# "tương tác" - nếu không, số lượt xem lớn sẽ áp đảo hoàn toàn thứ tự sắp xếp, vì nó
# thường lớn gấp 10-100 lần tổng số like/reply/repost/quote/reshare cộng lại, biến
# cách sắp xếp này thành "xem nhiều nhất" khoác nhãn "tương tác".
#
# Mỗi comment (reply_count) nặng gấp COMMENT_WEIGHT lần một like/share (2026-10-07): bài nhiều comment
# nhưng ít like trước đây rơi khỏi top 100, nên lượt lấy comment hằng ngày (list_posts_needing_comments)
# bỏ qua đúng những bài có nhiều ý kiến khán giả nhất. Mô phỏng trên D1 thật: với hệ số 3, top 100 của các
# từ khoá Facebook có thêm ~13.300 comment, đổi lại mất ~1.400. Danh sách top 100 trên dashboard dùng
# chung công thức này để thứ tự hiển thị khớp với thứ tự crawl.
COMMENT_WEIGHT = 3
ENGAGEMENT_SCORE_SQL = (
    f"(p.like_count + {COMMENT_WEIGHT} * p.reply_count + p.repost_count + p.quote_count + p.reshare_count)"
)

# Bỏ khoảng trắng / dấu câu để "#Anh Hùng" và "#AnhHung" đều khớp tên phim "Anh Hùng".
_HASHTAG_STRIP = re.compile(r"[\s._-]+")

# "Bài này được tính là nói về phim của nó" cho các query report/danh sách top/quét
# comment. relevance_label chỉ được ghi khi bộ phân loại thực sự chạy: bài được nhận
# qua lối tắt chuỗi con lúc ingest (nội dung chứa từ khoá, ví dụ mọi bài tìm được qua
# chính hashtag của phim) giữ nhãn NULL với keyword_match=1. Chỉ đòi
# relevance_label='related' thì đã âm thầm loại hết những bài đó - đã xác nhận
# 2026-09-28 với "SCOTTY: GIẢI CỨU HOÀNG THƯỢNG": 115 comment đã phân loại cảm xúc,
# tất cả đều nằm dưới bài có nhãn NULL, nên report của phim báo "không đủ comment".
# Nhãn mà bộ phân loại hoặc quy tắc đã ghi (not_related/uncertain) vẫn được ưu tiên.
RELEVANT_POST_SQL = "(p.relevance_label = 'related' OR (p.relevance_label IS NULL AND p.keyword_match > 0))"


# --- cột "tiếng nói" của bài + trạng thái quét comment -----------------------
# Thêm 2026-10-08. Nội dung bài cũng là tiếng nói khán giả (trên Threads phần lớn cảm nhận nằm ở chính bài, ít
# comment), nên bài được gắn nhãn cảm xúc/khía cạnh/giai đoạn y như comment (sentiment_sweep.classify_pending_posts).
# comments_crawled_at / comments_crawled_replies / comments_crawl_reason: lần cuối xếp hàng crawl comment cho bài,
# reply_count lúc đó và lý do (hot/sample) - để app/services/comment_planner.py biết bài nào cần quét lại.
POST_VOICE_COLUMNS = (
    ("sentiment", "TEXT"),
    ("aspects", "TEXT"),
    ("audience_stage", "TEXT"),
    ("insights_classified_at", "TEXT"),
    ("comments_crawled_at", "TEXT"),
    ("comments_crawled_replies", "INTEGER"),
    ("comments_crawl_reason", "TEXT"),
)
_post_voice_columns_ready = False
_post_voice_columns_lock = asyncio.Lock()


async def ensure_post_voice_columns() -> None:
    """Thêm các cột POST_VOICE_COLUMNS còn thiếu, một lần mỗi tiến trình (PRAGMA trước, không ALTER khi đã có)."""
    global _post_voice_columns_ready
    if _post_voice_columns_ready:
        return
    async with _post_voice_columns_lock:
        if _post_voice_columns_ready:
            return
        cols = await d1_query("PRAGMA table_info(posts)")
        existing = {row.get("name") for row in cols or []}
        for column, kind in POST_VOICE_COLUMNS:
            if column not in existing:
                await d1_query(f"ALTER TABLE posts ADD COLUMN {column} {kind}", quiet=True)
        _post_voice_columns_ready = True


# --- index ---------------------------------------------------------------
# Mỗi index bên dưới tồn tại vì có một query thật trong file này (hoặc phần kiểm tra
# upsert persist_post của ingest_consumer) lọc/sắp xếp đúng trên các cột đó - xem từng
# comment để biết là query nào. Không có cái nào là phỏng đoán: tới 2026-09-21 bảng
# này không có index nào ngoài khoá chính `id` (đã kiểm tra bằng PRAGMA index_list
# trên bản sao local), nghĩa là trước đó mọi query này đều quét toàn bảng.
_POST_INDEXES = (
    # /stats/posts không lọc nền tảng (tab mặc định "All platforms" của dashboard - xem
    # PostsReview.tsx của spider-hub-dashboard) sắp xếp theo scraped_at mà không có WHERE
    # nào - index (platform, scraped_at) bên dưới không phục vụ được (các dòng không được
    # sắp theo scraped_at trên toàn cục trừ khi cố định cả platform), nên index đơn giản
    # này phủ trường hợp đó.
    "CREATE INDEX IF NOT EXISTS idx_posts_scraped_at ON posts(scraped_at DESC)",
    # /stats/posts?platform=X (một tab nền tảng) - WHERE platform = ? ORDER BY scraped_at
    # DESC của list_posts.
    "CREATE INDEX IF NOT EXISTS idx_posts_platform_scraped_at ON posts(platform, scraped_at DESC)",
    # Top bài theo từ khoá (TopPostsModal / useTopPostsByKeyword) và lượt quét top comment
    # hằng ngày (list_posts_needing_comments) đều chỉ lọc theo keyword_id (một từ khoá đã
    # ngầm thuộc một nền tảng, nên một cột này đủ chọn lọc mà không cần index ghép).
    "CREATE INDEX IF NOT EXISTS idx_posts_keyword_id ON posts(keyword_id)",
    # Top bài theo phim (TopPostsModal / useTopPostsByMovie) chỉ lọc theo movie_id, cùng
    # lý do.
    "CREATE INDEX IF NOT EXISTS idx_posts_movie_id ON posts(movie_id)",
    # Phần kiểm tra upsert của persist_post (SELECT ... WHERE platform = ? AND
    # external_id = ?) và get_post_by_external_id chạy trên *từng* bài được ingest - query
    # nóng nhất trên bảng này, bỏ xa các query khác. Không UNIQUE: bản sao local đã có sẵn
    # vài dòng trùng (platform, external_id) từ trước khi có index này (do race trong
    # persist_post - xem docstring của hàm đó vốn giả định có một unique index chưa bao
    # giờ thực sự tồn tại) - thêm UNIQUE bây giờ sẽ tạo không thành. Khử trùng những dòng
    # đó là một quyết định riêng, có chủ đích, không gộp vào một lần migrate index.
    "CREATE INDEX IF NOT EXISTS idx_posts_platform_external_id ON posts(platform, external_id)",
)

# Dựng một lần ở nền (xem ensure_tab_filter_indexes) - không bao giờ từ list_posts.
# CREATE INDEX trên bảng posts đang chạy qua D1 HTTP có thể vượt timeout query 10s và,
# trong lúc chạy, bỏ đói mọi query dashboard khác (các lần chuyển tab bị lỗi được log
# là d1_request_failed).
_TAB_FILTER_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_posts_keyword_match_scraped_at ON posts(keyword_match, scraped_at DESC)",
    # /api/social/heat và /api/comparison/* của Worker cinemark đều lọc bài theo
    # movie_id IN (...) VÀ khoảng posted_at VÀ keyword_match > 0. Chỉ với index một cột
    # movie_id thì chúng đọc mọi bài của phim (22.932 dòng để trả về 825 với phim lớn
    # nhất, đo 2026-09-28). Index một phần trên keyword_match > 0 (khoảng 23% số bài) để
    # index chỉ chứa những dòng các query đó có thể trả về; SQLite chỉ dùng nó khi query
    # lặp lại đúng điều kiện đó.
    "CREATE INDEX IF NOT EXISTS idx_posts_movie_matched_posted_at ON posts(movie_id, posted_at) WHERE keyword_match > 0",
)

_post_indexes_ready = False
_post_indexes_lock = asyncio.Lock()


async def _ensure_post_indexes() -> None:
    global _post_indexes_ready
    if _post_indexes_ready:
        return
    async with _post_indexes_lock:
        if _post_indexes_ready:
            return
        for sql in _POST_INDEXES:
            await d1_query(sql, quiet=True)
        _post_indexes_ready = True


async def ensure_tab_filter_indexes() -> None:
    """CREATE INDEX cho các tab related/unrelated của PostsReview. Gọi từ lúc app khởi động
    dưới dạng task nền là an toàn - IF NOT EXISTS, timeout 90s."""
    for sql in _TAB_FILTER_INDEXES:
        await d1_query(sql, quiet=True, timeout=90.0)


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
    và chỉ tốn một lời gọi Kira (xem app/ai/tasks/post_relevance.py) cho những bài nó
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


# --- uy tín tác giả --------------------------------------------------------
# Củng cố tín hiệu yếu nhất của movie_hashtag_present (chỉ khớp tên phim nguyên văn,
# không có hashtag nào hỗ trợ) cho phim có tên cũng là từ vựng thông thường - xem
# docstring của hàm đó về sự cố "Huyết Thống" mà phần này sinh ra để xử lý. Uy tín của
# một tác giả được dựng bởi scripts/build_author_reputation.py bằng cách kiểm tra lại
# các bài relevance_label='related' trước đây của chính họ chỉ với các tín hiệu MẠNH
# của movie_hashtag_present (is_reputable_author mặc định False - xem tham số đó) -
# không bao giờ dùng chính tín hiệu yếu mà bảng này dùng để củng cố, nên không có vòng
# lặp: uy tín chỉ có được qua bằng chứng hashtag.

MIN_MOVIES_FOR_REPUTABLE_AUTHOR = 2
_REPUTABLE_AUTHORS_TTL_SECONDS = 300.0

_author_reputation_ready = False
_reputable_authors_cache: tuple[float, set[tuple[str, str]]] | None = None


async def ensure_author_reputation_table() -> None:
    global _author_reputation_ready
    if _author_reputation_ready:
        return
    await d1_query(
        """
        CREATE TABLE IF NOT EXISTS author_reputation (
            platform text NOT NULL,
            author text NOT NULL,
            distinct_movies integer NOT NULL DEFAULT 0,
            confirmed_posts integer NOT NULL DEFAULT 0,
            updated_at text NOT NULL,
            PRIMARY KEY (platform, author)
        )
        """,
        quiet=True,
    )
    _author_reputation_ready = True


async def reputable_authors() -> set[tuple[str, str]]:
    """Tập {(platform, author)} đã được xác nhận trên >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR
    phim khác nhau - cache trong tiến trình _REPUTABLE_AUTHORS_TTL_SECONDS giây (bảng
    này chỉ đổi khi có người chạy lại script dựng, không đổi giữa các request, nên cache
    ngắn giúp tránh thêm một lượt gọi D1 mỗi lần). Chỗ gọi kiểm tra
    `(platform, author) in reputable_authors()` rồi truyền kết quả làm
    is_reputable_author của movie_hashtag_present."""
    global _reputable_authors_cache
    now = time.monotonic()
    if _reputable_authors_cache is not None and now - _reputable_authors_cache[0] < _REPUTABLE_AUTHORS_TTL_SECONDS:
        return _reputable_authors_cache[1]
    await ensure_author_reputation_table()
    rows = await d1_query(
        "SELECT platform, author FROM author_reputation WHERE distinct_movies >= ?",
        [MIN_MOVIES_FOR_REPUTABLE_AUTHOR],
    )
    result = {(r["platform"], r["author"]) for r in (rows or [])}
    _reputable_authors_cache = (now, result)
    return result


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
       quan (đã xác nhận thực tế 2026-09-24: các bài
       "#botanasmemin"/"#echatelabotanaconbotanasmemin" của một hãng snack Mexico đạt
       99%+ "related" chỉ vì trùng hợp đó). Vẫn cần thêm _looks_vietnamese(content):
       "#memin" CŨNG là một biệt danh/hình ảnh văn hoá có sẵn trong tiếng Tây Ban Nha
       (một nhân vật truyện tranh Mexico kinh điển) dùng đúng token đó, chứ không chỉ là
       chuỗi con - xem docstring của hàm đó.

    3. Tên phim nguyên văn xuất hiện ở bất cứ đâu trong văn bản tự do, không có hashtag
       nào hỗ trợ - đáng tin với tên phim tự đặt (gần như không bao giờ tình cờ xuất
       hiện), không đáng tin khi tên phim cũng là từ vựng thông thường. Đã xác nhận thực
       tế 2026-09-25: "Huyết Thống" - nghĩa đen là quan hệ máu mủ - khớp với một bài về
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


# Video phát được / permalink không đặt vào <img> được. Các dòng TikTok cũ lưu playAddr
# làm media_url và chỉ để cover_url trong raw_json.
_VIDEO_URL_HINTS = (
    ".mp4",
    ".m3u8",
    "/video/tos/",
    "webapp-prime.tiktok.com",
    "facebook.com/reel/",
    "facebook.com/watch",
    "facebook.com/share/v",
    "facebook.com/video",
    "tiktok.com/@",
    "threads.com/@",
    "threads.net/@",
)
_IMAGE_URL_HINTS = (
    "fbcdn.net",
    "cdninstagram.com",
    "tiktokcdn",
    "byteicdn",
    "ibyteimg",
    "byteimg.com",
    "muscdn.com",
    "scontent",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
)


def _is_preview_image_url(url: Any) -> bool:
    if not isinstance(url, str) or not url.startswith("http"):
        return False
    lower = url.lower()
    if any(hint in lower for hint in _VIDEO_URL_HINTS):
        return False
    return any(hint in lower for hint in _IMAGE_URL_HINTS)


def _hydrate_post_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out = rows or []
    for row in out:
        media = json.loads(row.pop("media_json") or "{}")
        raw_cover = row.pop("raw_cover_url", None)
        row["media_type"] = media.get("media_type")
        candidates = (
            media.get("cover_url"),
            raw_cover,
            media.get("thumbnail_url"),
            media.get("media_url"),
        )
        row["media_url"] = next((url for url in candidates if _is_preview_image_url(url)), None)
        quoted = media.get("quoted") if isinstance(media.get("quoted"), dict) else None
        row["quoted"] = (
            {
                "author": quoted.get("author"),
                "content": quoted.get("content"),
                "url": quoted.get("url"),
                "media_url": quoted.get("media_url") if _is_preview_image_url(quoted.get("media_url")) else None,
            }
            if quoted
            else None
        )
    return out


class PostRepository:
    """Giữ mọi query trên `posts`. Một instance cho cả tiến trình (`post_repo` bên dưới) -
    không có trạng thái theo request, nên đây chỉ là namespace cho các query cộng với cờ
    chặn tạo index lười ở trên."""

    async def list_posts(
        self,
        *,
        platform: str | None = None,
        keyword_id: str | None = None,
        movie_id: str | None = None,
        keyword_match: bool | None = None,
        sort: str = "recent",
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Feed bài phân trang, join với phim/từ khoá để hiển thị - phục vụ tab "xem lại bài"
        của dashboard. Mọi bộ lọc đều không bắt buộc và cộng dồn; không truyền gì thì trả về
        trang mới nhất của cả bảng trên mọi nền tảng/phim. Trả về (rows, total_count) để chỗ
        gọi hiển thị phân trang mà không cần gọi thêm lượt nữa.

        Phân trang offset, không phải keyset: xem list_posts_cursor bên dưới để biết vì sao
        hàm này hiện vẫn là API đang dùng dù đã có method kia.

        sort="recent" (mặc định): crawl gần nhất trước, như trước giờ. sort="engagement":
        tương tác cao nhất trước (xem ENGAGEMENT_SCORE_SQL) - ví dụ keyword_id +
        sort="engagement" + limit=100 là màn hình "top 100 bài của từ khoá này" trên
        dashboard. Màn hình đó đòi relevance_label='related' (đặt lúc ingest, xem
        app/ai/tasks/post_relevance.py) VÀ kiểm tra độc lập của movie_hashtag_present -
        riêng relevance_label không phải ý kiến thứ hai như vẻ ngoài: persist_post lưu
        keyword_match như bản sao y nguyên phán quyết của AI mỗi khi AI thực sự được gọi (xem
        docstring của nó), nên chỉ dựa vào relevance_label thực chất là tin AI một lần, không
        phải hai lần. Đã xác nhận thực tế 2026-09-24: các bài của một hãng snack Mexico đạt
        99%+ "related" với phim "Mẹ Mìn" chỉ vì hashtag của nó fold ra cùng một chuỗi ngắn
        với tên phim - xem docstring của movie_hashtag_present cho cách sửa. Lấy dư
        (_ENGAGEMENT_OVERFETCH bên dưới) vì bộ lọc thứ hai này chạy bằng Python, rồi cắt lại
        còn `limit` - bài chưa được AI gắn nhãn, hoặc không có hashtag/tên phim khớp nào, sẽ
        không chiếm chỗ dù tương tác cao."""
        await _ensure_post_indexes()
        where = []
        params: list[Any] = []
        if platform:
            where.append("p.platform = ?")
            params.append(platform)
        if keyword_id:
            where.append("p.keyword_id = ?")
            params.append(keyword_id)
        if movie_id:
            where.append("p.movie_id = ?")
            params.append(movie_id)
        if keyword_match is not None:
            where.append("p.keyword_match = ?")
            params.append(1 if keyword_match else 0)
        if sort == "engagement":
            where.append(RELEVANT_POST_SQL)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        order_sql = f"{ENGAGEMENT_SCORE_SQL} DESC" if sort == "engagement" else "p.scraped_at DESC"
        # movie_hashtag_present chạy bằng Python sau khi lấy dữ liệu và loại bỏ một số dòng mà
        # riêng relevance_label='related' lẽ ra đã cho qua - nên lấy nhiều hơn `limit` ngay từ
        # đầu để sau khi lọc rồi cắt lại thì trang không bị thiếu/rỗng.
        sql_limit = max(limit * 4, limit + 100) if sort == "engagement" else limit

        rows = await d1_query(
            f"""
            SELECT
                p.id, p.platform, p.external_id, p.url, p.author, p.content, p.media_json,
                json_extract(p.raw_json, '$.cover_url') AS raw_cover_url,
                p.like_count, p.reply_count, p.repost_count, p.quote_count, p.reshare_count, p.view_count,
                p.posted_at, p.scraped_at, p.keyword_match,
                k.keyword, m.title AS movie_title
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, sql_limit, offset],
            timeout=20.0 if keyword_match is not None else 10.0,
        )
        hydrated = _hydrate_post_rows(rows)
        if sort == "engagement":
            reputable = await reputable_authors()
            hydrated = [
                row
                for row in hydrated
                if movie_hashtag_present(
                    row.get("content"),
                    row.get("movie_title"),
                    row.get("keyword"),
                    is_reputable_author=(row.get("platform"), row.get("author")) in reputable,
                )
            ][:limit]
            return hydrated, len(hydrated)

        # Tab có lọc: bỏ COUNT(*) - nó quét toàn bảng cho tới khi có index keyword_match, và
        # đã làm D1 timeout (~10s) mỗi lần bấm tab. Ước lượng "còn trang sau" từ kích thước
        # trang thay thế.
        if keyword_match is None:
            count_rows = await d1_query(f"SELECT COUNT(*) AS total FROM posts p {where_sql}", params)
            total = (count_rows[0]["total"] if count_rows else 0) or 0
        else:
            n = len(hydrated)
            total = offset + n + (limit if n == limit else 0)
        return hydrated, total

    async def list_posts_cursor(
        self,
        *,
        platform: str | None = None,
        keyword_id: str | None = None,
        movie_id: str | None = None,
        keyword_match: bool | None = None,
        sort: str = "recent",
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Bản phân trang keyset tương đương list_posts, chỉ cho sort="recent" (biểu thức sắp
        xếp theo tương tác không dùng làm cột keyset được nếu không có một bản sao
        generated/có index của nó - chưa đáng thêm cho tới khi method này thực sự gánh tải).
        `cursor` là chuỗi "<scraped_at>|<id>" không cần hiểu bên trong của dòng cuối trang
        trước; None là bắt đầu từ đầu. Không có tổng số - phân trang keyset đánh đổi "trang
        N trên M" (và, không phải ngẫu nhiên, cả con số tổng ước lượng/đôi khi sai mà nhánh
        keyword_match của list_posts báo - xem comment của nó) lấy chi phí O(limit) mỗi trang
        bất kể đi sâu tới đâu, và đó chính là mục đích.

        Giờ đã nối vào GET /stats/posts (xem stats.py) - OFFSET của list_posts, đã xác nhận
        thực tế khi số bài vượt khoảng 100 nghìn, *chậm dần theo độ sâu* đặc biệt khi đi kèm
        LEFT JOIN keyword/movie (bỏ JOIN thì đều khoảng 0,1s bất kể offset, so với tới vài
        giây ở offset 90 nghìn khi có JOIN) - D1 không đẩy LIMIT/OFFSET xuống dưới phép join.
        Keyset né hoàn toàn chuyện này: điều kiện WHERE giới hạn phạm vi quét trước khi join
        chạy, nên `cursor` trỏ sâu tới đâu cũng không sao."""
        if sort != "recent":
            raise ValueError("list_posts_cursor only supports sort='recent'")
        await _ensure_post_indexes()

        where = []
        params: list[Any] = []
        if platform:
            where.append("p.platform = ?")
            params.append(platform)
        if keyword_id:
            where.append("p.keyword_id = ?")
            params.append(keyword_id)
        if movie_id:
            where.append("p.movie_id = ?")
            params.append(movie_id)
        if keyword_match is not None:
            where.append("p.keyword_match = ?")
            params.append(1 if keyword_match else 0)
        if cursor:
            cursor_scraped_at, _, cursor_id = cursor.partition("|")
            where.append("(p.scraped_at < ? OR (p.scraped_at = ? AND p.id < ?))")
            params += [cursor_scraped_at, cursor_scraped_at, cursor_id]
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        rows = await d1_query(
            f"""
            SELECT
                p.id, p.platform, p.external_id, p.url, p.author, p.content, p.media_json,
                json_extract(p.raw_json, '$.cover_url') AS raw_cover_url,
                p.like_count, p.reply_count, p.repost_count, p.quote_count, p.reshare_count, p.view_count,
                p.posted_at, p.scraped_at, p.keyword_match,
                k.keyword, m.title AS movie_title
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            {where_sql}
            ORDER BY p.scraped_at DESC, p.id DESC
            LIMIT ?
            """,
            [*params, limit],
            timeout=20.0 if keyword_match is not None else 10.0,
        )
        hydrated = _hydrate_post_rows(rows)
        next_cursor = f"{hydrated[-1]['scraped_at']}|{hydrated[-1]['id']}" if len(hydrated) == limit else None
        return hydrated, next_cursor

    async def list_posts_needing_comments(
        self, *, platform: str, keyword_id: str, top_n: int = 100
    ) -> list[dict[str, Any]]:
        """Trong top `top_n` bài theo tương tác của từ khoá này (đúng cùng cách xếp hạng với
        list_posts(sort="engagement") ở trên), những bài chưa có comment nào được lưu - phục
        vụ lượt quét "top comments" hằng ngày (xem _top_comments_tick trong scheduler.py).
        Xếp hạng trước, *rồi* mới lọc bài không có comment (một CTE, không phải một WHERE
        duy nhất) - lọc trước sẽ để bài hạng 101/150 không có comment chen mất chỗ của một
        bài thật sự thuộc top 100 chỉ vì nó đã có vài comment, như vậy không còn là "trong
        top 100, bài nào vẫn cần comment" nữa.

        Bài đã được quét ở một ngày trước không bị xếp hàng lại chỉ vì vẫn nằm trong top
        100 của từ khoá - chỉ những bài mới vào top (hoặc lần đầu chưa lấy được comment)
        mới được xếp hàng - nên một từ khoá có top 100 gần như không đổi qua các ngày sẽ
        không tốn lại pool tài khoản/proxy cho những bài đã lấy comment rồi. Cùng cổng
        relevance_label='related' + movie_hashtag_present như danh sách top 100 trên
        dashboard (list_posts sort="engagement") - xem docstring của method đó và của
        movie_hashtag_present để biết vì sao riêng relevance_label không phải bằng chứng
        củng cố độc lập; lưu ý về bài đã được AI gắn nhãn cũng áp dụng (bài chưa được lượt
        chạy theo lô của label_posts_relevance.py quét qua cũng không hiện ở đây)."""
        await _ensure_post_indexes()
        overfetch = max(top_n * 4, top_n + 100)
        rows = await d1_query(
            f"""
            SELECT p.id, p.external_id, p.url, p.content, p.author, k.keyword, m.title AS movie_title,
                   COALESCE(c.n, 0) AS comment_n
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            LEFT JOIN (SELECT post_id, COUNT(*) AS n FROM comments GROUP BY post_id) c ON c.post_id = p.id
            WHERE p.platform = ? AND p.keyword_id = ? AND {RELEVANT_POST_SQL}
            ORDER BY {ENGAGEMENT_SCORE_SQL} DESC
            LIMIT ?
            """,
            [platform, keyword_id, overfetch],
        )
        reputable = await reputable_authors()
        selected: list[dict[str, Any]] = []
        for row in rows or []:
            if row.get("comment_n"):
                continue
            if not movie_hashtag_present(
                row.get("content"),
                row.get("movie_title"),
                row.get("keyword"),
                is_reputable_author=(platform, row.get("author")) in reputable,
            ):
                continue
            selected.append({"id": row["id"], "external_id": row["external_id"], "url": row["url"]})
            if len(selected) >= top_n:
                break
        return selected

    async def get_post_by_external_id(self, platform: str, external_id: str) -> dict[str, Any] | None:
        """Một bài theo (platform, external_id) - id mà payload comment của spider-hub mang theo
        (xem handle_comment trong app/workers/ingest_consumer/main.py), khác với id nội bộ D1
        của get_post."""
        await _ensure_post_indexes()
        rows = await d1_query(
            "SELECT id, platform, external_id, url FROM posts WHERE platform = ? AND external_id = ?",
            [platform, external_id],
        )
        return rows[0] if rows else None

    async def get_post(self, post_id: str) -> dict[str, Any] | None:
        """Một bài theo id D1 (không phải external_id) - dùng cho nút kích hoạt "lấy comment
        cho bài này" (xem app/api/routes/facebook.py) để tra ra id bài của nền tảng + url mà
        bootstrap/spider của spider-hub cần, từ id D1 mà dashboard đang có (xem Post.id
        trong app/schemas/stats.py)."""
        rows = await d1_query("SELECT id, platform, external_id, url FROM posts WHERE id = ?", [post_id])
        return rows[0] if rows else None

    async def persist_post(
        self,
        *,
        movie_id: str,
        keyword_id: str,
        keyword: str,
        platform: str,
        draft: PostDraft,
        ai_relevant: bool | None = None,
        relevance_label: str | None = None,
        relevance_confidence: float | None = None,
    ) -> bool:
        """Upsert một bài đã crawl (của bất kỳ nền tảng đã đăng ký nào) thẳng vào bảng `posts`
        của cinemark-scraper (+ một snapshot tương tác khi có thay đổi) - chuyển từ
        src/jobs/persist-post.ts bên đó để cả scraper riêng của Worker lẫn đường đi qua
        Kafka này ghi qua đúng cùng một logic. `draft` đã được mapper của nền tảng chuẩn hoá
        (xem app/services/platforms.py) - hàm này không tự biết trường nào riêng của nền
        tảng nào.

        `ai_relevant`, khi có (xem app/ai/tasks/post_relevance.py), là phán quyết "related"
        của Kira và được dùng cho keyword_match thay cho kiểm tra chuỗi con chính xác
        contains_keyword bên dưới - chỗ gọi truyền None để quay về kiểm tra chuỗi con (Kira
        không chắc, đang tắt, vượt hạn mức ngày, hoặc lời gọi thất bại) thay vì chặn việc
        ingest chỉ vì bộ phân loại trục trặc.

        `relevance_label`/`relevance_confidence`, khi có, là chính kết quả phân loại đó được
        lưu thẳng lên dòng lúc ingest (hệ 3 nhóm related/not_related/uncertain, không phải
        keyword_match kiểu boolean) - đặt ngay lúc đó, để bài hiện ra trong các màn hình lọc
        theo độ liên quan mà không phải chờ lượt chạy theo lô nào."""
        if not _configured() or platform not in registered_platforms():
            return False
        await _ensure_post_indexes()

        external_id = draft.get("external_id")
        if not external_id:
            return False

        content = draft.get("content")

        # Lọc rác: nội dung quá ngắn (một reaction/emoji trơ trọi không có tín hiệu gì để phân
        # tích) - bỏ qua hẳn. KHÔNG lọc theo keyword_match: người comment thật viết tắt, viết
        # tiếng Việt không dấu, hoặc dùng tên tiếng Anh của phim sẽ không bao giờ chứa nguyên
        # văn cụm từ khoá đã cấu hình, nên chặn việc lưu theo kiểu khớp đó sẽ âm thầm làm mất
        # bài thật. keyword_match vẫn được tính và lưu bên dưới như một cờ để chỗ gọi tự lọc
        # nếu muốn, như trước - chỉ là nó không phải lý do để bỏ hẳn việc lưu bài.
        if not content or len(content.strip()) < MIN_CONTENT_LENGTH:
            logger.info("post_skipped_junk", platform=platform, external_id=external_id, reason="content_too_short")
            # Cố ý bỏ qua, không phải ghi lỗi - trả True để `if not ok` của handle_post không kích
            # hoạt bộ đếm cảnh báo bài bị loại (bộ đếm đó lấy dữ liệu từ luồng Kafka
            # ingest_decisions, không phải D1).
            return True
        is_keyword_match = ai_relevant if ai_relevant is not None else contains_keyword(content, keyword)

        scraped_at = datetime.now(tz=timezone.utc).isoformat()
        media_json = json.dumps(draft.get("media") or {})
        raw_json = json.dumps(draft.get("raw")) if draft.get("raw") is not None else None
        engagement = {field: draft.get(field) or 0 for field in ENGAGEMENT_FIELDS}
        # D1 lưu boolean dưới dạng số nguyên SQLite (0/1) - truyền int, không truyền bool
        # JSON, để HTTP API bind đúng kiểu mà cột integer(..., {mode: "boolean"}) của Drizzle
        # cần. Có thể bằng 0 là hợp lệ - xem docstring của hàm này để biết vì sao bài không
        # khớp vẫn được lưu thay vì bị bỏ qua.
        keyword_match = int(is_keyword_match)

        existing_rows = await d1_query(
            "SELECT id, like_count, reply_count, repost_count, quote_count, reshare_count, view_count "
            "FROM posts WHERE platform = ? AND external_id = ?",
            [platform, external_id],
        )
        existing = existing_rows[0] if existing_rows else None

        if existing is None:
            post_id = f"post_{uuid.uuid4()}"
            relevance_labeled_at = scraped_at if relevance_label is not None else None
            inserted = await d1_query(
                """
                INSERT INTO posts (
                    id, movie_id, keyword_id, platform, external_id, url, author, content, media_json,
                    like_count, reply_count, repost_count, quote_count, reshare_count, view_count,
                    posted_at, scraped_at, raw_json, keyword_match,
                    relevance_label, relevance_confidence, relevance_labeled_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    post_id,
                    movie_id,
                    keyword_id,
                    platform,
                    external_id,
                    draft.get("url"),
                    draft.get("author"),
                    draft.get("content"),
                    media_json,
                    engagement["like_count"],
                    engagement["reply_count"],
                    engagement["repost_count"],
                    engagement["quote_count"],
                    engagement["reshare_count"],
                    engagement["view_count"],
                    draft.get("posted_at"),
                    scraped_at,
                    raw_json,
                    keyword_match,
                    relevance_label,
                    relevance_confidence,
                    relevance_labeled_at,
                ],
            )
            if inserted is None:
                # Insert thất bại (đua với một message khác cùng external_id, D1 sập, ...) - dòng bài
                # không tồn tại, nên một snapshot tham chiếu post_id ở đây sẽ thành mồ côi. Log rồi
                # dừng; lần crawl lại bài này sau sẽ thử lại toàn bộ upsert từ đầu.
                logger.warning("d1_post_insert_failed", platform=platform, external_id=external_id)
                return False
            await d1_query(
                "INSERT INTO post_engagement_snapshots "
                "(id, post_id, recorded_at, like_count, reply_count, repost_count, quote_count, reshare_count, view_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [f"eng_{uuid.uuid4()}", post_id, scraped_at, *engagement.values()],
            )
            from app.services.stats_summary import record_post

            await record_post(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=True)
            return True

        post_id = existing["id"]
        changed = any(existing.get(field) != engagement[field] for field in ENGAGEMENT_FIELDS)

        # relevance_* dùng COALESCE(?, column) thay vì ghi đè thẳng: nhánh này chạy lại mỗi
        # lần crawl lại một bài đã có (chỉ cập nhật tương tác), và nội dung không đổi nghĩa là
        # đi lại đúng đường contains_keyword/phân loại như trước, mà đường đó có thể trả None
        # một cách hợp lệ ở đây (chỉ khớp chuỗi con đã quyết định, không có phán quyết của
        # Kira) - ghi đè thẳng sẽ xoá mất một nhãn thật mà lượt quét theo lô (hoặc lần phân
        # loại lúc ingest trước đó) đã đặt.
        updated = await d1_query(
            """
            UPDATE posts SET
                url = ?, author = ?, content = ?, media_json = ?,
                like_count = ?, reply_count = ?, repost_count = ?, quote_count = ?, reshare_count = ?, view_count = ?,
                posted_at = ?, scraped_at = ?, raw_json = ?, keyword_match = ?,
                relevance_label = COALESCE(?, relevance_label),
                relevance_confidence = COALESCE(?, relevance_confidence),
                relevance_labeled_at = COALESCE(?, relevance_labeled_at)
            WHERE id = ?
            """,
            [
                draft.get("url"),
                draft.get("author"),
                draft.get("content"),
                media_json,
                engagement["like_count"],
                engagement["reply_count"],
                engagement["repost_count"],
                engagement["quote_count"],
                engagement["reshare_count"],
                engagement["view_count"],
                draft.get("posted_at"),
                scraped_at,
                raw_json,
                keyword_match,
                relevance_label,
                relevance_confidence,
                scraped_at if relevance_label is not None else None,
                post_id,
            ],
        )
        if updated is None:
            # UPDATE thất bại (D1 sập, ...) - post_id vẫn trỏ tới một dòng thật đã có từ trước
            # (khác với nhánh insert ở trên), nên không có gì bị mồ côi, nhưng số tương tác bên
            # dưới sẽ phản ánh payload của message này chứ không phải dữ liệu thực sự đang lưu.
            # Bỏ qua snapshot; lần crawl lại sau sẽ thử lại toàn bộ upsert.
            logger.warning("d1_post_update_failed", platform=platform, external_id=external_id, post_id=post_id)
            return False
        if changed:
            await d1_query(
                "INSERT INTO post_engagement_snapshots "
                "(id, post_id, recorded_at, like_count, reply_count, repost_count, quote_count, reshare_count, view_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [f"eng_{uuid.uuid4()}", post_id, scraped_at, *engagement.values()],
            )
        from app.services.stats_summary import record_post

        await record_post(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=False)
        return True


post_repo = PostRepository()


# --- các hàm tự do để tương thích ngược (xem docstring module) -----------


async def list_posts(**kwargs: Any) -> tuple[list[dict[str, Any]], int]:
    return await post_repo.list_posts(**kwargs)


async def list_posts_needing_comments(**kwargs: Any) -> list[dict[str, Any]]:
    return await post_repo.list_posts_needing_comments(**kwargs)


async def get_post_by_external_id(platform: str, external_id: str) -> dict[str, Any] | None:
    return await post_repo.get_post_by_external_id(platform, external_id)


async def get_post(post_id: str) -> dict[str, Any] | None:
    return await post_repo.get_post(post_id)


async def persist_post(**kwargs: Any) -> bool:
    return await post_repo.persist_post(**kwargs)
