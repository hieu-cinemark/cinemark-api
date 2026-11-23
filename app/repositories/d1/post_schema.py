"""Cột giọng điệu và chỉ mục của bảng posts: tạo cột còn thiếu và các chỉ mục tab lọc, chạy một lần mỗi tiến trình."""

from __future__ import annotations

import asyncio

from app.clients.d1 import d1_query

# --- cột "tiếng nói" của bài + trạng thái quét comment -----------------------
# Nội dung bài cũng là tiếng nói khán giả (trên Threads phần lớn cảm nhận nằm ở chính bài, ít
# comment), nên bài được gắn nhãn cảm xúc/khía cạnh/giai đoạn y như comment (sentiment_sweep.classify_pending_posts).
# comments_crawled_at / comments_crawled_replies / comments_crawl_reason: lần cuối crawl comment xong thật cho bài,
# reply_count lúc xếp hàng và lý do (hot/sample) - để app/services/crawl/comment_planner.py biết bài nào cần quét lại.
# comments_queued_at / comments_queued_replies / comments_queued_run_id: lease đang xếp hàng, ghi TRƯỚC khi publish
# và chỉ nhả bởi tín hiệu kết thúc của đúng run_id đó.
POST_VOICE_COLUMNS = (
    ("sentiment", "TEXT"),
    ("aspects", "TEXT"),
    ("audience_stage", "TEXT"),
    ("insights_classified_at", "TEXT"),
    ("comments_crawled_at", "TEXT"),
    ("comments_crawled_replies", "INTEGER"),
    ("comments_crawl_reason", "TEXT"),
    ("comments_queued_at", "TEXT"),
    ("comments_queued_replies", "INTEGER"),
    ("comments_queued_run_id", "TEXT"),
    # Lần đầu bài vào D1 (scraped_at bị ghi đè mỗi lần quét lại) - số đo Social Topic tách phần về trễ.
    ("first_seen_at", "TEXT"),
    # sentiment_sweep._save ghi cột này cho cả comments lẫn posts; posts thiếu nó nên mọi lần lưu cảm xúc bài đều lỗi
    # "no such column" (relabel + lượt quét 48h lặp lại cùng bài và gọi lại Bee mãi).
    ("sentiment_classified_at", "TEXT"),
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


async def ensure_post_indexes() -> None:
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
