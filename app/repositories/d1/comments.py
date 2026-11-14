"""Mọi thứ đọc/ghi bảng `comments` của D1 - xem docstring module của
app/repositories/d1/posts.py để biết vì sao bảng này được tách riêng và vì sao
app/repositories/d1/store.py vẫn re-export mọi tên bên dưới cho các chỗ gọi hiện có."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import and_, func, insert, or_, select, update

from app.clients.d1 import _configured, d1_query
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import comments, keywords, movies, posts
from app.services.crawl.platforms import CommentDraft

# Quá số dòng này thì list_comments không trả thêm - đây là danh sách để admin xem
# lại, không phải feed phân trang như list_posts; một bài hiếm khi có quá vài trăm
# comment, đây chỉ là trần an toàn chống một response phình to mất kiểm soát.
MAX_COMMENTS_PER_POST = 500

_comments_parent_column_ready = False
_comments_parent_column_lock = asyncio.Lock()

# Xem comment _POST_INDEXES trong posts.py - cùng lý do, dựa trên đúng dạng
# WHERE/ORDER BY/JOIN bên dưới và phần kiểm tra upsert persist_comment của
# ingest_consumer.
_COMMENT_INDEXES = (
    # /stats/comments không lọc nền tảng (tab mặc định "All platforms" của
    # CommentsReview.tsx) - ORDER BY scraped_at DESC của list_all_comments, không có
    # WHERE.
    "CREATE INDEX IF NOT EXISTS idx_comments_scraped_at ON comments(scraped_at DESC)",
    # /stats/comments?platform=X - WHERE platform = ? ORDER BY scraped_at DESC của
    # list_all_comments.
    "CREATE INDEX IF NOT EXISTS idx_comments_platform_scraped_at ON comments(platform, scraped_at DESC)",
    # WHERE post_id = ? ORDER BY scraped_at DESC của list_comments (luồng comment của một
    # bài) - cũng phủ luôn subquery `GROUP BY post_id` không lọc của
    # list_posts_needing_comments thành quét chỉ trên index thay vì tổng hợp toàn bảng.
    "CREATE INDEX IF NOT EXISTS idx_comments_post_id_scraped_at ON comments(post_id, scraped_at DESC)",
    # Phần kiểm tra upsert của persist_comment (SELECT ... WHERE platform = ? AND
    # external_id = ?) chạy trên từng comment được ingest.
    "CREATE INDEX IF NOT EXISTS idx_comments_platform_external_id ON comments(platform, external_id)",
)

_TAB_FILTER_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_comments_sentiment_scraped_at ON comments(sentiment, scraped_at DESC)",
)

_comment_indexes_ready = False
_comment_indexes_lock = asyncio.Lock()


async def _ensure_comment_indexes() -> None:
    global _comment_indexes_ready
    if _comment_indexes_ready:
        return
    async with _comment_indexes_lock:
        if _comment_indexes_ready:
            return
        for sql in _COMMENT_INDEXES:
            await d1_query(sql, quiet=True)
        _comment_indexes_ready = True


async def ensure_tab_filter_indexes() -> None:
    """CREATE INDEX cho các tab cảm xúc của CommentsReview. Task nền lúc khởi động - xem
    posts.ensure_tab_filter_indexes."""
    for sql in _TAB_FILTER_INDEXES:
        await d1_query(sql, quiet=True, timeout=90.0)


async def _ensure_comments_parent_column() -> None:
    """Thêm comments.parent_external_id một lần mỗi tiến trình. Cột này đã có sẵn trên DB
    đã migrate - trước đây ingest chạy song song đua nhau chạy N lệnh ALTER và spam
    local_db_query_failed('duplicate column name'). Khoá + kiểm tra tồn tại để chỉ ALTER
    khi còn thiếu, và không bao giờ coi lỗi trùng cột là thất bại thật."""
    global _comments_parent_column_ready
    if _comments_parent_column_ready:
        return
    async with _comments_parent_column_lock:
        if _comments_parent_column_ready:
            return
        from app.core.config import settings

        if settings.db_mode == "local":
            cols = await d1_query("PRAGMA table_info(comments)")
            if cols and any(row.get("name") == "parent_external_id" for row in cols):
                _comments_parent_column_ready = True
                return
        # quiet: DB đã migrate sẽ báo trùng cột — là chuyện bình thường.
        await d1_query("ALTER TABLE comments ADD COLUMN parent_external_id TEXT", quiet=True)
        _comments_parent_column_ready = True


_comment_insight_columns_ready = False
_comment_insight_columns_lock = asyncio.Lock()
# Khía cạnh khen/chê (JSON list "key:+"), giai đoạn khán giả và thời điểm gán - xem app/ai/domain/aspects.py.
# first_seen_at: lần đầu comment vào D1 (scraped_at bị ghi đè mỗi lần quét lại).
# Khoá raw_json của comment còn được đọc: tệp đính kèm hiện trên dashboard. Bản đầy đủ nằm ở tầng bronze trên R2 (lake_writer).
COMMENT_RAW_KEEP = ("sticker_url", "gif", "image", "video")


def trim_comment_raw(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    return {key: raw[key] for key in COMMENT_RAW_KEEP if raw.get(key)} or None


COMMENT_INSIGHT_COLUMNS = ("aspects", "audience_stage", "insights_classified_at", "first_seen_at")


async def ensure_comment_insight_columns() -> None:
    """Thêm các cột COMMENT_INSIGHT_COLUMNS còn thiếu, một lần mỗi tiến trình - cùng kiểu với
    _ensure_comments_parent_column: kiểm tra PRAGMA trước để không ALTER khi cột đã có."""
    global _comment_insight_columns_ready
    if _comment_insight_columns_ready:
        return
    async with _comment_insight_columns_lock:
        if _comment_insight_columns_ready:
            return
        cols = await d1_query("PRAGMA table_info(comments)")
        existing = {row.get("name") for row in cols or []}
        for column in COMMENT_INSIGHT_COLUMNS:
            if column not in existing:
                await d1_query(f"ALTER TABLE comments ADD COLUMN {column} TEXT", quiet=True)
        _comment_insight_columns_ready = True


# Comment + comment cha (reply) + bài + phim + từ khoá, cho các danh sách trên dashboard.
_C, _PARENT = comments.alias("c"), comments.alias("parent")
_P, _M, _K = posts.alias("p"), movies.alias("m"), keywords.alias("k")
# Ghép bình luận cha theo (platform, external_id) - có unique index comments_platform_external_id_unique. Trước 14/11/2026
# ghép theo (post_id, external_id), cặp cột không có index: mỗi dòng dò toàn bộ bình luận của bài (bài TikTok vài nghìn
# bình luận), một trang danh sách mất tới 35 giây và chặn D1. external_id là duy nhất trong một nền tảng nên kết quả như cũ.
_WITH_PARENT = _C.outerjoin(
    _PARENT,
    and_(
        _PARENT.c.platform == _C.c.platform,
        _PARENT.c.external_id == _C.c.parent_external_id,
        _PARENT.c.post_id == _C.c.post_id,
    ),
)
_COMMENT_FIELDS = [
    "id",
    "post_id",
    "platform",
    "external_id",
    "message",
    "author_name",
    "author_id",
    "author_url",
    "author_profile_picture",
    "reactions_count",
    "replies_count",
    "posted_at",
    "scraped_at",
    "parent_external_id",
]
_COMMENT_COLUMNS = (
    *(_C.c[name] for name in _COMMENT_FIELDS),
    _PARENT.c.message.label("parent_message"),
    _PARENT.c.author_name.label("parent_author_name"),
)
_FEED_FROM = (
    _WITH_PARENT.outerjoin(_P, _P.c.id == _C.c.post_id)
    .outerjoin(_M, _M.c.id == _P.c.movie_id)
    .outerjoin(_K, _K.c.id == _P.c.keyword_id)
)
_FEED_COLUMNS = (
    *_COMMENT_COLUMNS,
    _C.c.sentiment,
    _P.c.content.label("post_content"),
    _P.c.url.label("post_url"),
    _P.c.author.label("post_author"),
    _M.c.title.label("movie_title"),
    _K.c.keyword,
)


def _feed_filters(platform: str | None, movie_id: str | None, keyword_id: str | None, sentiment: str | None) -> list:
    """Bộ lọc không bắt buộc, cộng dồn, của list_all_comments / list_all_comments_cursor."""
    filters = []
    if platform:
        filters.append(_C.c.platform == platform)
    if movie_id:
        filters.append(_P.c.movie_id == movie_id)
    if keyword_id:
        filters.append(_P.c.keyword_id == keyword_id)
    if sentiment:
        filters.append(_C.c.sentiment == sentiment)
    return filters


class CommentRepository:
    """Giữ mọi query trên `comments`. Một instance cho cả tiến trình (`comment_repo` bên
    dưới), cùng dạng với PostRepository."""

    async def list_comments(self, post_id: str) -> list[dict[str, Any]]:
        """Mọi comment đã lưu của một bài (id D1, xem PostRepository.get_post ở trên), mới
        nhất trước - phục vụ GET /stats/posts/{post_id}/comments."""
        await _ensure_comments_parent_column()
        await _ensure_comment_indexes()
        rows = await d1_exec(
            select(*_COMMENT_COLUMNS)
            .select_from(_WITH_PARENT)
            .where(_C.c.post_id == post_id)
            .order_by(_C.c.scraped_at.desc())
            .limit(MAX_COMMENTS_PER_POST)
        )
        rows = rows or []
        print(f"list_comments({post_id}): {len(rows)} rows")
        return rows

    async def list_all_comments(
        self,
        *,
        platform: str | None = None,
        movie_id: str | None = None,
        keyword_id: str | None = None,
        sentiment: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Feed comment phân trang trên mọi bài (thu thập gần nhất trước), join với bài cha để
        hiển thị - phục vụ tab xem lại "Comments" riêng, cùng dạng với
        PostRepository.list_posts ở trên. Mọi bộ lọc đều không bắt buộc và cộng dồn."""
        await _ensure_comments_parent_column()
        await _ensure_comment_indexes()
        filters = _feed_filters(platform, movie_id, keyword_id, sentiment)
        rows = await d1_exec(
            select(*_FEED_COLUMNS)
            .select_from(_FEED_FROM)
            .where(*filters)
            .order_by(_C.c.scraped_at.desc())
            .limit(limit)
            .offset(offset),
            timeout=20.0 if sentiment else 10.0,
        )
        items = rows or []
        if sentiment and not movie_id and not keyword_id:
            n = len(items)
            total = offset + n + (limit if n == limit else 0)
        else:
            count_rows = await d1_exec(
                select(func.count().label("total"))
                .select_from(_C.outerjoin(_P, _P.c.id == _C.c.post_id))
                .where(*filters)
            )
            total = (count_rows[0]["total"] if count_rows else 0) or 0
        return items, total

    async def list_all_comments_cursor(
        self,
        *,
        platform: str | None = None,
        movie_id: str | None = None,
        keyword_id: str | None = None,
        sentiment: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Bản phân trang keyset tương đương list_all_comments - xem list_posts_cursor trong
        posts.py để biết đầy đủ lý do (đã xác nhận thực tế cùng kiểu chậm OFFSET+JOIN trên
        các join parent/post/movie/keyword của bảng này). `cursor` là chuỗi
        "<scraped_at>|<id>" không cần hiểu bên trong của dòng cuối trang trước; None là bắt
        đầu từ đầu. Không có tổng số - xem comment trong list_posts_cursor về việc đó là chủ
        đích, không phải thiếu sót."""
        await _ensure_comments_parent_column()
        await _ensure_comment_indexes()
        filters = _feed_filters(platform, movie_id, keyword_id, sentiment)
        if cursor:
            cursor_scraped_at, _, cursor_id = cursor.partition("|")
            filters.append(
                or_(
                    _C.c.scraped_at < cursor_scraped_at,
                    and_(_C.c.scraped_at == cursor_scraped_at, _C.c.id < cursor_id),
                )
            )
        rows = await d1_exec(
            select(*_FEED_COLUMNS)
            .select_from(_FEED_FROM)
            .where(*filters)
            .order_by(_C.c.scraped_at.desc(), _C.c.id.desc())
            .limit(limit),
            timeout=20.0 if sentiment else 10.0,
        )
        items = rows or []
        next_cursor = f"{items[-1]['scraped_at']}|{items[-1]['id']}" if len(items) == limit else None
        return items, next_cursor

    async def persist_comment(
        self, *, post_id: str, platform: str, draft: CommentDraft, sentiment: str | None = None
    ) -> bool:
        """Upsert một comment đã crawl theo (platform, external_id) - cùng dạng với
        PostRepository.persist_post nhưng đơn giản hơn (comment không có lịch sử snapshot
        tương tác riêng, chỉ có số reaction/reply hiện tại).

        `sentiment` là nhãn do AI phân loại ("positive"/"negative"/"neutral") từ
        app.ai.tasks.sentiment.classify_comments() (Kira), đã được chỗ gọi xác định trước
        khi gọi hàm này - None nghĩa là hoặc chưa phân loại (message quá ngắn) hoặc lời gọi
        AI thất bại, và chỉ để cột là NULL thay vì chặn việc upsert."""
        if not _configured():
            return False

        await ensure_comment_insight_columns()
        await _ensure_comments_parent_column()
        await _ensure_comment_indexes()

        external_id = draft.get("external_id")
        if not external_id:
            return False

        scraped_at = datetime.now(tz=UTC).isoformat()
        kept_raw = trim_comment_raw(draft.get("raw"))
        raw_json = json.dumps(kept_raw) if kept_raw else None
        sentiment_classified_at = scraped_at if sentiment is not None else None

        fields = {
            "message": draft.get("message"),
            "author_name": draft.get("author_name"),
            "author_id": draft.get("author_id"),
            "author_url": draft.get("author_url"),
            "author_profile_picture": draft.get("author_profile_picture"),
            "reactions_count": draft.get("reactions_count") or 0,
            "replies_count": draft.get("replies_count") or 0,
            "posted_at": draft.get("posted_at"),
            "scraped_at": scraped_at,
            "raw_json": raw_json,
        }
        existing_rows = await d1_exec(
            select(comments.c.id).where(comments.c.platform == platform, comments.c.external_id == external_id)
        )
        if existing_rows:
            # Giữ giá trị đã có khi lần quét này không mang theo (COALESCE).
            written = await d1_exec(
                update(comments)
                .where(comments.c.id == existing_rows[0]["id"])
                .values(
                    **fields,
                    parent_external_id=func.coalesce(draft.get("parent_external_id"), comments.c.parent_external_id),
                    sentiment=func.coalesce(sentiment, comments.c.sentiment),
                    sentiment_classified_at=func.coalesce(sentiment_classified_at, comments.c.sentiment_classified_at),
                )
            )
        else:
            written = await d1_exec(
                insert(comments).values(
                    id=f"comment_{uuid.uuid4()}",
                    post_id=post_id,
                    platform=platform,
                    external_id=external_id,
                    parent_external_id=draft.get("parent_external_id"),
                    first_seen_at=scraped_at,  # chỉ ghi lúc INSERT, các UPDATE không đụng tới
                    sentiment=sentiment,
                    sentiment_classified_at=sentiment_classified_at,
                    **fields,
                )
            )
        if written is None:
            return False
        parent = await d1_exec(select(posts.c.keyword_id).where(posts.c.id == post_id))
        from app.services.stats.stats_summary import record_comment

        await record_comment(
            platform=platform,
            keyword_id=parent[0].get("keyword_id") if parent else None,
            scraped_at=scraped_at,
            is_new=not existing_rows,
        )
        return True


comment_repo = CommentRepository()


# --- các hàm tự do để tương thích ngược (xem docstring module) -----------


async def list_comments(post_id: str) -> list[dict[str, Any]]:
    return await comment_repo.list_comments(post_id)


async def list_all_comments(**kwargs: Any) -> tuple[list[dict[str, Any]], int]:
    return await comment_repo.list_all_comments(**kwargs)


async def list_all_comments_cursor(**kwargs: Any) -> tuple[list[dict[str, Any]], str | None]:
    return await comment_repo.list_all_comments_cursor(**kwargs)


async def persist_comment(**kwargs: Any) -> bool:
    return await comment_repo.persist_comment(**kwargs)
