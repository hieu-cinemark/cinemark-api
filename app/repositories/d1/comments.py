"""Mọi thứ đọc/ghi bảng `comments` của D1 - xem docstring module của
app/repositories/d1/posts.py để biết vì sao bảng này được tách riêng và vì sao
app/services/d1.py vẫn re-export mọi tên bên dưới cho các chỗ gọi hiện có."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.clients.d1 import _configured, d1_query
from app.services.platforms import CommentDraft

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


class CommentRepository:
    """Giữ mọi query trên `comments`. Một instance cho cả tiến trình (`comment_repo` bên
    dưới), cùng dạng với PostRepository."""

    async def list_comments(self, post_id: str) -> list[dict[str, Any]]:
        """Mọi comment đã lưu của một bài (id D1, xem PostRepository.get_post ở trên), mới
        nhất trước - phục vụ GET /stats/posts/{post_id}/comments."""
        await _ensure_comments_parent_column()
        await _ensure_comment_indexes()
        rows = await d1_query(
            """
            SELECT c.id, c.post_id, c.platform, c.external_id, c.message, c.author_name, c.author_id, c.author_url,
                   c.author_profile_picture, c.reactions_count, c.replies_count, c.posted_at, c.scraped_at,
                   c.parent_external_id,
                   parent.message AS parent_message, parent.author_name AS parent_author_name
            FROM comments c
            LEFT JOIN comments parent
                ON parent.post_id = c.post_id AND parent.external_id = c.parent_external_id
            WHERE c.post_id = ?
            ORDER BY c.scraped_at DESC
            LIMIT ?
            """,
            [post_id, MAX_COMMENTS_PER_POST],
        )
        return rows or []

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
        where = []
        params: list[Any] = []
        if platform:
            where.append("c.platform = ?")
            params.append(platform)
        if movie_id:
            where.append("p.movie_id = ?")
            params.append(movie_id)
        if keyword_id:
            where.append("p.keyword_id = ?")
            params.append(keyword_id)
        if sentiment:
            where.append("c.sentiment = ?")
            params.append(sentiment)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        rows = await d1_query(
            f"""
            SELECT
                c.id, c.post_id, c.platform, c.external_id, c.message, c.author_name, c.author_id, c.author_url,
                c.author_profile_picture, c.reactions_count, c.replies_count, c.posted_at, c.scraped_at,
                c.parent_external_id, c.sentiment,
                parent.message AS parent_message, parent.author_name AS parent_author_name,
                p.content AS post_content, p.url AS post_url, p.author AS post_author, m.title AS movie_title,
                k.keyword AS keyword
            FROM comments c
            LEFT JOIN comments parent
                ON parent.post_id = c.post_id AND parent.external_id = c.parent_external_id
            LEFT JOIN posts p ON p.id = c.post_id
            LEFT JOIN movies m ON m.id = p.movie_id
            LEFT JOIN keywords k ON k.id = p.keyword_id
            {where_sql}
            ORDER BY c.scraped_at DESC
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
            timeout=20.0 if sentiment else 10.0,
        )
        items = rows or []
        if sentiment and not movie_id and not keyword_id:
            n = len(items)
            total = offset + n + (limit if n == limit else 0)
        else:
            count_rows = await d1_query(
                f"SELECT COUNT(*) AS total FROM comments c LEFT JOIN posts p ON p.id = c.post_id {where_sql}", params
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
        where = []
        params: list[Any] = []
        if platform:
            where.append("c.platform = ?")
            params.append(platform)
        if movie_id:
            where.append("p.movie_id = ?")
            params.append(movie_id)
        if keyword_id:
            where.append("p.keyword_id = ?")
            params.append(keyword_id)
        if sentiment:
            where.append("c.sentiment = ?")
            params.append(sentiment)
        if cursor:
            cursor_scraped_at, _, cursor_id = cursor.partition("|")
            where.append("(c.scraped_at < ? OR (c.scraped_at = ? AND c.id < ?))")
            params += [cursor_scraped_at, cursor_scraped_at, cursor_id]
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        rows = await d1_query(
            f"""
            SELECT
                c.id, c.post_id, c.platform, c.external_id, c.message, c.author_name, c.author_id, c.author_url,
                c.author_profile_picture, c.reactions_count, c.replies_count, c.posted_at, c.scraped_at,
                c.parent_external_id, c.sentiment,
                parent.message AS parent_message, parent.author_name AS parent_author_name,
                p.content AS post_content, p.url AS post_url, p.author AS post_author, m.title AS movie_title,
                k.keyword AS keyword
            FROM comments c
            LEFT JOIN comments parent
                ON parent.post_id = c.post_id AND parent.external_id = c.parent_external_id
            LEFT JOIN posts p ON p.id = c.post_id
            LEFT JOIN movies m ON m.id = p.movie_id
            LEFT JOIN keywords k ON k.id = p.keyword_id
            {where_sql}
            ORDER BY c.scraped_at DESC, c.id DESC
            LIMIT ?
            """,
            [*params, limit],
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
        app.ai.tasks.sentiment.classify_sentiment() (Kira), đã được chỗ gọi xác định trước
        khi gọi hàm này - None nghĩa là hoặc chưa phân loại (message quá ngắn) hoặc lời gọi
        AI thất bại, và chỉ để cột là NULL thay vì chặn việc upsert."""
        if not _configured():
            return False

        await _ensure_comments_parent_column()
        await _ensure_comment_indexes()

        external_id = draft.get("external_id")
        if not external_id:
            return False

        scraped_at = datetime.now(tz=timezone.utc).isoformat()
        raw_json = json.dumps(draft.get("raw")) if draft.get("raw") is not None else None
        sentiment_classified_at = scraped_at if sentiment is not None else None

        existing_rows = await d1_query(
            "SELECT id FROM comments WHERE platform = ? AND external_id = ?", [platform, external_id]
        )
        if existing_rows:
            updated = await d1_query(
                """
                UPDATE comments SET
                    message = ?, author_name = ?, author_id = ?, author_url = ?, author_profile_picture = ?,
                    reactions_count = ?, replies_count = ?, parent_external_id = COALESCE(?, parent_external_id),
                    posted_at = ?, scraped_at = ?, raw_json = ?,
                    sentiment = COALESCE(?, sentiment), sentiment_classified_at = COALESCE(?, sentiment_classified_at)
                WHERE id = ?
                """,
                [
                    draft.get("message"),
                    draft.get("author_name"),
                    draft.get("author_id"),
                    draft.get("author_url"),
                    draft.get("author_profile_picture"),
                    draft.get("reactions_count") or 0,
                    draft.get("replies_count") or 0,
                    draft.get("parent_external_id"),
                    draft.get("posted_at"),
                    scraped_at,
                    raw_json,
                    sentiment,
                    sentiment_classified_at,
                    existing_rows[0]["id"],
                ],
            )
            if updated is None:
                return False
            keyword_id: str | None = None
            parent = await d1_query("SELECT keyword_id FROM posts WHERE id = ?", [post_id])
            if parent:
                keyword_id = parent[0].get("keyword_id")
            from app.services.stats_summary import record_comment

            await record_comment(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=False)
            return True

        inserted = await d1_query(
            """
            INSERT INTO comments (
                id, post_id, platform, external_id, message, author_name, author_id, author_url,
                author_profile_picture, reactions_count, replies_count, parent_external_id, posted_at, scraped_at, raw_json,
                sentiment, sentiment_classified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"comment_{uuid.uuid4()}",
                post_id,
                platform,
                external_id,
                draft.get("message"),
                draft.get("author_name"),
                draft.get("author_id"),
                draft.get("author_url"),
                draft.get("author_profile_picture"),
                draft.get("reactions_count") or 0,
                draft.get("replies_count") or 0,
                draft.get("parent_external_id"),
                draft.get("posted_at"),
                scraped_at,
                raw_json,
                sentiment,
                sentiment_classified_at,
            ],
        )
        if inserted is None:
            return False
        keyword_id: str | None = None
        parent = await d1_query("SELECT keyword_id FROM posts WHERE id = ?", [post_id])
        if parent:
            keyword_id = parent[0].get("keyword_id")
        from app.services.stats_summary import record_comment

        await record_comment(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=True)
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
