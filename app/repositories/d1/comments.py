"""Everything that reads/writes D1's `comments` table - see
app/repositories/d1/posts.py's own module docstring for why this table got
split out on its own and why app/services/d1.py still re-exports every name
below for existing callers."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.services.d1_client import d1_query, _configured
from app.services.platforms import CommentDraft

# Above this many rows, list_comments stops returning more - an admin
# review list, not a paginated feed like list_posts; a single post rarely
# has more than a few hundred comments, and this is just a sanity ceiling
# against a runaway response.
MAX_COMMENTS_PER_POST = 500

_comments_parent_column_ready = False
_comments_parent_column_lock = asyncio.Lock()

# See posts.py's own _POST_INDEXES comment - same rationale, grounded in
# the actual WHERE/ORDER BY/JOIN shapes below and in ingest_consumer's
# persist_comment upsert-check.
_COMMENT_INDEXES = (
    # /stats/comments with no platform filter (CommentsReview.tsx's default
    # "All platforms" tab) - list_all_comments' ORDER BY scraped_at DESC
    # with no WHERE.
    "CREATE INDEX IF NOT EXISTS idx_comments_scraped_at ON comments(scraped_at DESC)",
    # /stats/comments?platform=X - list_all_comments' WHERE platform = ?
    # ORDER BY scraped_at DESC.
    "CREATE INDEX IF NOT EXISTS idx_comments_platform_scraped_at ON comments(platform, scraped_at DESC)",
    # list_comments' WHERE post_id = ? ORDER BY scraped_at DESC (one post's
    # comment thread) - also covers list_posts_needing_comments' unfiltered
    # `GROUP BY post_id` subquery as an index-only scan instead of a full
    # table aggregate.
    "CREATE INDEX IF NOT EXISTS idx_comments_post_id_scraped_at ON comments(post_id, scraped_at DESC)",
    # persist_comment's own upsert-check (SELECT ... WHERE platform = ? AND
    # external_id = ?) runs on every single ingested comment.
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
    """CREATE INDEX for CommentsReview sentiment tabs. Startup background
    task - see posts.ensure_tab_filter_indexes."""
    for sql in _TAB_FILTER_INDEXES:
        await d1_query(sql, quiet=True, timeout=90.0)


async def _ensure_comments_parent_column() -> None:
    """Adds comments.parent_external_id once per process. The column already
    exists on migrated DBs - concurrent ingest used to race N ALTERs and
    spam local_db_query_failed('duplicate column name'). Lock + existence
    check so we only ALTER when missing, and never treat duplicate as a
    real failure."""
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
        # quiet: already-migrated DBs raise duplicate column — expected.
        await d1_query("ALTER TABLE comments ADD COLUMN parent_external_id TEXT", quiet=True)
        _comments_parent_column_ready = True


class CommentRepository:
    """Owns every query against `comments`. One process-wide instance
    (`comment_repo` below), same shape as PostRepository."""

    async def list_comments(self, post_id: str) -> list[dict[str, Any]]:
        """Every comment stored for one post (D1 id, see PostRepository.get_post
        above), newest first - backs GET /stats/posts/{post_id}/comments."""
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
        """Paginated comment feed across every post (most recently collected
        first), joined to its parent post for display - backs a dedicated
        "Comments" review tab, same shape as PostRepository.list_posts above.
        Every filter is optional and additive."""
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
        """Keyset-paginated equivalent of list_all_comments - see
        posts.py's list_posts_cursor for the full rationale (same OFFSET+
        JOIN slowdown confirmed live on this table's own parent/post/
        movie/keyword joins). `cursor` is the opaque "<scraped_at>|<id>"
        of the last row from the previous page; None starts from the top.
        No total - see list_posts_cursor's own comment on why that's the
        point, not a gap."""
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
        """Upsert one scraped comment by (platform, external_id) - same shape
        as PostRepository.persist_post but simpler (comments have no
        engagement-snapshot history of their own, just a live
        reactions/replies count).

        `sentiment` is the AI-classified label ("positive"/"negative"/"neutral")
        from app.kira.sentiment.classify_sentiment() (PhoBERT by default; Kira
        optional), already resolved by the
        caller before this is invoked - None means either classification wasn't
        attempted (message too short) or the Kira call failed, and just leaves
        the column NULL rather than blocking the upsert."""
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


# --- backward-compatible free functions (see module docstring) -----------


async def list_comments(post_id: str) -> list[dict[str, Any]]:
    return await comment_repo.list_comments(post_id)


async def list_all_comments(**kwargs: Any) -> tuple[list[dict[str, Any]], int]:
    return await comment_repo.list_all_comments(**kwargs)


async def list_all_comments_cursor(**kwargs: Any) -> tuple[list[dict[str, Any]], str | None]:
    return await comment_repo.list_all_comments_cursor(**kwargs)


async def persist_comment(**kwargs: Any) -> bool:
    return await comment_repo.persist_comment(**kwargs)
