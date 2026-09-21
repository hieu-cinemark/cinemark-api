"""Per-table D1 repositories - see posts.py/comments.py. Each repository
owns the raw SQL for one table (schema in cinemark-scraper/src/db/schema.ts)
so app/services/d1.py doesn't have to; that module stays the transport
(HTTP-vs-local d1_query) every repository calls through."""

from __future__ import annotations

from app.repositories.d1.comments import CommentRepository, comment_repo
from app.repositories.d1.posts import PostRepository, post_repo

__all__ = ["CommentRepository", "PostRepository", "comment_repo", "post_repo"]
