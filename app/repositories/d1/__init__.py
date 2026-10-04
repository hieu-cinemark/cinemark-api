"""Repository D1 theo từng bảng - xem posts.py/comments.py. Mỗi repository giữ phần SQL
thô của một bảng (schema ở cinemark-scraper/src/db/schema.ts) để app/services/d1.py
không phải giữ; module đó chỉ còn là tầng truyền tải (d1_query qua HTTP hoặc local)
mà mọi repository gọi qua."""

from __future__ import annotations

from app.repositories.d1.comments import CommentRepository, comment_repo
from app.repositories.d1.posts import PostRepository, post_repo

__all__ = ["CommentRepository", "PostRepository", "comment_repo", "post_repo"]
