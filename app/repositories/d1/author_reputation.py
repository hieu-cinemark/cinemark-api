"""Uy tín tác giả: bảng author_reputation (cinemark-api dựng) và danh sách tác giả đáng tin cho luật hashtag.

Tách từ posts.py (17/11/2026); posts.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

import time

from sqlalchemy import Column, Integer, Table, Text, select, text
from sqlalchemy.schema import CreateTable

from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, metadata

logger = get_logger(__name__)


# --- uy tín tác giả --------------------------------------------------------
# Củng cố tín hiệu yếu nhất của movie_hashtag_present (chỉ khớp tên phim nguyên văn,
# không có hashtag nào hỗ trợ) cho phim có tên cũng là từ vựng thông thường - xem
# docstring của hàm đó về sự cố "Huyết Thống" mà phần này sinh ra để xử lý. Uy tín của
# một tác giả được dựng bởi scripts/jobs/build_author_reputation.py bằng cách kiểm tra lại
# các bài relevance_label='related' trước đây của chính họ chỉ với các tín hiệu MẠNH
# của movie_hashtag_present (is_reputable_author mặc định False - xem tham số đó) -
# không bao giờ dùng chính tín hiệu yếu mà bảng này dùng để củng cố, nên không có vòng
# lặp: uy tín chỉ có được qua bằng chứng hashtag.

MIN_MOVIES_FOR_REPUTABLE_AUTHOR = 2
_REPUTABLE_AUTHORS_TTL_SECONDS = 300.0

_author_reputation_ready = False
author_reputation = Table(
    "author_reputation",
    metadata,
    Column("platform", Text, primary_key=True),
    Column("author", Text, primary_key=True),
    Column("distinct_movies", Integer, nullable=False, server_default=text("0")),
    Column("confirmed_posts", Integer, nullable=False, server_default=text("0")),
    Column("updated_at", Text, nullable=False),
)
_reputable_authors_cache: tuple[float, set[tuple[str, str]]] | None = None


async def ensure_author_reputation_table() -> None:
    global _author_reputation_ready
    if _author_reputation_ready:
        return
    await d1_exec(CreateTable(author_reputation, if_not_exists=True), quiet=True)
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
    rows = await d1_exec(
        select(author_reputation.c.platform, author_reputation.c.author).where(
            author_reputation.c.distinct_movies >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR
        )
    )
    result = {(r["platform"], r["author"]) for r in (rows or [])}
    _reputable_authors_cache = (now, result)
    return result
