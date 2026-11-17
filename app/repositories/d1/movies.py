"""Phim: danh sách, tạo, sửa, tắt (bảng movies của D1 scraper) và slug.

Tách từ store.py (17/11/2026); store.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import insert, select, update

from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import keywords, movies

logger = get_logger(__name__)


_MOVIE_FIELDS = ("released_at", "poster_url", "description", "director", "cast", "distributor")
# Thông tin phim đưa vào prompt AI (app/ai/domain/movie_context.py).
MOVIE_CONTEXT_FIELDS = ("title", "director", "cast", "distributor", "released_at", "description")
_MOVIE_COLUMNS = (movies.c.id, movies.c.title, movies.c.slug, *(movies.c[f] for f in _MOVIE_FIELDS))
_KEYWORD_JOIN = keywords.join(movies, movies.c.id == keywords.c.movie_id)
_KEYWORD_ROW = (keywords.c.id, keywords.c.movie_id, movies.c.title.label("movie_title"), keywords.c.keyword)


def movie_slug(title: str) -> str:
    """Slug URL gần như ASCII từ một tiêu đề (đã bỏ dấu tiếng Việt)."""
    folded = title.strip().replace("đ", "d").replace("Đ", "D")
    normalized = unicodedata.normalize("NFKD", folded)
    ascii_ish = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_ish.lower()).strip("-")
    return slug or "movie"


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


async def _unique_movie_slug(base: str, exclude_id: str | None = None) -> str | None:
    slug = base
    suffix = 2
    while True:
        rows = await d1_exec(select(movies.c.id).where(movies.c.slug == slug))
        if rows is None:
            return None
        clash = next((row for row in rows if row["id"] != exclude_id), None)
        if clash is None:
            return slug
        slug = f"{base}-{suffix}"
        suffix += 1


async def list_movies() -> list[dict[str, Any]]:
    """Mọi phim đang bật - cấp dữ liệu cho ô chọn "từ khoá mới này thuộc phim nào" khi tạo
    từ khoá ngay trong form kích hoạt crawl, và bảng chi tiết phim của dashboard. Cũng
    được scripts/jobs/generate_social_topic_reports.py dùng để duyệt qua mọi phim cần report
    (các trường thừa ở đây chỗ gọi đó đơn giản là không dùng, không làm hỏng gì). `cast`
    cần dấu backtick - nó là từ khoá SQL (hàm CAST()) trong ngữ pháp của chính SQLite,
    không chỉ của Python."""
    rows = await d1_exec(select(*_MOVIE_COLUMNS).where(movies.c.enabled == 1).order_by(movies.c.title))
    return rows or []


async def get_movie(movie_id: str) -> dict[str, Any] | None:
    rows = await d1_exec(select(*_MOVIE_COLUMNS).where(movies.c.id == movie_id, movies.c.enabled == 1))
    if rows is None:
        return None
    return rows[0] if rows else None


async def create_movie(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Tạo phim từ trang Movies của dashboard - nhân viên gõ tên và các trường
    release/cast tuỳ chọn; slug được tự suy ra trừ khi họ truyền vào."""
    title = (fields.get("title") or "").strip()
    if not title:
        return None
    requested = _blank_to_none(fields.get("slug"))
    slug = await _unique_movie_slug(movie_slug(requested or title))
    if slug is None:
        return None
    now = datetime.now(tz=UTC).isoformat()
    movie_id = f"movie_{uuid.uuid4()}"
    inserted = await d1_exec(
        insert(movies).values(
            id=movie_id,
            title=title,
            slug=slug,
            enabled=1,
            created_at=now,
            updated_at=now,
            **{f: _blank_to_none(fields.get(f)) for f in _MOVIE_FIELDS},
        )
    )
    if inserted is None:
        return None
    return await get_movie(movie_id)


async def update_movie(movie_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    existing = await get_movie(movie_id)
    if not existing:
        return None

    title = existing["title"]
    if "title" in fields and fields["title"] is not None:
        title = fields["title"].strip() or existing["title"]

    slug = existing["slug"]
    if "slug" in fields:
        requested = _blank_to_none(fields.get("slug"))
        if requested:
            unique = await _unique_movie_slug(movie_slug(requested), exclude_id=movie_id)
            if unique is None:
                return None
            slug = unique

    now = datetime.now(tz=UTC).isoformat()

    def pick(key: str) -> str | None:
        if key not in fields:
            return existing.get(key)
        return _blank_to_none(fields.get(key))

    updated = await d1_exec(
        update(movies)
        .where(movies.c.id == movie_id, movies.c.enabled == 1)
        .values(title=title, slug=slug, updated_at=now, **{f: pick(f) for f in _MOVIE_FIELDS})
    )
    if updated is None:
        return None
    return await get_movie(movie_id)


async def disable_movie(movie_id: str) -> bool | None:
    """Xoá mềm khỏi danh sách trên dashboard (keywords/posts vẫn giữ khoá ngoại)."""
    existing = await get_movie(movie_id)
    if existing is None:
        rows = await d1_exec(select(movies.c.id).where(movies.c.id == movie_id))
        if rows is None:
            return None
        return False
    now = datetime.now(tz=UTC).isoformat()
    result = await d1_exec(
        update(movies).where(movies.c.id == movie_id, movies.c.enabled == 1).values(enabled=0, updated_at=now)
    )
    if result is None:
        return None
    return True
