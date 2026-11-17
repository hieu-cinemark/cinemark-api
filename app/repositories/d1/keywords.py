"""Từ khoá theo dõi (bảng keywords): tạo, bật/tắt, đọc theo nền tảng/phim.

Tách từ store.py (17/11/2026); store.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import insert, select, update

from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import keywords, movies

logger = get_logger(__name__)

from app.repositories.d1.movies import _KEYWORD_JOIN, _KEYWORD_ROW, MOVIE_CONTEXT_FIELDS


async def get_or_create_keyword(movie_id: str, platform: str, keyword: str) -> dict[str, Any] | None:
    """Dùng cho luồng "gõ từ khoá mới" ngay trên dashboard (form kích hoạt crawl) - tìm
    dòng (movie_id, platform, keyword) đã có trước (bộ ba đó có unique index - xem
    src/db/schema.ts của cinemark-scraper) để gửi lặp lại hoặc đua với một tab khác chỉ
    trả về cùng dòng đó thay vì lỗi ràng buộc."""
    existing = await d1_exec(
        select(*_KEYWORD_ROW)
        .select_from(_KEYWORD_JOIN)
        .where(keywords.c.movie_id == movie_id, keywords.c.platform == platform, keywords.c.keyword == keyword)
    )
    if existing:
        return existing[0]

    movie_rows = await d1_exec(select(movies.c.title).where(movies.c.id == movie_id, movies.c.enabled == 1))
    if not movie_rows:
        return None
    movie_title = movie_rows[0]["title"]

    keyword_id = f"kw_{uuid.uuid4()}"
    inserted = await d1_exec(
        insert(keywords).values(
            id=keyword_id,
            movie_id=movie_id,
            platform=platform,
            keyword=keyword,
            enabled=1,
            created_at=datetime.now(tz=UTC).isoformat(),
        )
    )
    if inserted is None:
        return None
    return {"id": keyword_id, "movie_id": movie_id, "movie_title": movie_title, "keyword": keyword}


async def add_keyword(
    movie_id: str, platform: str, keyword: str, keyword_id: str | None = None, *, enabled: bool = True
) -> str | None:
    """Thêm một từ khoá (luồng tự tìm từ khoá). enabled=False: từ khoá chỉ để gắn bài chuyển sang phim (ingest), không cào.
    Trả về id, None khi ghi lỗi."""
    keyword_id = keyword_id or f"kw_{uuid.uuid4()}"
    inserted = await d1_exec(
        insert(keywords).values(
            id=keyword_id,
            movie_id=movie_id,
            platform=platform,
            keyword=keyword,
            enabled=1 if enabled else 0,
            created_at=datetime.now(tz=UTC).isoformat(),
            related_keywords="[]",
        )
    )
    return keyword_id if inserted is not None else None


async def enable_keyword(keyword_id: str, enabled: bool) -> None:
    """Bật/tắt từ khoá theo id, không kiểm tra nền tảng (luồng tự dọn từ khoá)."""
    await d1_exec(update(keywords).where(keywords.c.id == keyword_id).values(enabled=1 if enabled else 0))


async def movie_of_keyword(keyword_id: str) -> dict[str, Any] | None:
    """Thông tin phim (MOVIE_CONTEXT_FIELDS) của phim sở hữu từ khoá; None khi không có hoặc D1 lỗi."""
    rows = await d1_exec(
        select(*(movies.c[f] for f in MOVIE_CONTEXT_FIELDS))
        .select_from(_KEYWORD_JOIN)
        .where(keywords.c.id == keyword_id),
        quiet=True,
    )
    return rows[0] if rows else None


async def list_keywords(platform: str) -> list[dict[str, Any]]:
    """Mọi từ khoá đang bật của nền tảng này, kèm tên phim - cấp dữ liệu cho ô chọn từ khoá
    trên dashboard (GET /<platform>/keywords) để một lần kích hoạt crawl tay có thể nhắm
    vào một từ khoá thay vì "mọi từ khoá đang bật của nền tảng này" (xem
    get_enabled_keywords bên dưới, vẫn dùng cho trường hợp chạy hàng loạt đó)."""
    rows = await d1_exec(
        select(*_KEYWORD_ROW)
        .select_from(_KEYWORD_JOIN)
        .where(keywords.c.platform == platform, keywords.c.enabled == 1, movies.c.enabled == 1)
        .order_by(movies.c.title, keywords.c.keyword)
    )
    return rows or []


async def get_keyword(keyword_id: str, platform: str, *, include_disabled: bool = False) -> dict[str, Any] | None:
    """Một từ khoá đang bật theo id, trên nền tảng đã cho, join với cờ enabled của phim -
    giống những gì KeywordRepository.get() + tra phim của Postgres (đã xoá) từng làm cho
    nút kích hoạt "chạy một từ khoá". Tham số platform là bắt buộc, không phải tình cờ:
    mỗi nền tảng có router riêng (xem app/api/routes/facebook.py + platform_scraper.py)
    chỉ muốn từ khoá của chính nó - một keyword_id thuộc nền tảng khác không được âm thầm
    khớp ở đây.

    include_disabled=True (ingest): nhận cả từ khoá đã tắt - bài đang trên đường về khi keyword_yield/người dùng vừa tắt
    từ khoá vẫn là bài thật của phim, trước đây bị bỏ với lý do unknown_keyword_id. Phim vẫn phải đang bật."""
    stmt = (
        select(
            *_KEYWORD_ROW,
            keywords.c.platform,
            movies.c.director.label("movie_director"),
            movies.c.cast.label("movie_cast"),
            movies.c.distributor.label("movie_distributor"),
        )
        .select_from(_KEYWORD_JOIN)
        .where(keywords.c.id == keyword_id, keywords.c.platform == platform, movies.c.enabled == 1)
    )
    if not include_disabled:
        stmt = stmt.where(keywords.c.enabled == 1)
    rows = await d1_exec(stmt)
    return rows[0] if rows else None


async def get_enabled_keywords(platform: str, movie_id: str | None = None) -> list[dict[str, Any]]:
    """Mọi từ khoá đang bật trên nền tảng đã cho (có thể giới hạn trong một phim) mà phim
    của nó cũng đang bật - dùng cho cả nút "chạy mọi từ khoá của một phim" lẫn lời gọi
    cron hằng ngày của một nền tảng ("chạy mọi thứ của nền tảng này")."""
    stmt = (
        select(keywords.c.id, keywords.c.movie_id, keywords.c.platform, keywords.c.keyword)
        .select_from(_KEYWORD_JOIN)
        .where(keywords.c.platform == platform, keywords.c.enabled == 1, movies.c.enabled == 1)
    )
    if movie_id:
        stmt = stmt.where(keywords.c.movie_id == movie_id)
    rows = await d1_exec(stmt)
    return rows or []


async def set_keyword_enabled(platform: str, keyword_id: str, enabled: bool) -> dict[str, Any] | None:
    """Bật/tắt một từ khoá - cho người vận hành tạm dừng một từ khoá cũ/dùng một lần (hoặc
    một lô vừa thêm để thử) mà không xoá, để lịch hằng ngày của nền tảng
    (get_enabled_keywords ở trên) lấy đúng tập mong muốn. platform là phạm vi phòng thủ,
    không tự nó là key tra cứu - keyword_id vốn đã duy nhất - để một platform không khớp
    trên URL không thể âm thầm bật/tắt dòng của nền tảng khác. Trả về dòng đã cập nhật,
    hoặc None nếu id không tồn tại dưới nền tảng đó, hoặc việc ghi thất bại."""
    updated = await d1_exec(
        update(keywords)
        .where(keywords.c.id == keyword_id, keywords.c.platform == platform)
        .values(enabled=1 if enabled else 0)
    )
    if updated is None:
        logger.warning("d1_set_keyword_enabled_failed", platform=platform, keyword_id=keyword_id, enabled=enabled)
        return None
    rows = await d1_exec(
        select(*_KEYWORD_ROW, keywords.c.enabled)
        .select_from(_KEYWORD_JOIN)
        .where(keywords.c.id == keyword_id, keywords.c.platform == platform)
    )
    return rows[0] if rows else None
