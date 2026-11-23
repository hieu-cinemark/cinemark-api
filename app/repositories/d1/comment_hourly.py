"""Bình luận theo phim x giờ (giờ Việt Nam của posted_at) cho Attack Detection - dấu hiệu A (chê dồn dập).

    movie_comment_hourly (movie_id, hour, total, negative, run)

- hour: 'YYYY-MM-DDTHH' giờ Việt Nam của c.posted_at. Đếm theo GIỜ ĐĂNG, không theo first_seen_at: thu bù dữ liệu cũ sẽ
  tạo đợt tăng giả (docs/review-angles-va-attack.md mục 2.4 A);
- chỉ bình luận thuộc bài liên quan (relevant_post);
- negative = bình luận chê có nội dung (bỏ nhãn 'rule' - sticker/icon/tag tên).

Tính lại theo khoảng giờ cùng mẫu comment_rollup.rebuild_day: upsert số mới của các giờ rồi xoá các dòng của khoảng đó
không còn xuất hiện (đánh dấu bằng `run`) - không có lúc nào khoảng đó trống. Lịch: scheduler/attack.py mỗi 10 phút tính lại
3 giờ gần nhất (bình luận về muộn vẫn vào đúng giờ đăng).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import Column, Integer, Table, Text, and_, case, delete, func, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateTable

from app.repositories.d1.post_filters import relevant_post
from app.repositories.d1.sql import d1_exec, metadata
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import posts as posts_table

movie_comment_hourly = Table(
    "movie_comment_hourly",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("hour", Text, primary_key=True),
    Column("total", Integer, nullable=False),
    Column("negative", Integer, nullable=False),
    Column("run", Text, nullable=False),
)

_ready = False


async def ensure_schema() -> bool:
    global _ready
    if not _ready:
        _ready = await d1_exec(CreateTable(movie_comment_hourly, if_not_exists=True), timeout=60.0) is not None
    return _ready


def vn_hour(at: datetime) -> str:
    """'YYYY-MM-DDTHH' giờ Việt Nam - cùng dạng cột hour."""
    return (at.astimezone(UTC) + timedelta(hours=7)).strftime("%Y-%m-%dT%H")


def upsert_statement(start: datetime, end: datetime, run: str):
    """INSERT ... SELECT ... ON CONFLICT cho các giờ trong [start, end) (UTC) - tách riêng để test bằng SQLite."""
    c, p, h = comments_table.alias("c"), posts_table.alias("p"), movie_comment_hourly
    hour = func.strftime("%Y-%m-%dT%H", c.c.posted_at, "+7 hours")
    negative = func.sum(
        case((and_(c.c.sentiment == "negative", func.coalesce(c.c.sentiment_source, "") != "rule"), 1), else_=0)
    )
    rows = (
        select(p.c.movie_id, hour, func.count(), negative, func.cast(run, Text))
        .select_from(c.join(p, p.c.id == c.c.post_id))
        .where(
            c.c.posted_at >= start.astimezone(UTC).isoformat(timespec="seconds"),
            c.c.posted_at < end.astimezone(UTC).isoformat(timespec="seconds"),
            p.c.movie_id.is_not(None),
            relevant_post(p),
        )
        .group_by(p.c.movie_id, hour)
    )
    stmt = insert(h).from_select(["movie_id", "hour", "total", "negative", "run"], rows)
    return stmt.on_conflict_do_update(
        index_elements=[h.c.movie_id, h.c.hour],
        set_={"total": stmt.excluded.total, "negative": stmt.excluded.negative, "run": stmt.excluded.run},
    )


def cleanup_statement(start: datetime, end: datetime, run: str):
    """Xoá dòng của khoảng [start, end) không thuộc lượt `run` (giờ không còn bình luận - bài bị ẩn/xoá)."""
    h = movie_comment_hourly
    return delete(h).where(h.c.hour >= vn_hour(start), h.c.hour < vn_hour(end), h.c.run != run)


async def rebuild_hours(start: datetime, end: datetime) -> bool:
    """Tính lại các giờ trong [start, end) (UTC) cho mọi phim. False khi D1 lỗi (giữ số cũ của khoảng đó)."""
    if not await ensure_schema():
        return False
    run = uuid.uuid4().hex[:12]
    if await d1_exec(upsert_statement(start, end, run), timeout=120.0) is None:
        return False
    return await d1_exec(cleanup_statement(start, end, run), timeout=60.0) is not None
