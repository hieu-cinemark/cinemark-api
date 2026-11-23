"""Bản chụp hằng ngày quy mô hashtag TikTok (tổng video / lượt xem do TikTok báo) trong D1 dùng chung với Cinemark Worker.

Producer là cinemark-api (job hằng ngày, app/services/omar/omar_hashtag_stats.py); Worker đọc để vẽ thẻ "Quy mô TikTok" và biểu
đồ tăng trưởng ở Social Topic. Mỗi (ngày, từ khoá) một dòng; chạy lại trong ngày thì ghi đè."""

from sqlalchemy import Column, Index, Integer, Table, Text, func, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateIndex, CreateTable

from app.repositories.d1.sql import d1_exec, metadata

hashtag_stats = Table(
    "tiktok_hashtag_stats",
    metadata,
    Column("day", Text, primary_key=True),  # ngày giờ Việt Nam, YYYY-MM-DD
    Column("keyword_id", Text, primary_key=True),
    Column("movie_id", Text, nullable=False),
    Column("hashtag", Text, nullable=False),  # tên không có "#", đúng chữ TikTok trả về
    Column("hashtag_id", Text),
    Column("video_count", Integer, nullable=False),
    Column("view_count", Integer, nullable=False),
    Column("captured_at", Text, nullable=False),
)

by_movie_day = Index("tiktok_hashtag_stats_movie_day", hashtag_stats.c.movie_id, hashtag_stats.c.day)

_ready = False


async def ensure_table() -> None:
    """Tạo bảng và chỉ mục theo phim (Worker đọc theo movie_id) nếu chưa có; chỉ chạy một lần mỗi tiến trình."""
    global _ready
    if _ready:
        return
    await d1_exec(CreateTable(hashtag_stats, if_not_exists=True))
    await d1_exec(CreateIndex(by_movie_day, if_not_exists=True))
    _ready = True


async def upsert_snapshot(
    *,
    day: str,
    keyword_id: str,
    movie_id: str,
    hashtag: str,
    hashtag_id: str | None,
    video_count: int,
    view_count: int,
    captured_at: str,
) -> None:
    await ensure_table()
    stmt = insert(hashtag_stats).values(
        day=day,
        keyword_id=keyword_id,
        movie_id=movie_id,
        hashtag=hashtag,
        hashtag_id=hashtag_id,
        video_count=video_count,
        view_count=view_count,
        captured_at=captured_at,
    )
    await d1_exec(
        stmt.on_conflict_do_update(
            index_elements=["day", "keyword_id"],
            set_={
                "movie_id": stmt.excluded.movie_id,
                "hashtag": stmt.excluded.hashtag,
                "hashtag_id": stmt.excluded.hashtag_id,
                "video_count": stmt.excluded.video_count,
                "view_count": stmt.excluded.view_count,
                "captured_at": stmt.excluded.captured_at,
            },
        )
    )


async def latest_by_keyword() -> dict[str, dict]:
    """keyword_id -> bản chụp mới nhất {day, video_count, view_count}; rỗng khi bảng chưa có."""
    await ensure_table()
    latest = (
        select(hashtag_stats.c.keyword_id, func.max(hashtag_stats.c.day).label("day"))
        .group_by(hashtag_stats.c.keyword_id)
        .subquery()
    )
    rows = await d1_exec(
        select(
            hashtag_stats.c.keyword_id, hashtag_stats.c.day, hashtag_stats.c.video_count, hashtag_stats.c.view_count
        ).join(
            latest,
            (hashtag_stats.c.keyword_id == latest.c.keyword_id) & (hashtag_stats.c.day == latest.c.day),
        ),
        quiet=True,
    )
    return {row["keyword_id"]: row for row in rows or []}
