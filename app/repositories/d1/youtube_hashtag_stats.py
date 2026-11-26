"""Bản chụp hằng ngày quy mô hashtag YouTube (tổng video / số kênh do trang hashtag YouTube báo) trong D1 dùng chung với
Cinemark Worker.

Producer là cinemark-api (job hằng ngày, app/services/youtube_hashtag_stats.py); Worker đọc cho thẻ "Quy mô trên nền tảng" ở
Social Topic. Hashtag là các từ khoá TikTok dạng "#..." của phim (YouTube không có danh sách hashtag riêng), nên keyword_id
trỏ tới từ khoá TikTok đó. Mỗi (ngày, từ khoá) một dòng; chạy lại trong ngày thì ghi đè."""

from sqlalchemy import Column, Index, Integer, Table, Text, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateIndex, CreateTable

from app.repositories.d1.sql import d1_exec, metadata

youtube_hashtag_stats = Table(
    "youtube_hashtag_stats",
    metadata,
    Column("day", Text, primary_key=True),  # ngày giờ Việt Nam, YYYY-MM-DD
    Column("keyword_id", Text, primary_key=True),
    Column("movie_id", Text, nullable=False),
    Column("hashtag", Text, nullable=False),  # tên không có "#"
    Column("video_count", Integer, nullable=False),
    Column("channel_count", Integer, nullable=False),
    Column("captured_at", Text, nullable=False),
)

by_movie_day = Index("youtube_hashtag_stats_movie_day", youtube_hashtag_stats.c.movie_id, youtube_hashtag_stats.c.day)

_ready = False


async def ensure_table() -> None:
    """Tạo bảng và chỉ mục theo phim nếu chưa có; chỉ chạy một lần mỗi tiến trình."""
    global _ready
    if _ready:
        return
    await d1_exec(CreateTable(youtube_hashtag_stats, if_not_exists=True))
    await d1_exec(CreateIndex(by_movie_day, if_not_exists=True))
    _ready = True


async def upsert_snapshot(
    *, day: str, keyword_id: str, movie_id: str, hashtag: str, video_count: int, channel_count: int, captured_at: str
) -> None:
    await ensure_table()
    stmt = insert(youtube_hashtag_stats).values(
        day=day,
        keyword_id=keyword_id,
        movie_id=movie_id,
        hashtag=hashtag,
        video_count=video_count,
        channel_count=channel_count,
        captured_at=captured_at,
    )
    await d1_exec(
        stmt.on_conflict_do_update(
            index_elements=["day", "keyword_id"],
            set_={
                "movie_id": stmt.excluded.movie_id,
                "hashtag": stmt.excluded.hashtag,
                "video_count": stmt.excluded.video_count,
                "channel_count": stmt.excluded.channel_count,
                "captured_at": stmt.excluded.captured_at,
            },
        )
    )


async def known_hashtags() -> list[dict]:
    """[{movie_id, hashtag}] - mọi hashtag YouTube từng báo số video thật (từ 100 video trở lên), không lặp."""
    await ensure_table()
    stmt = select(youtube_hashtag_stats.c.movie_id, youtube_hashtag_stats.c.hashtag).distinct()
    return await d1_exec(stmt, quiet=True) or []
