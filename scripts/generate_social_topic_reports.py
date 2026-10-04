"""Tạo lại report social listening "top 10 topic" bằng AI cho mọi phim đang bật (hoặc
một phim qua --movie-id), ghi đè social_topic_reports theo movie_id. Đây cố ý là job
theo lô chạy định kỳ, không tính theo mỗi lượt xem trang - gom topic comment của một
phim là một lời gọi AI tốn kém (Kira mặc định, hoặc Bee nếu chọn trong Settings), và
chủ đề thảo luận của một phim không thay đổi đáng kể trong vài giờ với lượng comment
hiện tại. Chạy hằng ngày qua cron (xem scripts/trigger_scheduled_crawl.sh); dùng
--movie-id để tạo lại một lần khi cần (ví dụ ngay sau một lượt backfill, không phải
chờ chu kỳ cron sau) - nút "Tạo report" bấm tay trên dashboard (POST
/movies/{id}/generate-report) cũng gọi đúng cho một phim như vậy.

Thuật toán thật theo từng phim nằm ở app/services/social_topic.py, dùng chung với nút
đó - script này chỉ là lượt quét các phim đang bật + CLI bọc quanh nó.

Cách dùng:
    python -m scripts.generate_social_topic_reports
    python -m scripts.generate_social_topic_reports --movie-id abc123
    python -m scripts.generate_social_topic_reports --dry-run
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.logging import get_logger
from app.services.d1 import list_movies
from app.services.social_topic import generate_report_for_movie, get_movie_for_report

logger = get_logger(__name__)


async def generate(movie_id: str | None, dry_run: bool) -> None:
    if movie_id:
        movie = await get_movie_for_report(movie_id)
        if movie is None:
            logger.warning("report_movie_not_found", movie_id=movie_id)
            return
        movies = [movie]
    else:
        movies = await list_movies()

    logger.info("generate_social_topic_reports_started", count=len(movies), dry_run=dry_run)
    for movie in movies:
        await generate_report_for_movie(movie, dry_run=dry_run)
    logger.info("generate_social_topic_reports_finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--movie-id", help="Regenerate only this movie (skips the enabled-movies list)")
    parser.add_argument("--dry-run", action="store_true", help="Log what would happen, don't write anything")
    args = parser.parse_args()
    asyncio.run(generate(args.movie_id, args.dry_run))
