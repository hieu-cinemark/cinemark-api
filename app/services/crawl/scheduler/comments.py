"""Lượt cào bình luận mỗi giờ (bảng `comment_crawl_schedules`, app/services/crawl/comment_planner.py)."""

from __future__ import annotations

from datetime import datetime

from app.core.logging import get_logger
from app.services.accounts.config_db import schedules as schedules_cfg
from app.services.crawl.comment_planner import run_comment_round, run_youtube_comment_round
from app.services.crawl.platforms import COMMENT_CRAWL_PLATFORMS
from app.services.crawl.scheduler.core import TIMEZONE, spawn

logger = get_logger(__name__)

# Chạy từ phút này (cho lượt crawl bài đầu giờ kịp đổ bài mới về). Giờ đã chạy của mỗi nền tảng mất khi khởi động lại: tệ
# nhất chạy thêm một lượt, các bài vừa xếp hàng đã có lease comments_queued_at nên không bị quét trùng.
COMMENT_ROUND_MINUTE = 5
comment_round_done: dict[str, str] = {}
# Số bài nóng mỗi phim = top_n của lịch, kẹp trong khoảng này. top_n trước 2026-10-08 nghĩa là "top N bài mỗi từ khoá" (mặc
# định 100) - giá trị lớn hơn mức trần coi là cấu hình cũ và dùng mặc định.
_HOT_PER_MOVIE_RANGE = (3, 30)
_HOT_PER_MOVIE_DEFAULT = 15


def sample_hours(run_time: str) -> set[int]:
    """Giờ chạy lượt mẫu phân tầng: giờ của run_time và 12 tiếng sau đó."""
    try:
        hour = int(run_time.split(":")[0]) % 24
    except ValueError, AttributeError:
        hour = 8
    return {hour, (hour + 12) % 24}


def hot_per_movie(top_n: int | None) -> int:
    low, high = _HOT_PER_MOVIE_RANGE
    value = int(top_n or _HOT_PER_MOVIE_DEFAULT)
    return max(value, low) if value <= high else _HOT_PER_MOVIE_DEFAULT


async def comments_tick() -> None:
    now = datetime.now(TIMEZONE)
    if now.minute < COMMENT_ROUND_MINUTE:
        return
    hour_key = now.strftime("%Y-%m-%dT%H")
    for sched in await schedules_cfg.list_comment_crawl_schedules():
        platform = sched["platform"]
        if (
            not sched["enabled"]
            or platform not in COMMENT_CRAWL_PLATFORMS
            or comment_round_done.get(platform) == hour_key
        ):
            continue
        # Facebook không phụ thuộc công tắc lịch cào: comment theo lịch mặc định ẩn danh, không dùng tài khoản.
        comment_round_done[platform] = hour_key
        per_movie = hot_per_movie(sched.get("top_n"))
        with_sample = now.hour in sample_hours(sched.get("run_time") or "08:00")
        logger.info(
            "scheduled_comments_firing",
            platform=platform,
            hour=hour_key,
            hot_per_movie=per_movie,
            with_sample=with_sample,
        )
        # Không await tại chỗ - một lượt chọn bài chậm cho một nền tảng không được làm trễ lịch của nền tảng khác.
        if platform == "youtube":
            spawn(run_youtube_comment_round())
        else:
            spawn(run_comment_round(platform, hot_per_movie=per_movie, with_sample=with_sample))
