"""Lịch tính lại bảng tổng hợp bình luận theo phim x ngày (app/repositories/d1/comment_rollup.py), chạy trong bộ lập lịch:

- mỗi 10 phút: hôm nay + hôm qua (giờ VN) - bình luận mới về và nhãn cảm xúc/giai đoạn được gán trong ~48 giờ đầu;
- mỗi đêm (NIGHTLY_HOUR giờ VN): NIGHTLY_DAYS ngày gần nhất, bắt nhãn liên quan của bài được gán lại muộn
  (relabel_post_relevance) và bình luận cũ được cào thêm.

Mỗi ngày là một lượt riêng (vài giây trên D1), nghỉ PAUSE_SECONDS giữa các ngày để ingest và trang web không phải chờ.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.core.logging import get_logger
from app.repositories.d1.comment_rollup import rebuild_day

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
RECENT_DAYS = 2
NIGHTLY_DAYS = 60
NIGHTLY_HOUR = 3
PAUSE_SECONDS = 2.0

_lock = asyncio.Lock()


def vn_days(count: int, now: datetime | None = None) -> list[str]:
    """`count` ngày VN gần nhất, mới nhất trước."""
    today = (now or datetime.now(TIMEZONE)).astimezone(TIMEZONE).date()
    return [(today - timedelta(days=i)).isoformat() for i in range(count)]


async def rebuild_days(days: list[str]) -> dict[str, int]:
    """Tính lại lần lượt từng ngày; bỏ lượt nếu một lượt khác đang chạy (lượt đêm dài không chồng lên lượt 10 phút)."""
    if _lock.locked():
        return {"skipped": 1}
    async with _lock:
        done = failed = 0
        for i, day in enumerate(days):
            if i:
                await asyncio.sleep(PAUSE_SECONDS)
            if await rebuild_day(day):
                done += 1
            else:
                failed += 1
        logger.info("comment_rollup_rebuilt", days=len(days), done=done, failed=failed, newest=days[0] if days else None)
        return {"done": done, "failed": failed}


async def refresh_recent() -> dict[str, int]:
    return await rebuild_days(vn_days(RECENT_DAYS))


async def refresh_nightly() -> dict[str, int]:
    return await rebuild_days(vn_days(NIGHTLY_DAYS))
