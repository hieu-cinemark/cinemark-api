"""Lịch tính lại bảng tổng hợp bình luận theo phim x ngày (app/repositories/d1/comment_rollup.py), chạy trong bộ lập lịch:

- mỗi 10 phút: hôm nay + hôm qua (giờ VN) - bình luận mới về và nhãn cảm xúc/giai đoạn được gán trong ~48 giờ đầu;
- mỗi đêm (NIGHTLY_HOUR giờ VN): NIGHTLY_DAYS ngày gần nhất, bắt nhãn liên quan của bài được gán lại muộn
  (relabel_post_relevance) và bình luận cũ được cào thêm.

Mỗi ngày là một lượt riêng (vài giây trên D1), nghỉ PAUSE_SECONDS giữa các ngày để ingest và trang web không phải chờ.
"""

from __future__ import annotations

import asyncio

from app.core.logging import get_logger
from app.repositories.d1.comment_rollup import rebuild_day
from app.services.stats.rollup.constant import PAUSE_SECONDS

logger = get_logger(__name__)
_lock = asyncio.Lock()

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
        logger.info(
            "comment_rollup_rebuilt", days=len(days), done=done, failed=failed, newest=days[0] if days else None
        )
        return {"done": done, "failed": failed}
