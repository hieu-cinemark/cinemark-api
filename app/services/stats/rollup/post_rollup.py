"""Lịch tính bảng tổng hợp bài theo phim x nền tảng x ngày đăng (movie_post_daily), chạy trong bộ lập lịch cạnh bảng bình luận.

Chuyển từ Worker sang đây 27/11/2026 để gom việc tính các bảng tổng hợp về một chỗ. Worker chỉ tính được 1 ngày mỗi 10 phút
(144 lượt/ngày) trong khi nhịp tính lại cần ~330 lượt/ngày; ở đây mỗi nhịp tính tối đa POST_DAYS_PER_TICK ngày.

settings.post_rollup_mode:
- "off" (mặc định): không chạy;
- "shadow": ghi vào bảng _shadow để đối chiếu với bảng của Worker (scripts/jobs/compare_post_rollup.py);
- "live": ghi vào bảng thật - chỉ bật cùng lần deploy gỡ processPostRollup khỏi worker.ts của cinemark.
"""

from __future__ import annotations

import asyncio
import time

from app.core.logging import get_logger
from app.repositories.d1 import post_rollup as repo
from app.services.stats.rollup.constant import PAUSE_SECONDS, POST_DAYS_PER_TICK
from app.services.stats.rollup.post_rollup_core import build_rollup_rows, is_organic, next_rollup_day, rollup_handle

logger = get_logger(__name__)

MODES = ("shadow", "live")

_lock = asyncio.Lock()


async def compute_and_write(mode: str, day: str, now_ms: int) -> int | None:
    """Tính lại một ngày đăng và ghi. Số dòng đã ghi, hoặc None khi D1 lỗi."""
    if not await repo.ensure_schema(mode):
        return None
    loaded = await repo.load_day(day)
    if loaded is None:
        logger.warning("post_rollup_day_failed", mode=mode, day=day, step="load")
        return None
    posts, snapshots, tiers = loaded
    for post in posts:
        post["organic"] = is_organic(rollup_handle(post["platform"], post.get("author"), post.get("url")), tiers)
    rows = build_rollup_rows(day, posts, snapshots, now_ms)
    if not await repo.write_day(mode, day, rows, now_ms):
        return None
    return len(rows)


async def process(mode: str, max_days: int = POST_DAYS_PER_TICK) -> dict[str, int]:
    """Tính tối đa `max_days` ngày đến hạn (ngày chưa dựng trước, rồi ngày quá hạn lâu nhất). Bỏ lượt nếu lượt trước chưa xong."""
    if mode not in MODES:
        return {"skipped": 1}
    if _lock.locked():
        return {"skipped": 1}
    async with _lock:
        if not await repo.ensure_schema(mode):
            return {"failed": 1}
        done = failed = 0
        first = None
        for i in range(max_days):
            computed = await repo.load_computed(mode)
            if computed is None:
                failed += 1
                break
            now_ms = int(time.time() * 1000)
            picked = next_rollup_day(computed, now_ms)
            if picked is None:
                break
            day, _backlog = picked
            if i:
                await asyncio.sleep(PAUSE_SECONDS)
            if await compute_and_write(mode, day, now_ms) is None:
                failed += 1
                break  # D1 lỗi/quá tải: dừng lượt này, lượt sau thử lại
            done += 1
            first = first or day
        if done or failed:
            logger.info("post_rollup_rebuilt", mode=mode, days=done, failed=failed, first=first)
        return {"done": done, "failed": failed}
