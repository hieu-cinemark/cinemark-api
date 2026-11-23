"""Phần dùng chung của các nhịp lịch: giờ Việt Nam, kiểm tra tới giờ, chạy nền và khoá chống chạy lặp."""

from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.services.accounts.config_db import schedules as schedules_cfg

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
POLL_INTERVAL_SECONDS = 30
# Một lượt bị lỡ vì vòng lặp bị kẹt hoặc máy đang ngủ vẫn được bắn nếu vòng lặp hồi phục trong khoảng này sau run_time.
CATCH_UP_MINUTES = 30
KEY_PREFIX = f"{REDIS_KEY_PREFIX}scheduler:"

# Giữ tham chiếu mạnh tới các task bắn-rồi-quên - event loop chỉ giữ tham chiếu yếu, nên task không ai tham chiếu có thể bị
# garbage-collect giữa chừng.
_background_tasks: set[asyncio.Task] = set()


def is_due(run_time: str, now: datetime) -> bool:
    """`run_time` "HH:MM" đã tới và chưa quá CATCH_UP_MINUTES."""
    try:
        hour, minute = (int(part) for part in run_time.split(":", 1))
    except ValueError:
        return False
    late = (now.hour * 60 + now.minute) - (hour * 60 + minute)
    return 0 <= late <= CATCH_UP_MINUTES


def spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def claim(name: str, ttl_seconds: float, value: str = "1") -> bool:
    """Khoá Redis `scheduler:<name>` sống ttl_seconds: True khi lượt này giành được (restart không chạy lặp)."""
    return bool(await get_redis_client().set(f"{KEY_PREFIX}{name}", value, nx=True, ex=max(1, int(ttl_seconds))))


def cycle_ttl(hours: float) -> int:
    """Khoá sống một chu kỳ `hours` giờ, trừ 2 phút cho lệch nhịp, tối thiểu 1 phút."""
    return max(60, int(hours * 3600) - 120)


async def facebook_crawl_enabled() -> bool:
    """Việc nền có đăng nhập cần bật riêng VÀ có lịch đang bật.

    Khung giờ khai báo rõ gọi thẳng crawl_tick. Cào sâu hằng giờ và làm mới bài theo dõi hằng ngày không được lặng lẽ biến
    một khung giờ mỗi ngày thành nhiều phiên tài khoản.
    """
    if not settings.facebook_background_crawl_enabled:
        return False
    return any(
        sched["platform"] == "facebook" and sched["enabled"] for sched in await schedules_cfg.list_crawl_schedules()
    )
