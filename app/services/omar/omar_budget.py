"""Nhịp request và hạn mức của một lượt cào Omar: giãn cách theo request/phút, trần theo ngày, số request đồng thời."""

from __future__ import annotations

import asyncio
import time

from app.services.accounts.config_db.omar import omar_requests_cap

_CAP_REFRESH_SECONDS = 60.0


class _Pacer:
    """Rải đều thời điểm BẮT ĐẦU các request theo giới hạn mỗi phút của gói - dùng chung cho mọi worker chạy song song
    (request chạy chồng lên nhau, chỉ lúc bắt đầu bị giãn cách), nên tận dụng hết giới hạn mà không bị 429.

    Trần đặt ở tab Omar (omar_api_settings.requests_per_minute, 0 = theo gói) đọc lại mỗi phút: hạ trần là lượt đang
    chạy chậm lại ngay, không phải chờ lượt sau."""

    def __init__(self, per_minute: int) -> None:
        self.per_minute = per_minute
        self.cap = 0
        self.cap_checked_at: float | None = None
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            if self.cap_checked_at is None or now - self.cap_checked_at >= _CAP_REFRESH_SECONDS:
                self.cap_checked_at = now
                self.cap = await omar_requests_cap()
            per_minute = min(self.per_minute, self.cap) if self.cap > 0 else self.per_minute
            start = max(now, self.next_at)
            self.next_at = start + 60.0 / max(per_minute, 1) + 0.05
        if start > now:
            await asyncio.sleep(start - now)


class Budget:
    """Ngân sách request của một lượt, dùng chung cho mọi việc trong lượt (bình luận tuần, hashtag, làm mới trang 1) và
    mọi worker song song - giữ chỗ TRƯỚC khi gọi nên chạy song song không vượt."""

    def __init__(self, limit: int, parent: Budget | None = None) -> None:
        self.limit = limit
        self.used = 0
        # Ngân sách con (vd. phần bình luận tuần): mỗi request trừ cả vào ngân sách cha của lượt.
        self.parent = parent

    def take(self) -> bool:
        if self.used >= self.limit or (self.parent is not None and not self.parent.take()):
            return False
        self.used += 1
        return True

    @property
    def exhausted(self) -> bool:
        return self.used >= self.limit or (self.parent is not None and self.parent.exhausted)
