"""Phần dùng chung của các bảng tổng hợp theo ngày (bình luận, bài): ngày giờ Việt Nam và lịch tính lại.

Ngày "VN" là ngày theo giờ Việt Nam (UTC+7), dạng 'YYYY-MM-DD'. Các hàm theo mili giây (day_start, day_label, utc_iso, parse_ms)
giữ đúng cách tính của Worker (Date.parse / toISOString) để bảng tổng hợp bài ra cùng số với bản TS - xem post_rollup_core.py.
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.services.stats.rollup.constant import NIGHTLY_DAYS, RECENT_DAYS

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

MINUTE_MS = 60_000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS
_VN_OFFSET_MS = 7 * HOUR_MS
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

RebuildDays = Callable[[list[str]], Awaitable[dict[str, int]]]


def day_start(day: str) -> int:
    """00:00 giờ VN của ngày `day`, mili giây UTC."""
    return math.floor(datetime.fromisoformat(f"{day}T00:00:00+07:00").timestamp() * 1000)


def day_label(ms: int) -> str:
    """Ngày VN của một thời điểm mili giây."""
    return (_EPOCH + timedelta(milliseconds=ms + _VN_OFFSET_MS)).date().isoformat()


def utc_iso(ms: int) -> str:
    """Như Date.toISOString(): '2026-11-26T17:00:00.000Z'."""
    moment = _EPOCH + timedelta(milliseconds=ms)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def parse_ms(value: Any) -> int | None:
    """Như Date.parse của JS cho các dạng thời gian trong DB ('...+00:00', '...Z', '...000Z'); không có múi giờ thì coi là UTC
    (Worker chạy ở UTC). None khi không đọc được."""
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return math.floor(parsed.timestamp() * 1000)


def vn_days(count: int, now: datetime | None = None) -> list[str]:
    """`count` ngày VN gần nhất, mới nhất trước."""
    today = (now or datetime.now(TIMEZONE)).astimezone(TIMEZONE).date()
    return [(today - timedelta(days=i)).isoformat() for i in range(count)]


async def refresh_recent(rebuild_days: RebuildDays, include_yesterday: bool = True) -> dict[str, int]:
    """Lượt thường: hôm nay, thêm hôm qua khi `include_yesterday` (bài/bình luận về trễ sau nửa đêm)."""
    return await rebuild_days(vn_days(RECENT_DAYS if include_yesterday else 1))


async def refresh_nightly(rebuild_days: RebuildDays) -> dict[str, int]:
    """Lượt đêm: NIGHTLY_DAYS ngày gần nhất (nhãn liên quan gán lại muộn, dữ liệu cũ cào thêm)."""
    return await rebuild_days(vn_days(NIGHTLY_DAYS))
