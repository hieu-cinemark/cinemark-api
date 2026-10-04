"""So số bài hôm nay của từng nền tảng với trung vị của 7 ngày trước đó - bắt các trường
hợp "crawl đã chạy và báo thành công, nhưng dạng response của nền tảng âm thầm thay
đổi và giờ trích xuất ra ít/nhiều hơn hẳn bình thường" mà các cảnh báo dựa trên lỗi ở
chỗ khác trong project không thấy được (trong tình huống đó không có exception nào
được raise).

Chạy hằng ngày qua cron, ngay sau lần kích hoạt crawl theo lịch:
    python -m scripts.check_volume_anomaly
"""

from __future__ import annotations

import asyncio
import statistics
from collections import defaultdict
from datetime import date

from app.clients.telegram import send_telegram_message
from app.core.logging import get_logger
from app.services.d1 import get_post_timeseries

logger = get_logger(__name__)

BASELINE_DAYS = 7
# Dưới tỉ lệ này so với trung vị gốc -> "nền tảng này có âm thầm hỏng không". Trên bội
# số này -> "đây có phải đột biến do spam/bug không". Cả hai chỉ là điểm xuất phát -
# chỉnh lại khi đã thấy một tuần dữ liệu thật.
LOW_RATIO = 0.3
HIGH_RATIO = 3.0
MIN_BASELINE_TO_ALERT = 5  # nền tảng chỉ có 1-2 bài/ngày thì quá nhiễu để cảnh báo chỉ dựa vào tỉ lệ


async def check() -> None:
    rows = await get_post_timeseries(BASELINE_DAYS + 1)
    by_platform: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        by_platform[row["platform"]][row["day"]] = row["count"]

    today = date.today().isoformat()

    for platform, counts_by_day in by_platform.items():
        today_count = counts_by_day.get(today, 0)
        baseline_counts = [c for day, c in counts_by_day.items() if day != today]
        if len(baseline_counts) < 3:
            continue  # chưa đủ lịch sử để đánh giá

        baseline = statistics.median(baseline_counts)
        if baseline < MIN_BASELINE_TO_ALERT:
            continue

        ratio = today_count / baseline
        if ratio < LOW_RATIO:
            await send_telegram_message(
                f"⚠️ Volume anomaly: {platform} got only {today_count} posts today "
                f"vs a {baseline:.0f}/day baseline ({ratio:.0%}) - check if extraction silently broke."
            )
        elif ratio > HIGH_RATIO:
            await send_telegram_message(
                f"⚠️ Volume anomaly: {platform} got {today_count} posts today "
                f"vs a {baseline:.0f}/day baseline ({ratio:.0%}) - check for spam/duplicate crawls."
            )
        logger.info("volume_check", platform=platform, today=today_count, baseline=baseline, ratio=round(ratio, 2))


if __name__ == "__main__":
    asyncio.run(check())
