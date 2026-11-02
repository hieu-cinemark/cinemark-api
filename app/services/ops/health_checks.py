"""Hai việc kiểm tra định kỳ trước đây chạy qua scripts/trigger_scheduled_crawl.sh (crontab, đã bỏ 02/11/2026) - giờ
scheduler (app/services/crawl/scheduler.py, _health_checks_tick) gọi:

  - check_account_health(): evaluate_account_health() cho mọi tài khoản + lưu kết quả - bản tự động của nút "Check" trên
    dashboard (POST /settings/accounts/{id}/check). Chỉ cảnh báo Telegram khi tài khoản *chuyển sang* warning/disabled,
    không phải mỗi lần chạy, để sự cố kéo dài không cảnh báo lại mỗi chu kỳ.
  - check_volume_anomaly(): số bài hôm nay của từng nền tảng so với trung vị 7 ngày trước - bắt trường hợp crawl vẫn
    "thành công" nhưng response đổi dạng nên trích ra ít/nhiều hẳn (không có exception nào để cảnh báo khác thấy).
    Ngày = ngày UTC của stats_platform_daily (date('now') trên D1), nên chỉ chạy gần cuối ngày UTC.

CLI chạy tay: python -m scripts.jobs.check_account_health / python -m scripts.jobs.check_volume_anomaly."""

from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import UTC, datetime

from app.clients.telegram import send_telegram_message
from app.core.logging import get_logger
from app.repositories.d1.store import get_post_timeseries
from app.services.accounts.account_health import evaluate_account_health
from app.services.accounts.platform_config_db import list_accounts, update_account_check_result

logger = get_logger(__name__)

_DEGRADED_STATUSES = {"warning", "disabled"}

BASELINE_DAYS = 7
# Dưới tỉ lệ này so với trung vị gốc -> "nền tảng này có âm thầm hỏng không". Trên bội số này -> "đây có phải đột biến do
# spam/bug không". Cả hai chỉ là điểm xuất phát - chỉnh lại khi đã thấy một tuần dữ liệu thật.
LOW_RATIO = 0.3
HIGH_RATIO = 3.0
MIN_BASELINE_TO_ALERT = 5  # nền tảng chỉ có 1-2 bài/ngày thì quá nhiễu để cảnh báo chỉ dựa vào tỉ lệ


async def check_account_health() -> None:
    accounts = await list_accounts()
    for account in accounts:
        previous_status = account["last_check_status"]
        status = await evaluate_account_health(account)
        await update_account_check_result(account["id"], status=status)

        newly_degraded = status in _DEGRADED_STATUSES and previous_status not in _DEGRADED_STATUSES
        if newly_degraded:
            await send_telegram_message(
                f"⚠️ Account health: {account['platform']}/{account['account_id']} is now "
                f"'{status}' (was '{previous_status or 'never checked'}') - check the Settings page."
            )

        logger.info(
            "account_health_checked",
            platform=account["platform"],
            account_id=account["account_id"],
            status=status,
            previous_status=previous_status,
        )


async def check_volume_anomaly() -> None:
    rows = await get_post_timeseries(BASELINE_DAYS + 1)
    by_platform: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        by_platform[row["platform"]][row["day"]] = row["count"]

    today = datetime.now(UTC).date().isoformat()

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
