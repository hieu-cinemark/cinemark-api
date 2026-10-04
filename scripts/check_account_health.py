"""Chạy evaluate_account_health() cho mọi tài khoản và lưu kết quả - bản tự động tương
ứng với nút "Check" bấm tay trên dashboard (xem POST
/settings/accounts/{id}/check trong app/api/routes/settings.py, làm đúng hai bước đó
cho một tài khoản). Không có script này thì tín hiệu sức khoẻ chỉ cập nhật khi có người
nhớ bấm nút - đúng lỗ hổng "không ai theo dõi" đã để việc thiếu mapper của TikTok (và
các tài khoản Threads) bị bỏ sót trước đây (xem _note_drop trong
app/workers/ingest_consumer/main.py cho cùng bài học áp dụng với một tín hiệu khác).

Chỉ cảnh báo Telegram khi *chuyển sang* warning/disabled - không phải mỗi lần chạy -
để một sự cố kéo dài không cảnh báo lại ở mỗi chu kỳ cron mà nó còn hỏng (cùng lý do
"cảnh báo một lần, không phải mỗi lần xảy ra" như _note_drop). Tắt tay qua dashboard
cũng sẽ cảnh báo một lần ở chu kỳ cron kế tiếp - đó là chuyện bình thường, không phải
lỗi: tài khoản quả thật đang bị tắt.

Chạy định kỳ qua cron (xem scripts/trigger_scheduled_crawl.sh):
    python -m scripts.check_account_health
"""

from __future__ import annotations

import asyncio

from app.clients.telegram import send_telegram_message
from app.core.logging import get_logger
from app.services.account_health import evaluate_account_health
from app.services.platform_config_db import list_accounts, update_account_check_result

logger = get_logger(__name__)

_DEGRADED_STATUSES = {"warning", "disabled"}


async def check() -> None:
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


if __name__ == "__main__":
    asyncio.run(check())
