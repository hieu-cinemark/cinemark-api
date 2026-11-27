"""Việc vận hành: cảnh báo Telegram, sức khoẻ tài khoản, đối soát thống kê, report theo lịch, đồng bộ cấu hình AI sang D1."""

from __future__ import annotations

from datetime import UTC, datetime

from app.core.config import settings
from app.core.logging import get_logger
from app.services.crawl.scheduler.core import POLL_INTERVAL_SECONDS, TIMEZONE, claim, spawn
from app.services.ops import health_checks, ops_alerts
from app.services.stats.rollup import comment_rollup, helper, post_rollup
from app.services.stats.rollup.constant import NIGHTLY_HOUR
from app.services.stats.stats_reconcile import reconcile_recent_days

logger = get_logger(__name__)

# Sản lượng bất thường kiểm tra mỗi ngày lúc 18:xx UTC = gần cuối ngày UTC của stats_platform_daily.
_VOLUME_CHECK_UTC_HOUR = 18
# Đối soát bảng tổng hợp (nguồn DUY NHẤT của bảng tổng hợp - ingest không cộng từng bài) mỗi 10 phút: lượt thường chỉ tính
# hôm nay; lượt slot FULL mỗi giờ (phút :40-:49) tính cả hôm qua (bài về trễ sau nửa đêm).
_STATS_RECONCILE_EVERY_MINUTES = 10
_STATS_RECONCILE_FULL_SLOT = 4
# Lưu ở dashboard thì đã chép ngay; vòng này bắt cả thay đổi sửa thẳng trên D1.
AI_RUNTIME_SYNC_SECONDS = 600
REPORT_SCHEDULE_CHECK_SECONDS = 600
_report_schedule_running = False


async def _guarded(name: str, job) -> None:
    try:
        await job()
    except Exception as exc:  # noqa: BLE001 - lượt sau thử lại
        logger.error("ops_job_failed", job=name, error=exc)


async def ops_alerts_tick() -> None:
    """Cảnh báo vận hành (ops_alerts) mỗi settings.ops_alerts_interval_minutes phút; chạy nền vì /usage của Omar và D1 có
    thể chậm. Trừ một nhịp kiểm tra để lượt kế tiếp không trễ thêm 30 giây mỗi lần."""
    if not settings.ops_alerts_enabled:
        return
    if await claim("ops_alerts:last_run", max(60, settings.ops_alerts_interval_minutes * 60 - POLL_INTERVAL_SECONDS)):
        spawn(_guarded("ops_alerts", ops_alerts.run_checks))


async def health_checks_tick() -> None:
    """Sức khoẻ tài khoản mỗi 6 giờ + sản lượng bất thường mỗi ngày (health_checks)."""
    now = datetime.now(UTC)
    if await claim(f"account_health:{now:%Y-%m-%d}:{now.hour // 6}", 7 * 3600):
        spawn(_guarded("account_health_check", health_checks.check_account_health))
    if now.hour == _VOLUME_CHECK_UTC_HOUR and await claim(f"volume_anomaly:{now:%Y-%m-%d}", 26 * 3600):
        spawn(_guarded("volume_anomaly_check", health_checks.check_volume_anomaly))


async def stats_reconcile_tick() -> None:
    now = datetime.now(TIMEZONE)
    slot = now.minute // _STATS_RECONCILE_EVERY_MINUTES
    if not await claim(f"stats_reconcile:{now:%Y-%m-%dT%H}:{slot}", 2 * 3600):
        return
    # Mỗi câu tổng hợp có thể tới vài chục giây khi D1 bận - chạy nền, không giữ vòng lặp lịch.
    spawn(reconcile_recent_days(2 if slot == _STATS_RECONCILE_FULL_SLOT else 1))
    # Bảng tổng hợp bình luận theo phim x ngày (Social Topic / Social Buzz) cùng nhịp; lượt đêm tính lại NIGHTLY_DAYS ngày.
    if now.hour == NIGHTLY_HOUR and slot == 3:
        spawn(helper.refresh_nightly(comment_rollup.rebuild_days))
    else:
        spawn(helper.refresh_recent(comment_rollup.rebuild_days, include_yesterday=slot == _STATS_RECONCILE_FULL_SLOT))


async def post_rollup_tick() -> None:
    """Bảng tổng hợp bài theo ngày (post_rollup) mỗi 10 phút, lệch 5 phút với lượt đối soát/bảng bình luận ở trên để hai lượt
    không cùng đọc D1 một lúc. Tắt khi settings.post_rollup_mode = "off"."""
    if settings.post_rollup_mode not in post_rollup.MODES:
        return
    now = datetime.now(TIMEZONE)
    if now.minute % _STATS_RECONCILE_EVERY_MINUTES < 5:
        return
    slot = now.minute // _STATS_RECONCILE_EVERY_MINUTES
    if await claim(f"post_rollup:{now:%Y-%m-%dT%H}:{slot}", 2 * 3600):
        spawn(_guarded("post_rollup", lambda: post_rollup.process(settings.post_rollup_mode)))


async def _run_report_schedule() -> None:
    global _report_schedule_running
    from app.services.reports.report_schedule import run_due_reports

    try:
        await run_due_reports()
    except Exception as exc:  # noqa: BLE001 - lượt sau thử lại
        logger.error("scheduler_report_schedule_failed", error=exc)
    finally:
        _report_schedule_running = False


async def report_schedule_tick() -> None:
    """Report Social Topic theo ngày hẹn (app/services/reports/report_schedule.py)."""
    global _report_schedule_running
    if _report_schedule_running:
        return
    if await claim("report_schedule", REPORT_SCHEDULE_CHECK_SECONDS, datetime.now(TIMEZONE).isoformat()):
        _report_schedule_running = True
        spawn(_run_report_schedule())


async def ai_runtime_tick() -> None:
    """Chép cấu hình AI sang D1 cho Worker cinemark (app/services/ops/ai_runtime_sync.py)."""
    from app.services.ops.ai_runtime_sync import sync_ai_runtime

    if await claim("ai_runtime_sync", AI_RUNTIME_SYNC_SECONDS, datetime.now(TIMEZONE).isoformat()):
        spawn(sync_ai_runtime())
