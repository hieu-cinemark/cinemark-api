"""Ngày hẹn tạo report Social Topic (22/10/2026): người xem trang Social Topic của cinemark chọn "ngày muốn nhận báo cáo
tiếp theo" + giờ (Worker ghi D1 bảng report_schedules - cinemark api/lib/reportSchedule.ts). Scheduler (app/services/scheduler.py)
mỗi REPORT_SCHEDULE_CHECK_SECONDS kiểm tra: tới ngày hẹn và đã qua giờ hẹn (giờ VN, mặc định DEFAULT_REPORT_TIME) thì tạo report cho phim
đó (cùng generate_report_for_movie với nút "Tạo report" / cron), ghi last_run_for / last_run_at / last_result và xoá
ngày hẹn. Lượt report hằng ngày (scripts/generate_social_topic_reports.py) bỏ qua phim có ngày hẹn ở tương lai.
"""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from app.core.logging import get_logger
from app.services.d1 import d1_query

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
DEFAULT_REPORT_TIME = "06:00"

_TABLE_SQL = """CREATE TABLE IF NOT EXISTS report_schedules (
  movie_id TEXT PRIMARY KEY,
  next_report_date TEXT,
  requested_by TEXT,
  updated_at TEXT,
  last_run_for TEXT,
  last_run_at TEXT,
  last_result TEXT
)"""


async def _ensure_table() -> None:
    await d1_query(_TABLE_SQL, quiet=True)
    # Cột giờ thêm 22/10/2026 (bảng tạo trước đó chưa có) - đã có thì D1 báo trùng cột, bỏ qua.
    await d1_query("ALTER TABLE report_schedules ADD COLUMN next_report_time TEXT", quiet=True)


async def future_scheduled_movie_ids() -> set[str]:
    """Phim đang có ngày hẹn sau hôm nay - lượt report hằng ngày không đè report của chúng."""
    await _ensure_table()
    today = datetime.now(TIMEZONE).date().isoformat()
    rows = await d1_query("SELECT movie_id FROM report_schedules WHERE next_report_date > ?", [today], quiet=True)
    return {row["movie_id"] for row in rows or []}


async def run_due_reports() -> int:
    """Tạo report cho mọi phim tới ngày hẹn. Nhận việc bằng UPDATE ... RETURNING (xoá ngày hẹn, ghi last_run_for) trước khi
    chạy nên hai tiến trình / hai lượt không tạo trùng. Số phim đã nhận."""
    now = datetime.now(TIMEZONE)
    await _ensure_table()
    today, clock = now.date().isoformat(), now.strftime("%H:%M")
    claimed = await d1_query(
        "UPDATE report_schedules SET last_run_for = next_report_date, next_report_date = NULL, last_run_at = ?, "
        "last_result = 'running' WHERE next_report_date IS NOT NULL AND (next_report_date < ? OR "
        "(next_report_date = ? AND COALESCE(next_report_time, ?) <= ?)) RETURNING movie_id, last_run_for",
        [datetime.now(tz=UTC).isoformat(), today, today, DEFAULT_REPORT_TIME, clock],
    )
    from app.services.social_topic import generate_report_for_movie, get_movie_for_report

    for row in claimed or []:
        movie_id = row["movie_id"]
        result = "movie_not_found"
        try:
            movie = await get_movie_for_report(movie_id)
            if movie is not None:
                result = await generate_report_for_movie(movie)
        except Exception as exc:  # noqa: BLE001 - phim lỗi không chặn phim khác; ghi lỗi để trang hiện ra
            logger.error("scheduled_report_failed", movie_id=movie_id, error=str(exc)[:300])
            result = f"error: {str(exc)[:150]}"
        await d1_query(
            "UPDATE report_schedules SET last_result = ?, last_run_at = ? WHERE movie_id = ?",
            [result, datetime.now(tz=UTC).isoformat(), movie_id],
        )
        logger.info("scheduled_report_done", movie_id=movie_id, scheduled_for=row.get("last_run_for"), result=result)
    return len(claimed or [])
