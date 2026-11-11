"""Ngày hẹn tạo report Social Topic: người xem trang Social Topic của cinemark chọn "ngày muốn nhận báo cáo
tiếp theo" + giờ (Worker ghi D1 bảng report_schedules - cinemark api/lib/reportSchedule.ts). Scheduler (app/services/crawl/scheduler.py)
mỗi REPORT_SCHEDULE_CHECK_SECONDS kiểm tra: tới ngày hẹn và đã qua giờ hẹn (giờ VN, mặc định DEFAULT_REPORT_TIME) thì tạo report cho phim
đó (cùng generate_report_for_movie với nút "Tạo report" / cron), ghi last_run_for / last_run_at / last_result và xoá
ngày hẹn. Lượt report hằng ngày (scripts/jobs/generate_social_topic_reports.py) bỏ qua phim có ngày hẹn ở tương lai.
"""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import Column, Table, Text, func, or_, select, update
from sqlalchemy.schema import CreateTable

from app.clients.d1 import d1_query
from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, metadata

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
DEFAULT_REPORT_TIME = "06:00"

schedules = Table(
    "report_schedules",
    metadata,
    Column("movie_id", Text, primary_key=True),
    *(
        Column(name, Text)
        for name in ("next_report_date", "requested_by", "updated_at", "last_run_for", "last_run_at", "last_result")
    ),
    Column("next_report_time", Text),  # thêm 22/10/2026, bảng cũ được ALTER ở _ensure_table
)


async def _ensure_table() -> None:
    await d1_exec(CreateTable(schedules, if_not_exists=True), quiet=True)
    # Cột giờ thêm 22/10/2026 (bảng tạo trước đó chưa có) - đã có thì D1 báo trùng cột, bỏ qua.
    await d1_query("ALTER TABLE report_schedules ADD COLUMN next_report_time TEXT", quiet=True)


async def future_scheduled_movie_ids() -> set[str]:
    """Phim đang có ngày hẹn sau hôm nay - lượt report hằng ngày không đè report của chúng."""
    await _ensure_table()
    today = datetime.now(TIMEZONE).date().isoformat()
    rows = await d1_exec(select(schedules.c.movie_id).where(schedules.c.next_report_date > today), quiet=True)
    return {row["movie_id"] for row in rows or []}


async def run_due_reports() -> int:
    """Tạo report cho mọi phim tới ngày hẹn. Nhận việc bằng UPDATE ... RETURNING (xoá ngày hẹn, ghi last_run_for) trước khi
    chạy nên hai tiến trình / hai lượt không tạo trùng. Số phim đã nhận."""
    now = datetime.now(TIMEZONE)
    await _ensure_table()
    today, clock = now.date().isoformat(), now.strftime("%H:%M")
    s = schedules.c
    due = or_(
        s.next_report_date < today,
        (s.next_report_date == today) & (func.coalesce(s.next_report_time, DEFAULT_REPORT_TIME) <= clock),
    )
    claimed = await d1_exec(
        update(schedules)
        .where(s.next_report_date.is_not(None), due)
        .values(
            last_run_for=s.next_report_date,
            next_report_date=None,
            last_run_at=datetime.now(tz=UTC).isoformat(),
            last_result="running",
        )
        .returning(s.movie_id, s.last_run_for)
    )
    from app.services.reports.social_topic import generate_report_for_movie, get_movie_for_report

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
        await d1_exec(
            update(schedules)
            .where(schedules.c.movie_id == movie_id)
            .values(last_result=result, last_run_at=datetime.now(tz=UTC).isoformat())
        )
        logger.info("scheduled_report_done", movie_id=movie_id, scheduled_for=row.get("last_run_for"), result=result)
    return len(claimed or [])
