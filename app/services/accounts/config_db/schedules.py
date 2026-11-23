"""Từ khoá lọc, lịch cào bài và lịch cào bình luận (giờ Việt Nam, sửa trên dashboard).

Dữ liệu ở D1, cài đặt chi tiết ở d1_ops.py.

- filter_keywords: bộ lọc nội dung chung (không theo nền tảng), spider-hub đọc để quyết định một bài/comment có đáng giữ không.
- crawl_schedules: giờ cào hằng ngày theo nền tảng, scheduler (app/services/crawl/scheduler/) đọc.
- comment_crawl_schedules: bật/tắt cào comment theo nền tảng. Chạy mỗi giờ (xem _comments_tick và comment_planner.py):
  top_n = số bài nóng mỗi phim được theo dõi, run_time = giờ chạy lượt mẫu phân tầng (và 12 tiếng sau)."""

from __future__ import annotations

from typing import Any

from app.services.accounts.config_db import d1_ops


async def list_filter_keywords(category: str | None = None) -> list[dict[str, Any]]:
    return await d1_ops.list_filter_keywords(category)


async def create_filter_keyword(fields: dict[str, Any]) -> dict[str, Any]:
    return await d1_ops.create_filter_keyword(fields)


async def update_filter_keyword(keyword_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    return await d1_ops.update_filter_keyword(keyword_id, fields)


async def delete_filter_keyword(keyword_id: int) -> None:
    await d1_ops.delete_filter_keyword(keyword_id)


async def list_crawl_schedules() -> list[dict[str, Any]]:
    return await d1_ops.list_crawl_schedules()


async def ensure_default_crawl_schedules(platforms: set[str]) -> None:
    """Gọi một lần lúc khởi động (app/main.py) cho mọi nền tảng đã đăng ký: một nền tảng không có dòng ở đây sẽ không bao giờ
    xuất hiện trong vòng lặp của scheduler và không bao giờ chạy, mà không có gì báo ra lỗ hổng đó. Chỉ tạo dòng còn thiếu
    (mặc định 07:00, bật); nền tảng người vận hành đã cấu hình hay cố ý tắt không bị đụng tới."""
    await d1_ops.ensure_default_crawl_schedules(platforms)


async def upsert_crawl_schedule(
    platform: str, *, run_time: str, enabled: bool, nurture_before: bool = False, nurture_after: bool = False
) -> dict[str, Any]:
    """Lần ghi phía dashboard: dashboard không cần biết dòng của nền tảng đã có hay chưa."""
    return await d1_ops.upsert_crawl_schedule(
        platform, run_time=run_time, enabled=enabled, nurture_before=nurture_before, nurture_after=nurture_after
    )


async def mark_crawl_schedule_triggered(platform: str, triggered_date: str) -> None:
    """Ghi chống chạy lặp của chính scheduler (cột last_triggered_date); không dành cho người dùng."""
    await d1_ops.mark_crawl_schedule_triggered(platform, triggered_date)


async def list_comment_crawl_schedules() -> list[dict[str, Any]]:
    return await d1_ops.list_comment_crawl_schedules()


async def ensure_default_comment_crawl_schedules(platforms: set[str]) -> None:
    """Cùng lý do như ensure_default_crawl_schedules: chỉ tạo dòng còn thiếu, không đụng cấu hình đã có."""
    await d1_ops.ensure_default_comment_crawl_schedules(platforms)


async def upsert_comment_crawl_schedule(platform: str, *, run_time: str, enabled: bool, top_n: int) -> dict[str, Any]:
    return await d1_ops.upsert_comment_crawl_schedule(platform, run_time=run_time, enabled=enabled, top_n=top_n)
