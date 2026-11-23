"""Cài đặt và lịch sử lượt dọn bài không liên quan (cleanup_settings, cleanup_run_history).

cleanup_settings là một dòng JSON các tham số sửa được trên dashboard; cleanup_run_history là log chỉ-thêm của mọi lượt chạy
(theo lịch hoặc bấm tay) để dashboard hiện "lần trước đã xảy ra gì". Dữ liệu ở D1, cài đặt chi tiết ở d1_ops.py."""

from __future__ import annotations

from typing import Any

from app.services.accounts.config_db import d1_ops


async def get_cleanup_settings() -> dict[str, Any]:
    """{"settings": {...chỉ các key đã lưu...}, "updated_at": ...} - việc trộn lên giá trị mặc định là việc của schema."""
    return await d1_ops.get_cleanup_settings()


async def upsert_cleanup_settings(values: dict[str, Any]) -> dict[str, Any]:
    """Chỉ ghi các key có trong `values` (cập nhật một phần), key thiếu giữ giá trị đã lưu."""
    return await d1_ops.upsert_cleanup_settings(values)


async def record_cleanup_run_start(*, dry_run: bool, triggered_by: str) -> int:
    return await d1_ops.record_cleanup_run_start(dry_run=dry_run, triggered_by=triggered_by)


async def record_cleanup_run_finish(run_id: int, *, summary: dict[str, Any], error: str | None = None) -> None:
    await d1_ops.record_cleanup_run_finish(run_id, summary=summary, error=error)


async def list_cleanup_run_history(limit: int = 20) -> list[dict[str, Any]]:
    return await d1_ops.list_cleanup_run_history(limit)
