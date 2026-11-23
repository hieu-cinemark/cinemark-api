"""Cài đặt khám phá tự động và Omar API (key, trần request/phút, request/ngày, số request đồng thời).

API key của Omar nằm ở DB (không phải env); base URL có thể ghi đè bằng OMAR_API_BASE_URL (app/core/config.py).
Dữ liệu ở D1, cài đặt chi tiết ở d1_ops.py."""

from __future__ import annotations

from typing import Any

from app.core.logging import get_logger
from app.services.accounts.config_db import d1_ops

logger = get_logger(__name__)


async def get_discovery_settings() -> dict[str, Any]:
    """{"enabled": bool, "updated_at": ...} - chưa có dòng nào thì bật (hành vi trước khi có công tắc)."""
    return await d1_ops.get_discovery_settings()


async def set_discovery_enabled(enabled: bool) -> dict[str, Any]:
    return await d1_ops.set_discovery_enabled(enabled)


async def get_omar_api_settings() -> dict[str, Any] | None:
    """Trả về {"api_key", "requests_per_minute", "daily_requests", "concurrency", "updated_at"} hoặc None nếu chưa có."""
    return await d1_ops.get_omar_api_settings()


_omar_cap = 0


async def omar_requests_cap() -> int:
    """Trần request/phút người dùng đặt cho Omar (0 = theo gói). Đọc lỗi thì giữ giá trị lần trước."""
    global _omar_cap
    try:
        row = await get_omar_api_settings()
        _omar_cap = int((row or {}).get("requests_per_minute") or 0)
    except Exception as exc:  # noqa: BLE001 - lỗi đọc cấu hình không được chặn lượt cào
        logger.warning("omar_requests_cap_read_failed", error=exc)
    return _omar_cap


_omar_limits: dict[str, int] = {"daily_requests": 0, "concurrency": 0}


async def omar_limits() -> dict[str, int]:
    """Tổng request mỗi ngày và số request đồng thời người dùng đặt ở tab Omar (0 = theo gói / mặc định). Đọc lỗi thì giữ giá
    trị lần trước."""
    try:
        row = await get_omar_api_settings()
        _omar_limits["daily_requests"] = int((row or {}).get("daily_requests") or 0)
        _omar_limits["concurrency"] = int((row or {}).get("concurrency") or 0)
    except Exception as exc:  # noqa: BLE001 - lỗi đọc cấu hình không được chặn lượt cào
        logger.warning("omar_limits_read_failed", error=exc)
    return dict(_omar_limits)


async def set_omar_limits(
    *, requests_per_minute: int | None = None, daily_requests: int | None = None, concurrency: int | None = None
) -> None:
    """Lưu các giới hạn người dùng đặt (None = giữ nguyên, 0 = theo gói / mặc định)."""
    values = {
        "requests_per_minute": requests_per_minute,
        "daily_requests": daily_requests,
        "concurrency": concurrency,
    }
    changes = {name: max(0, int(value)) for name, value in values.items() if value is not None}
    if changes:
        await d1_ops.set_omar_limits(changes)


async def upsert_omar_api_key(api_key: str | None) -> dict[str, Any]:
    """Upsert API key - None/rỗng thì xoá (để toggle không dùng được)."""
    return await d1_ops.upsert_omar_api_key(api_key or "")
