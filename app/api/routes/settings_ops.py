"""Route vận hành trong Settings: dọn bài không liên quan, khám phá tự động, Omar API."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import ValidationError as PydanticValidationError

from app.core.logging import get_logger
from app.schemas.settings import (
    CleanupRunHistoryEntry,
    CleanupSettings,
    CleanupSettingsOut,
    CleanupSettingsUpdate,
    DiscoverySettings,
    DiscoverySettingsUpdate,
    OmarApiSettingsOut,
    OmarApiSettingsUpdate,
)
from app.services.accounts.config_db import cleanup as cleanup_db
from app.services.accounts.config_db import omar as omar_db
from app.services.crawl.scheduler import purge_in_progress, run_purge_now

logger = get_logger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])


def _cleanup_settings_out(row: dict, *, last_run: dict | None) -> CleanupSettingsOut:
    stored = value if isinstance(value := row.get("settings"), dict) else {}
    defaults = CleanupSettings()
    merged: dict = defaults.model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            merged[key] = getattr(CleanupSettings.model_validate({key: value}), key)
        except PydanticValidationError:
            logger.warning("cleanup_setting_invalid_stored_value", key=key)
    return CleanupSettingsOut(
        values=CleanupSettings(**merged),
        defaults=defaults,
        last_run_at=(last_run or {}).get("started_at") if last_run else None,
        last_run_summary=last_run,
        running=purge_in_progress(),
        updated_at=row.get("updated_at"),
    )


@router.get("/cleanup", response_model=CleanupSettingsOut)
async def get_cleanup_route() -> CleanupSettingsOut:
    """Các tham số đang có hiệu lực (giá trị lưu trên dashboard đè lên mặc định từ env),
    dòng của lần chạy gần nhất trong cleanup_run_history, và có lượt chạy nào đang diễn
    ra không (dashboard dùng thông tin này để khoá nút Run-now và hiện "Running…")."""
    row = await cleanup_db.get_cleanup_settings()
    history = await cleanup_db.list_cleanup_run_history(limit=1)
    return _cleanup_settings_out(row, last_run=history[0] if history else None)


@router.put("/cleanup", response_model=CleanupSettingsOut)
async def set_cleanup_route(payload: CleanupSettingsUpdate) -> CleanupSettingsOut:
    """Cập nhật một phần - key không có trong payload giữ nguyên giá trị đã lưu. Giống PUT
    của proxy-settings: không cần gửi lại mọi trường chỉ để bật/tắt."""
    allowed = {"run_time", "enabled", "grace_hours"}
    set_fields = set(payload.model_fields_set) & allowed
    values_to_write = {key: getattr(payload, key) for key in set_fields}
    row = await cleanup_db.upsert_cleanup_settings(values_to_write)
    logger.info("cleanup_settings_updated", **values_to_write)
    history = await cleanup_db.list_cleanup_run_history(limit=1)
    return _cleanup_settings_out(row, last_run=history[0] if history else None)


@router.post("/cleanup/run", response_model=dict)
async def run_cleanup_route() -> dict:
    """Kích hoạt tay từ dashboard. Trả về ngay {"started": True/False}; việc purge thật
    chạy trong task nền mà scheduler cũng dùng, nên người vận hành có thể chuyển trang
    mà không huỷ nó. `started: false` nghĩa là lượt trước vẫn đang chạy - dashboard nên
    hiển thị điều đó, không thử lại."""
    started = await run_purge_now()
    return {"started": started}


@router.get("/cleanup/history", response_model=list[CleanupRunHistoryEntry])
async def cleanup_history_route(limit: int = 20) -> list[CleanupRunHistoryEntry]:
    """Danh sách các dòng cleanup_run_history, mới nhất trước, giới hạn bởi `limit` (mặc
    định 20). Số liệu của mỗi mục là số id thật từ DELETE RETURNING trong cleanup.py -
    cùng con số được log trong `irrelevant_purge_done telegram=True`."""
    rows = await cleanup_db.list_cleanup_run_history(limit=limit)
    return [CleanupRunHistoryEntry(**row) for row in rows]


@router.get("/discovery", response_model=DiscoverySettings)
async def get_discovery_settings() -> DiscoverySettings:
    """AI có đang tự thêm hashtag / câu tìm kiếm / trang nguồn mới không (scheduler/discovery.discovery_enabled)."""
    return DiscoverySettings(**await omar_db.get_discovery_settings())


@router.put("/discovery", response_model=DiscoverySettings)
async def set_discovery_settings(payload: DiscoverySettingsUpdate) -> DiscoverySettings:
    return DiscoverySettings(**await omar_db.set_discovery_enabled(payload.enabled))


@router.get("/omar-api", response_model=OmarApiSettingsOut)
async def get_omar_api_settings() -> OmarApiSettingsOut:
    """Cấu hình Omar API: api_key đã lưu (không bao giờ trả về giá trị thô), base URL đang có hiệu lực, tổng request mỗi
    ngày và số request đồng thời. API key được đọc từ bảng omar_api_settings trên D1."""
    from app.core.config import settings as app_settings

    row = await omar_db.get_omar_api_settings()
    return OmarApiSettingsOut(
        api_key_set=bool(row and row.get("api_key")),
        base_url=app_settings.omar_api_base_url,
        requests_per_minute=int((row or {}).get("requests_per_minute") or 0),
        daily_requests=int((row or {}).get("daily_requests") or 0),
        concurrency=int((row or {}).get("concurrency") or 0),
        updated_at=(row or {}).get("updated_at"),
    )


@router.put("/omar-api", response_model=OmarApiSettingsOut)
async def set_omar_api_settings(payload: OmarApiSettingsUpdate) -> OmarApiSettingsOut:
    """Lưu api_key (rỗng = xoá), base_url, tổng request mỗi ngày, số request đồng thời, trần request/phút.

    Vừa lưu key mới: gọi GET /usage kiểm tra key. Key đúng thì bỏ trạng thái tạm dừng và mở lượt cào ngay - lượt chạy liên
    tục, tự đi tiếp tới hết hạn mức ngày (key sai thì chỉ báo key_valid=false, không mở lượt)."""
    from app.clients.omar import fetch_usage
    from app.core.config import settings as app_settings
    from app.services.crawl.scheduler import trigger_omar_now
    from app.services.omar import omar_pause

    if payload.api_key is not None:
        await omar_db.upsert_omar_api_key(payload.api_key.strip())
    await omar_db.set_omar_limits(
        requests_per_minute=payload.requests_per_minute,
        daily_requests=payload.daily_requests,
        concurrency=payload.concurrency,
    )
    # Ghi base_url override nếu có (cập nhật tại runtime, mất khi restart)
    if payload.base_url is not None and payload.base_url.strip():
        app_settings.omar_api_base_url = payload.base_url.strip()

    result = await get_omar_api_settings()
    key = ((await omar_db.get_omar_api_settings()) or {}).get("api_key") or ""
    if payload.api_key is not None and key:
        usage = await fetch_usage(key)
        # /usage trả status "active" khi key dùng được; không đọc được (key sai / Omar lỗi) thì usage là None.
        result.key_valid = bool(usage) and str(usage.get("status") or "active").lower() in ("active", "ok")
        result.usage = usage
        if result.key_valid:
            await omar_pause.clear_pause()
            trigger_omar_now()
    return result
