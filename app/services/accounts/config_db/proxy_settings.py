"""Cấu hình cào (proxy_settings) và nguồn proxy (proxy_providers). Nguồn sự thật duy nhất: spider-hub chỉ đọc dòng proxy_settings mà ensure_proxy_settings_complete ghi đủ khoá."""

from __future__ import annotations

from typing import Any

from app.core.logging import get_logger
from app.services.accounts.config_db import d1_accounts, d1_store

logger = get_logger(__name__)

# proxy_settings: một dòng JSON các tham số tinh chỉnh (xem ProxySettings trong app/schemas/settings.py cho key/mặc định/giới hạn).
# proxy_providers: mỗi nguồn proxy một dòng (URL reset + key). Cả hai được spider-hub đọc (cache 60s bên đó).


async def get_proxy_settings() -> dict[str, Any]:
    """{"settings": {...chỉ các key đã lưu...}, "updated_at": ...} - việc trộn lên trên giá
    trị mặc định là việc của schema (ProxySettings)."""
    return await d1_store.get_proxy_settings() or {"settings": {}, "updated_at": None}


# Khoá đã đổi tên (tên cũ -> tên mới): giá trị lưu dưới tên cũ vẫn có hiệu lực, lần ghi kế tiếp ghi tên mới.
RENAMED_PROXY_SETTINGS = {"provider_min_get_new_interval_seconds": "provider_min_call_interval_seconds"}


def merge_proxy_settings(stored: dict[str, Any] | None) -> dict[str, Any]:
    """Giá trị hiệu lực của mọi khoá: mặc định của ProxySettings, đè bằng giá trị đã lưu còn hợp lệ (khoá lạ bỏ qua; giá trị
    lưu không qua được kiểm tra - vd giới hạn bị siết lại - thì quay về mặc định của riêng khoá đó)."""
    from app.schemas.settings import ProxySettings

    stored = dict(stored or {})
    for old, new in RENAMED_PROXY_SETTINGS.items():
        if old in stored and new not in stored:
            stored[new] = stored[old]
    merged = ProxySettings().model_dump()
    for key, value in stored.items():
        if key not in merged:
            continue
        try:
            merged[key] = getattr(ProxySettings.model_validate({key: value}), key)
        except Exception:  # noqa: BLE001 - pydantic ValidationError
            logger.warning("proxy_setting_invalid_stored_value", key=key)
    return merged


async def ensure_proxy_settings_complete() -> bool:
    """Ghi đủ mọi khoá (mặc định + giá trị đã lưu) vào dòng proxy_settings - spider-hub chỉ đọc dòng này, không có mặc định
    riêng. Gọi khi web/scheduler khởi động; chỉ ghi khi thiếu khoá, còn khoá cũ/lạ hoặc giá trị không hợp lệ. True khi đã ghi."""
    stored = (await get_proxy_settings()).get("settings") or {}
    merged = merge_proxy_settings(stored)
    if stored == merged:
        return False
    await upsert_proxy_settings(merged)
    logger.info(
        "proxy_settings_completed", added=sorted(set(merged) - set(stored)), dropped=sorted(set(stored) - set(merged))
    )
    return True


_crawl_settings_cache: tuple[float, dict[str, Any]] | None = None


async def crawl_setting(name: str) -> Any:
    """Một khoá của proxy_settings (giá trị hiệu lực, xem merge_proxy_settings) - cache 60 giây; DB lỗi thì dùng bản tốt gần
    nhất / mặc định. Dùng cho các ngưỡng hiệu năng cào sửa trên dashboard (Settings › Proxy)."""
    import time

    global _crawl_settings_cache
    now = time.monotonic()
    if _crawl_settings_cache is None or now - _crawl_settings_cache[0] > 60:
        try:
            values = merge_proxy_settings((await get_proxy_settings()).get("settings"))
        except Exception as exc:  # noqa: BLE001 - DB lỗi: giữ bản tốt gần nhất
            logger.warning("crawl_settings_read_failed", error=exc)
            values = _crawl_settings_cache[1] if _crawl_settings_cache is not None else merge_proxy_settings(None)
        _crawl_settings_cache = (now, values)
    return _crawl_settings_cache[1][name]


async def upsert_proxy_settings(values: dict[str, Any]) -> dict[str, Any]:
    return await d1_store.upsert_proxy_settings(values)


async def list_proxy_providers() -> list[dict[str, Any]]:
    return await d1_accounts.list_proxy_providers()


async def upsert_proxy_provider(key: str, *, api_url: str, token: str | None) -> dict[str, Any]:
    """token=None thì giữ nguyên key đã lưu (cùng hợp đồng với api_key của upsert_ai_provider). Mỗi nguồn là một link reset +
    một key - mọi nhà cung cấp dùng chung một cơ chế nên không có trường phân loại."""
    return await d1_accounts.upsert_proxy_provider(key, api_url=api_url, token=token)


async def delete_proxy_provider(key: str) -> None:
    """Xoá nguồn proxy. Nguồn đang được dùng (có proxy trỏ tới, hoặc là tiktok_synthetic_provider) thì không xoá - xoá nó sẽ
    để các proxy đó mồ côi (không ai xoay IP cho chúng nữa) mà không ai hỏi."""
    settings = (await get_proxy_settings()).get("settings") or {}
    await d1_accounts.delete_proxy_provider(key, settings.get("tiktok_synthetic_provider"))
