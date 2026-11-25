"""Key API của nguồn ngoài (TMDB, Trakt...) - chỉ lưu ở cinemark-config-db, bảng external_api_keys, không đọc
.env. Đặt / đổi key: python -m scripts.tools.set_external_api_key <tên> <key>. Đọc có cache 60 giây."""

from __future__ import annotations

import time

from app.core.logging import get_logger

logger = get_logger(__name__)

_CACHE_SECONDS = 60
_cache: dict[str, tuple[float, str]] = {}


async def get_key(name: str) -> str:
    """Key của nguồn `name`; chưa đặt (hoặc DB lỗi) thì chuỗi rỗng."""
    now = time.monotonic()
    hit = _cache.get(name)
    if hit and now - hit[0] < _CACHE_SECONDS:
        return hit[1]
    from app.services.accounts.config_db.d1_ops import get_external_api_key

    try:
        key = (await get_external_api_key(name) or "").strip()
    except Exception as exc:  # noqa: BLE001 - DB lỗi tạm thời: KHÔNG cache, lần gọi sau đọc lại
        logger.warning("external_api_key_read_failed", source=name, error=exc)
        # Còn key đọc được trước đó thì dùng tiếp.
        return hit[1] if hit else ""
    _cache[name] = (now, key)
    return key


def forget(name: str | None = None) -> None:
    if name is None:
        _cache.clear()
    else:
        _cache.pop(name, None)
