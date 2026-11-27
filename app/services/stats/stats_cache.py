"""Cache Redis cho số liệu tổng hợp đọc từ D1 (chuyển từ app/api/routes/stats.py 27/11/2026 để dịch vụ khác dùng chung).

Số liệu tổng hợp cho trang Tổng quan: D1 quá tải ("7429 overloaded") làm truy vấn mất 15-20 giây kèm thử
lại, dashboard hết chờ và hiện 0. Giờ: kết quả còn mới (< _FRESH_SECONDS) trả thẳng từ Redis - nhiều tab/người cùng
mở không nhân số truy vấn D1 lên; truy vấn D1 quá _QUERY_TIMEOUT_SECONDS hoặc lỗi/rỗng thì trả số gần nhất đã có
(giữ tới _STALE_SECONDS) thay vì 0.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger

logger = get_logger(__name__)

_FRESH_SECONDS = 60
_STALE_SECONDS = 2 * 86400
_QUERY_TIMEOUT_SECONDS = 8.0
_CACHE_KEY = f"{REDIS_KEY_PREFIX}stats_cache:{{name}}"


async def cached_rows(name: str, compute: Callable[[], Awaitable[list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    """Kết quả còn mới thì trả từ Redis; không thì đọc D1 (giới hạn thời gian) và lưu; D1 lỗi/rỗng/quá giờ thì trả số gần nhất."""
    redis = get_redis_client()
    key = _CACHE_KEY.format(name=name)
    cached: dict[str, Any] | None = None
    try:
        raw = await redis.get(key)
        cached = json.loads(raw) if raw else None
    except Exception as exc:  # noqa: BLE001 - Redis trục trặc: vẫn đọc D1 như cũ
        logger.warning("stats_cache_read_failed", name=name, error=exc)
    if cached and time.time() - float(cached.get("at") or 0) < _FRESH_SECONDS:
        return cached["rows"]
    try:
        rows = await asyncio.wait_for(compute(), timeout=_QUERY_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - timeout / D1 lỗi: rơi về số gần nhất
        logger.warning("stats_query_failed", name=name, error=exc)
        rows = None
    if rows:
        try:
            await redis.set(key, json.dumps({"at": time.time(), "rows": rows}, default=str), ex=_STALE_SECONDS)
        except Exception as exc:  # noqa: BLE001 - không lưu được cache thì vẫn trả kết quả vừa đọc
            logger.warning("stats_cache_write_failed", name=name, error=exc)
        return rows
    if cached:
        logger.info("stats_served_stale", name=name, age_seconds=round(time.time() - float(cached.get("at") or 0)))
        return cached["rows"]
    return rows or []
