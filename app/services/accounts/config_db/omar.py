"""Cài đặt khám phá tự động và Omar API (key, trần request/phút)."""

from __future__ import annotations

from typing import Any

from app.core.logging import get_logger

logger = get_logger(__name__)
from app.services.accounts.config_db import d1_ops, d1_store
from app.services.accounts.config_db.conn import _connect, _get_pool, _trusted_sql  # noqa: F401

# --- Cấu hình Omar API -----------------------------------------------------
# Cấu hình API key cho Omar (dev.omar-thing.site) - dùng cho crawl video/hashtag thay vì
# spider-hub khi người vận hành bật toggle trên dashboard. API key được đọc từ
# bảng này (không bao giờ lưu trong env). Base URL có thể ghi đè bằng
# OMAR_API_BASE_URL env var (app/core/config.py).


_discovery_settings_ready = False


async def _ensure_discovery_settings_table() -> None:
    global _discovery_settings_ready
    if _discovery_settings_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS discovery_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                enabled boolean NOT NULL DEFAULT true,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.commit()
    _discovery_settings_ready = True


async def get_discovery_settings() -> dict[str, Any]:
    """{"enabled": bool, "updated_at": ...} - chưa có dòng nào thì bật (hành vi trước khi có công tắc)."""
    if d1_store.enabled():
        return await d1_ops.get_discovery_settings()
    await _ensure_discovery_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT enabled, updated_at FROM discovery_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {"enabled": True, "updated_at": None}


async def set_discovery_enabled(enabled: bool) -> dict[str, Any]:
    if d1_store.enabled():
        return await d1_ops.set_discovery_enabled(enabled)
    await _ensure_discovery_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO discovery_settings (id, enabled) VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now()
            RETURNING enabled, updated_at
            """,
            (enabled,),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def get_omar_api_settings() -> dict[str, Any] | None:
    """Trả về {"api_key", "requests_per_minute", "daily_requests", "concurrency", "updated_at"} hoặc None nếu chưa có."""
    if d1_store.enabled():
        return await d1_ops.get_omar_api_settings()
    await _ensure_omar_api_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT api_key, requests_per_minute, daily_requests, concurrency, updated_at FROM omar_api_settings WHERE id = 1"
        )
        return await cur.fetchone()


_omar_cap = 0


async def omar_requests_cap() -> int:
    """Trần request/phút người dùng đặt cho Omar (0 = theo gói). Đọc lỗi thì giữ giá trị lần trước."""
    global _omar_cap
    try:
        row = await get_omar_api_settings()
        _omar_cap = int((row or {}).get("requests_per_minute") or 0)
    except Exception as exc:  # noqa: BLE001 - lỗi đọc cấu hình không được chặn lượt cào
        logger.warning("omar_requests_cap_read_failed", error=str(exc)[:200])
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
        logger.warning("omar_limits_read_failed", error=str(exc)[:200])
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
    if not changes:
        return
    if d1_store.enabled():
        await d1_ops.set_omar_limits(changes)
        return
    await _ensure_omar_api_settings_table()
    columns = ", ".join(changes)
    placeholders = ", ".join(["%s"] * len(changes))
    updates = ", ".join(f"{name} = EXCLUDED.{name}" for name in changes)
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"INSERT INTO omar_api_settings (id, {columns}) VALUES (1, {placeholders}) "
            f"ON CONFLICT (id) DO UPDATE SET {updates}, updated_at = now()",
            tuple(changes.values()),
        )
        await conn.commit()


_omar_api_settings_ready = False


async def _ensure_omar_api_settings_table() -> None:
    global _omar_api_settings_ready
    if _omar_api_settings_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS omar_api_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                api_key text NOT NULL DEFAULT '',
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # Trần request/phút đặt ở tab Omar (0 = theo gói), thêm 11/11/2026.
        await cur.execute(
            "ALTER TABLE omar_api_settings ADD COLUMN IF NOT EXISTS requests_per_minute integer NOT NULL DEFAULT 0"
        )
        # Tổng request mỗi ngày và số request đồng thời đặt ở tab Omar (0 = theo gói / mặc định).
        await cur.execute(
            "ALTER TABLE omar_api_settings ADD COLUMN IF NOT EXISTS daily_requests integer NOT NULL DEFAULT 0"
        )
        await cur.execute(
            "ALTER TABLE omar_api_settings ADD COLUMN IF NOT EXISTS concurrency integer NOT NULL DEFAULT 0"
        )
        await conn.commit()
    _omar_api_settings_ready = True


async def upsert_omar_api_key(api_key: str | None) -> dict[str, Any]:
    """Upsert API key - None/rỗng thì xoá (để toggle không dùng được)."""
    if d1_store.enabled():
        return await d1_ops.upsert_omar_api_key(api_key or "")
    await _ensure_omar_api_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO omar_api_settings (id, api_key) VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET api_key = EXCLUDED.api_key, updated_at = now()
            RETURNING api_key, updated_at
            """,
            (api_key or "",),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]
