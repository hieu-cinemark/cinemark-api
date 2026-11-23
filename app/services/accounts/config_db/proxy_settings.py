"""Cấu hình cào (proxy_settings) và nguồn proxy (proxy_providers). Nguồn sự thật duy nhất: spider-hub chỉ đọc dòng proxy_settings mà ensure_proxy_settings_complete ghi đủ khoá."""

from __future__ import annotations

from typing import Any

from app.core.errors import ConflictError, NotFoundError
from app.core.logging import get_logger

logger = get_logger(__name__)
from app.services.accounts.config_db import d1_store
from app.services.accounts.config_db.accounts import _ensure_pool_columns
from app.services.accounts.config_db.conn import _connect, _get_pool, _trusted_sql  # noqa: F401

# --- Setting hành vi proxy -----------------------------------------------
# proxy_settings: jsonb singleton chứa các tham số tinh chỉnh (xem ProxySettings trong
# app/schemas/settings.py cho key/mặc định/giới hạn). proxy_providers: mỗi nguồn proxy
# của nhà cung cấp một dòng (URL reset + key). Cả hai
# đều được social_crawler/db/proxy_settings.py của spider-hub đọc (cache 60s bên đó,
# nên lưu ở đây thì các crawler đang chạy áp dụng trong vòng một phút).

_proxy_settings_ready = False


async def _ensure_proxy_settings_tables() -> None:
    global _proxy_settings_ready
    if _proxy_settings_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS proxy_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                settings jsonb NOT NULL DEFAULT '{}'::jsonb,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS proxy_providers (
                key text PRIMARY KEY,
                api_url text NOT NULL DEFAULT '',
                token text NOT NULL DEFAULT '',
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # spider-hub ghi hai cột này mỗi lần thực sự dùng gói (proxy_settings.record_provider_use).
        await cur.execute("ALTER TABLE proxy_providers ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
        await cur.execute("ALTER TABLE proxy_providers ADD COLUMN IF NOT EXISTS last_status text")
        # Cột `kind` cũ (phân loại gói) đã bị bỏ - mọi nguồn dùng chung 1 cơ chế
        # (link reset + key) nên không cần phân loại.
        # Cột của chế độ "cấp quyền theo IP nguồn" đã bị bỏ cùng API lease của nhà cung cấp cũ.
        # Cột vẫn còn trong DB cho tới khi spider-hub chạy migrate --drop-ip-allowlist.
        await conn.commit()
    _proxy_settings_ready = True


async def get_proxy_settings() -> dict[str, Any]:
    """{"settings": {...chỉ các key đã lưu...}, "updated_at": ...} - việc trộn lên trên giá
    trị mặc định là việc của schema (ProxySettings)."""
    if d1_store.enabled():
        return await d1_store.get_proxy_settings() or {"settings": {}, "updated_at": None}
    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT settings, updated_at FROM proxy_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {"settings": {}, "updated_at": None}


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
            logger.warning("crawl_settings_read_failed", error=str(exc)[:200])
            values = _crawl_settings_cache[1] if _crawl_settings_cache is not None else merge_proxy_settings(None)
        _crawl_settings_cache = (now, values)
    return _crawl_settings_cache[1][name]


async def upsert_proxy_settings(values: dict[str, Any]) -> dict[str, Any]:
    if d1_store.enabled():
        return await d1_store.upsert_proxy_settings(values)
    from psycopg.types.json import Json

    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO proxy_settings (id, settings) VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = now()
            RETURNING settings, updated_at
            """,
            (Json(values),),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


PROXY_PROVIDER_COLUMNS = "key, api_url, token, updated_at, last_used_at, last_status"


async def list_proxy_providers() -> list[dict[str, Any]]:
    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"SELECT {PROXY_PROVIDER_COLUMNS}, "
            "(SELECT count(*) FROM platform_proxies pp WHERE pp.provider_key = proxy_providers.key) AS proxy_count "
            "FROM proxy_providers ORDER BY key"
        )
        return await cur.fetchall()


async def upsert_proxy_provider(key: str, *, api_url: str, token: str | None) -> dict[str, Any]:
    """token=None thì giữ nguyên key đã lưu (cùng hợp đồng với api_key của
    upsert_ai_provider). Mỗi nguồn là một link reset + một key - mọi nhà cung cấp dùng
    chung một cơ chế nên không có trường phân loại."""
    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        if token is None:
            await cur.execute(
                f"""
                INSERT INTO proxy_providers (key, api_url) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    api_url = EXCLUDED.api_url, updated_at = now()
                RETURNING {PROXY_PROVIDER_COLUMNS}
                """,
                (key, api_url),
            )
        else:
            await cur.execute(
                f"""
                INSERT INTO proxy_providers (key, api_url, token) VALUES (%s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    api_url = EXCLUDED.api_url, token = EXCLUDED.token, updated_at = now()
                RETURNING {PROXY_PROVIDER_COLUMNS}
                """,
                (key, api_url, token),
            )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def delete_proxy_provider(key: str) -> None:
    """Xoá nguồn proxy. Nguồn đang được dùng (có proxy trong platform_proxies trỏ tới, hoặc
    là nguồn của tiktok_synthetic_provider) thì không xoá - xoá nó sẽ để các proxy đó mồ
    côi (không ai xoay IP cho chúng nữa) mà không ai hỏi."""
    await _ensure_proxy_settings_tables()
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT count(*) FROM platform_proxies WHERE provider_key = %s", (key,))
        used_by_proxies = int((await cur.fetchone())["count"])
        if used_by_proxies:
            raise ConflictError(f"Provider {key} is still used by {used_by_proxies} proxy row(s)")
    settings = (await get_proxy_settings()).get("settings") or {}
    if isinstance(settings, dict) and settings.get("tiktok_synthetic_provider") == key:
        raise ConflictError(f"Provider {key} is the tiktok_synthetic_provider - point it elsewhere first")
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM proxy_providers WHERE key = %s", (key,))
        deleted = cur.rowcount
        await conn.commit()
    if not deleted:
        raise NotFoundError(f"Proxy provider {key} not found")
