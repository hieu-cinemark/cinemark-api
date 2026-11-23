"""Chép cấu hình AI (D1 ai_providers + ai_settings.active_provider) sang bảng ai_runtime của D1 cho Worker cinemark,
vốn không đọc được D1 cinemark-config-db. Worker đọc dòng active = 1 làm nhà cung cấp duy nhất của nó (Box Office AI, đọc
ảnh, hỏi AI, đọc sheet...) - xem cinemark api/lib/ai/providerConfig.ts.

Chạy sau mỗi lần lưu ở tab AI của dashboard spider-hub và định kỳ trong scheduler (đổi thẳng trên D1 vẫn tới Worker).
API key nằm trong D1 (chỉ Worker và token Cloudflare của cinemark-api đọc được).
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Column, Integer, Table, Text, delete, text
from sqlalchemy.schema import CreateTable

from app.ai.providers.client import active_provider_key
from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, d1_insert_many, metadata
from app.services.accounts import platform_config_db as db

logger = get_logger(__name__)

# Bảng D1 giữ các cột cũ (NOT NULL) từ trước khi đơn giản hoá: chỉ base_url, api_key, model là cấu hình thật, các cột còn lại ghi
# giá trị cố định (Worker không còn đọc chúng).
_TEXT_COLUMNS = (
    "base_url", "chat_path", "api_key", "model", "image_model", "decisions_path", "comment_model", "post_model",
)  # fmt: skip
_LEGACY_FIXED = {
    "chat_path": "/chat/completions",
    "image_model": "",
    "decisions_path": "",
    "comment_model": "",
    "post_model": "",
}
ai_runtime = Table(
    "ai_runtime",
    metadata,
    Column("key", Text, primary_key=True),
    *(Column(name, Text, nullable=False) for name in _TEXT_COLUMNS),
    Column("active", Integer, nullable=False, server_default=text("0")),
    Column("updated_at", Text, nullable=False),
)


async def sync_ai_runtime() -> int:
    """Số nhà cung cấp đã chép; -1 khi lỗi (đã log)."""
    try:
        active = await active_provider_key()
        rows = [row for row in await db.list_ai_providers() if row.get("base_url")]
        if await d1_exec(CreateTable(ai_runtime, if_not_exists=True), quiet=True) is None:
            raise RuntimeError("ai_runtime_create_failed")
        now = datetime.now(tz=UTC).isoformat(timespec="seconds")
        await d1_insert_many(
            ai_runtime,
            [
                {
                    "key": row["key"],
                    **{
                        name: _LEGACY_FIXED[name] if name in _LEGACY_FIXED else (row.get(name) or "")
                        for name in _TEXT_COLUMNS
                    },
                    "active": int(row["key"] == active),
                    "updated_at": now,
                }
                for row in rows
            ],
            conflict=["key"],
        )
        keys = [row["key"] for row in rows]
        if keys:
            await d1_exec(delete(ai_runtime).where(ai_runtime.c.key.not_in(keys)), quiet=True)
        logger.info("ai_runtime_synced", active=active, providers=keys)
        return len(rows)
    except Exception as exc:  # noqa: BLE001 - lần sau (lưu / scheduler) chép lại
        logger.error("ai_runtime_sync_failed", error=exc)
        return -1
