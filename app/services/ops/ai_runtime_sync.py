"""Chép cấu hình AI (Supabase ai_providers + ai_settings.active_provider) sang bảng ai_runtime của D1 cho Worker cinemark,
vốn không kết nối được Supabase (2026-10-22). Worker đọc dòng active = 1 làm nhà cung cấp "bee" của nó (Box Office AI đọc
ảnh, hỏi AI, đọc sheet...) - xem cinemark api/lib/ai/providerConfig.ts.

Chạy sau mỗi lần lưu ở tab AI của dashboard spider-hub và định kỳ trong scheduler (đổi thẳng trên Supabase vẫn tới Worker).
API key nằm trong D1 (chỉ Worker và token Cloudflare của cinemark-api đọc được).
"""

from __future__ import annotations

from datetime import UTC, datetime

from app.ai.client import active_provider_key
from app.core.logging import get_logger
from app.repositories.d1.store import d1_query
from app.services.accounts import platform_config_db as db

logger = get_logger(__name__)

# Dòng "bee" cũ (trước 2026-10-22) không còn là một nhà cung cấp thật - không chép.
_SKIP = {"bee"}

_TABLE_SQL = """CREATE TABLE IF NOT EXISTS ai_runtime (
  key TEXT PRIMARY KEY,
  base_url TEXT NOT NULL,
  chat_path TEXT NOT NULL,
  api_key TEXT NOT NULL,
  model TEXT NOT NULL,
  image_model TEXT NOT NULL,
  decisions_path TEXT NOT NULL,
  comment_model TEXT NOT NULL,
  post_model TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
)"""


async def sync_ai_runtime() -> int:
    """Số nhà cung cấp đã chép; -1 khi lỗi (đã log)."""
    try:
        active = await active_provider_key()
        rows = [row for row in await db.list_ai_providers() if row["key"] not in _SKIP and row.get("base_url")]
        if await d1_query(_TABLE_SQL, quiet=True) is None:
            raise RuntimeError("ai_runtime_create_failed")
        now = datetime.now(tz=UTC).isoformat(timespec="seconds")
        for row in rows:
            await d1_query(
                "INSERT INTO ai_runtime (key, base_url, chat_path, api_key, model, image_model, decisions_path, "
                "comment_model, post_model, active, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (key) DO UPDATE SET base_url = excluded.base_url, chat_path = excluded.chat_path, "
                "api_key = excluded.api_key, model = excluded.model, image_model = excluded.image_model, "
                "decisions_path = excluded.decisions_path, comment_model = excluded.comment_model, "
                "post_model = excluded.post_model, active = excluded.active, updated_at = excluded.updated_at",
                [
                    row["key"],
                    row["base_url"],
                    row.get("chat_path") or "/chat/completions",
                    row.get("api_key") or "",
                    row.get("model") or "",
                    row.get("image_model") or "",
                    row.get("decisions_path") or "",
                    row.get("comment_model") or "",
                    row.get("post_model") or "",
                    int(row["key"] == active),
                    now,
                ],
            )
        keys = [row["key"] for row in rows]
        if keys:
            await d1_query(f"DELETE FROM ai_runtime WHERE key NOT IN ({','.join('?' * len(keys))})", keys, quiet=True)
        logger.info("ai_runtime_synced", active=active, providers=keys)
        return len(rows)
    except Exception as exc:  # noqa: BLE001 - lần sau (lưu / scheduler) chép lại
        logger.error("ai_runtime_sync_failed", error=str(exc)[:300])
        return -1
