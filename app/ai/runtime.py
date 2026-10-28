"""Lớp chính sách theo task cho mọi lời gọi AI, nằm trên app.ai.client dùng chung: bật/tắt + system prompt ghi đè theo
task (bảng ai_settings trên Supabase). Mọi lời gọi đi qua Bee (ai_providers key="bee") - Kira đã bỏ hẳn từ 2026-10-21
(chậm, hay 429/504). Mọi bộ phân loại trong app/ai/ đều đi qua call_llm() để dùng chung một lần đọc enabled/prompts và
một ngân sách retry."""

from __future__ import annotations

import time
from typing import Any

from app.ai.client import call_ai, invalidate_provider_cache, parse_json_response
from app.ai.defaults import default_system_prompts
from app.core.logging import get_logger

__all__ = [
    "AI_PROVIDER",
    "ai_is_enabled",
    "call_llm",
    "invalidate_ai_runtime_cache",
    "load_ai_runtime",
    "parse_json_response",
]

AI_PROVIDER = "bee"

logger = get_logger(__name__)

_ai_cfg_cache: tuple[float, dict[str, Any]] | None = None
# Mỗi lần đọc phải mở kết nối Supabase mới (khoảng 3 giây), nên TTL ngắn làm gần như
# mọi lời gọi AI đều phải chờ. Tiến trình API tự xoá cache này khi lưu; các
# tiến trình khác (ingest consumer) nhận thay đổi trong vòng một phút.
_AI_CFG_TTL_SECONDS = 60.0


def _normalize_prompts(raw: object) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, str) and value.strip():
            out[key] = value
    return out


async def load_ai_runtime() -> dict[str, Any]:
    """enabled + prompt ghi đè theo task từ dòng ai_settings trên Supabase, mặc định là
    tắt/không ghi đè nếu bảng chưa có. Thông tin provider/model là chuyện riêng - xem
    app.ai.client.load_provider()."""
    global _ai_cfg_cache
    now = time.monotonic()
    if _ai_cfg_cache is not None and now - _ai_cfg_cache[0] < _AI_CFG_TTL_SECONDS:
        return _ai_cfg_cache[1]
    cfg: dict[str, Any] = {"enabled": False, "prompts": {}, "updated_at": None}
    try:
        from app.services.accounts.platform_config_db import get_ai_settings

        row = await get_ai_settings()
        cfg = {
            "enabled": bool(row.get("enabled")),
            "prompts": _normalize_prompts(row.get("prompts")),
            "updated_at": row.get("updated_at"),
        }
    except Exception as exc:
        logger.warning("ai_settings_load_failed", error=str(exc))
    _ai_cfg_cache = (now, cfg)
    return cfg


def invalidate_ai_runtime_cache() -> None:
    global _ai_cfg_cache
    _ai_cfg_cache = None
    invalidate_provider_cache(AI_PROVIDER)


async def ai_is_enabled() -> bool:
    return bool((await load_ai_runtime())["enabled"])


def resolve_system_prompt(task: str, fallback: str | None, stored: dict[str, str]) -> str:
    custom = (stored.get(task) or "").strip()
    if custom:
        return custom
    if fallback and fallback.strip():
        return fallback.strip()
    return default_system_prompts().get(task, "")


async def call_llm(
    *,
    system_prompt: str | None = None,
    user_prompt: str,
    max_tokens: int,
    temperature: float = 0.0,
    force: bool = False,
    task: str = "chat",
    platform: str | None = None,
) -> str:
    """Chạy một lần chat completion (Bee) qua app.ai.client.call_ai(). force=True dành
    cho import Settings do người vận hành bấm (vẫn chạy khi các bộ phân loại ingest
    đang tắt). Raise khi bị tắt, cấu hình sai hoặc provider lỗi - mọi bên gọi ở đây đều
    đã bắt rộng và fail open (xem ví dụ app/ai/tasks/post_relevance.py)."""
    cfg = await load_ai_runtime()
    if not cfg["enabled"] and not force:
        raise RuntimeError("ai_temporarily_disabled")
    resolved = resolve_system_prompt(task, system_prompt, cfg["prompts"])
    return await call_ai(
        provider=AI_PROVIDER,
        task=task,
        system_prompt=resolved,
        user_prompt=user_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
        platform=platform,
    )
