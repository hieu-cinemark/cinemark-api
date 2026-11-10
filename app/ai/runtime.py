"""Lớp chính sách cho mọi lời gọi LLM.

Đọc cấu hình theo task (bật/tắt, system prompt ghi đè) từ bảng `ai_settings`, rồi gọi provider qua
`app.ai.providers.client`. Mọi task trong `app.ai` đi qua `call_llm` / `call_llm_json` để dùng chung cấu hình và retry.
"""

from __future__ import annotations

import time
from typing import Any

from app.ai.prompts.defaults import default_system_prompts
from app.ai.providers.client import call_ai, invalidate_provider_cache, parse_json_response
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
# Đọc ai_settings tốn ~3 giây (kết nối Supabase mới) nên giữ 60 giây; API tự xoá cache khi lưu cấu hình.
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
    """Cấu hình AI theo task: `{"enabled", "prompts", "updated_at"}`. Bảng chưa có thì tắt, không ghi đè."""
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
    """Một lần chat completion, trả nội dung trả lời.

    `force=True` chạy cả khi AI đang tắt (thao tác người vận hành bấm, job nền). Raise khi AI tắt, cấu hình sai hoặc
    provider lỗi; bên gọi tự bắt và fail open.
    """
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


async def call_llm_json(
    *, system_prompt: str, user_prompt: str, max_tokens: int, task: str, temperature: float = 0.1
) -> tuple[Any, str]:
    """Gọi LLM và parse JSON. Trả (dữ liệu hoặc None, nội dung thô); không raise.

    Dùng cho job nền (app/services/ai_jobs): lỗi provider hay JSON hỏng đều coi là một lượt hỏng để bên gọi tự xử lý.
    Chạy cả khi bộ phân loại ingest đang tắt AI (force=True).
    """
    try:
        content = await call_llm(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            force=True,
            task=task,
        )
    except Exception as exc:  # noqa: BLE001 - provider lỗi: coi là lượt hỏng
        logger.warning("llm_json_call_failed", task=task, error=str(exc)[:300])
        return None, ""
    try:
        return parse_json_response(content), content
    except ValueError:
        logger.warning("llm_json_unparsed", task=task, preview=content[:200])
        return None, content
