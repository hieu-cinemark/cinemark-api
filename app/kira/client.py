"""Kira-task policy layer over the shared app.ai_client: per-task enabled
toggle + system-prompt overrides (ai_settings table in Supabase), on top
of the provider-agnostic HTTP/retry/concurrency machinery in
app.ai_client. Every classifier in app/kira/ goes through call_kira() so
they share one enabled/prompts read and one retry budget."""

from __future__ import annotations

import time
from typing import Any

from app.ai_client import call_ai, invalidate_provider_cache, parse_json_response
from app.core.logging import get_logger
from app.kira.defaults import default_system_prompts

__all__ = [
    "active_report_provider",
    "call_kira",
    "invalidate_ai_runtime_cache",
    "kira_is_enabled",
    "load_ai_runtime",
    "parse_json_response",
]

logger = get_logger(__name__)

_ai_cfg_cache: tuple[float, dict[str, Any]] | None = None
_AI_CFG_TTL_SECONDS = 5.0


def _normalize_prompts(raw: object) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, str) and value.strip():
            out[key] = value
    return out


async def load_ai_runtime() -> dict[str, Any]:
    """enabled + per-task prompt overrides from Supabase's ai_settings row,
    falling back to disabled/no-overrides if the table isn't there yet.
    Provider credentials/model are a separate concern - see
    app.ai_client.load_provider()."""
    global _ai_cfg_cache
    now = time.monotonic()
    if _ai_cfg_cache is not None and now - _ai_cfg_cache[0] < _AI_CFG_TTL_SECONDS:
        return _ai_cfg_cache[1]
    cfg: dict[str, Any] = {"enabled": False, "prompts": {}, "active_report_provider": "bee", "updated_at": None}
    try:
        from app.services.platform_config_db import get_ai_settings

        row = await get_ai_settings()
        cfg = {
            "enabled": bool(row.get("enabled")),
            "prompts": _normalize_prompts(row.get("prompts")),
            "active_report_provider": (row.get("active_report_provider") or "bee").strip().lower(),
            "updated_at": row.get("updated_at"),
        }
    except Exception as exc:
        logger.warning("ai_settings_load_failed", error=str(exc))
    _ai_cfg_cache = (now, cfg)
    return cfg


async def active_report_provider() -> str:
    """"kira" or "bee" - which provider app/bee/report.py's
    generate_topics_and_verbatims/generate_narrative should call, switchable
    from the dashboard's AI settings tab without touching ai_providers'
    own credentials. Defaults to "bee" (report generation's original,
    still-supported provider) if unset/unrecognized."""
    value = (await load_ai_runtime())["active_report_provider"]
    return value if value in ("kira", "bee") else "bee"


def invalidate_ai_runtime_cache() -> None:
    global _ai_cfg_cache
    _ai_cfg_cache = None
    invalidate_provider_cache("kira")


async def kira_is_enabled() -> bool:
    return bool((await load_ai_runtime())["enabled"])


def resolve_system_prompt(task: str, fallback: str | None, stored: dict[str, str]) -> str:
    custom = (stored.get(task) or "").strip()
    if custom:
        return custom
    if fallback and fallback.strip():
        return fallback.strip()
    return default_system_prompts().get(task, "")


async def call_kira(
    *,
    system_prompt: str | None = None,
    user_prompt: str,
    max_tokens: int,
    temperature: float = 0.0,
    force: bool = False,
    task: str = "chat",
    platform: str | None = None,
) -> str:
    """Runs one Kira chat completion via app.ai_client.call_ai(). force=True
    is for operator-triggered Settings import (works even while ingest
    classifiers are toggled off). Raises on disabled/misconfigured/
    provider errors - every caller here already catches broadly and fails
    open (see e.g. app/kira/relevance.py)."""
    cfg = await load_ai_runtime()
    if not cfg["enabled"] and not force:
        raise RuntimeError("kira_temporarily_disabled")
    resolved = resolve_system_prompt(task, system_prompt, cfg["prompts"])
    return await call_ai(
        provider="kira",
        task=task,
        system_prompt=resolved,
        user_prompt=user_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
        platform=platform,
    )
