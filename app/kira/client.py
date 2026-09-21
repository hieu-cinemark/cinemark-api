"""Shared low-level Kira call primitive - retry/backoff/concurrency-limiting
wrapper around KiraAI.complete(), used by every classifier in app/kira/
so they share ONE process-scoped rate-limit budget and one KiraResponse
shape (see app.kira.base)."""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from typing import Any

import openai

from app.core.config import settings
from app.core.logging import get_logger
from app.kira.base import KiraResponse, get_kira_ai, reset_kira_ai
from app.kira.defaults import DEFAULT_KIRA_MODEL, default_system_prompts

logger = get_logger(__name__)

JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

# Env default when the ai_settings row has never been saved. Dashboard
# Settings is the runtime switch (see kira_is_enabled).
KIRA_ENABLED = settings.kira_enabled

KIRA_CONCURRENCY = asyncio.Semaphore(2)
_MAX_RATE_LIMIT_RETRIES = 3
_RETRY_BASE_SECONDS = 2.0

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
    """enabled/model/prompts from Postgres, falling back to env + code
    defaults if DATABASE_URL is missing or the table isn't there yet."""
    global _ai_cfg_cache
    now = time.monotonic()
    if _ai_cfg_cache is not None and now - _ai_cfg_cache[0] < _AI_CFG_TTL_SECONDS:
        return _ai_cfg_cache[1]
    cfg: dict[str, Any] = {
        "enabled": bool(settings.kira_enabled),
        "model": DEFAULT_KIRA_MODEL,
        "prompts": {},
        "updated_at": None,
    }
    try:
        from app.services.platform_config_db import get_ai_settings

        row = await get_ai_settings()
        cfg = {
            "enabled": bool(row.get("enabled")),
            "model": (row.get("model") or DEFAULT_KIRA_MODEL).strip() or DEFAULT_KIRA_MODEL,
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
    reset_kira_ai()


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
    extra: dict[str, Any] | None = None,
) -> str:
    """Runs KiraAI.complete() off the event loop. Retries 429s. force=True
    is for operator-triggered Settings import. Returns the message content
    string; the structured KiraResponse is always logged."""
    cfg = await load_ai_runtime()
    if not cfg["enabled"] and not force:
        raise RuntimeError("kira_temporarily_disabled")
    resolved = resolve_system_prompt(task, system_prompt, cfg["prompts"])
    model = cfg["model"]
    async with KIRA_CONCURRENCY:
        for attempt in range(1, _MAX_RATE_LIMIT_RETRIES + 1):
            try:
                def _invoke() -> KiraResponse:
                    return get_kira_ai(model).complete(
                        task=task,
                        user_prompt=user_prompt,
                        system_prompt=resolved,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        platform=platform,
                        extra=extra,
                    )

                result = await asyncio.to_thread(_invoke)
                return result.content
            except openai.RateLimitError:
                if attempt == _MAX_RATE_LIMIT_RETRIES:
                    logger.error(
                        "kira_call_failed",
                        task=task,
                        model=model,
                        platform=platform,
                        error="rate_limited",
                        attempts=attempt,
                    )
                    raise
                delay = _RETRY_BASE_SECONDS * attempt + random.uniform(0, 1)
                logger.warning("kira_rate_limited_retrying", attempt=attempt, delay_seconds=round(delay, 1), task=task)
                await asyncio.sleep(delay)
            except Exception as exc:
                logger.error("kira_call_failed", task=task, model=model, platform=platform, error=str(exc))
                raise


def parse_json_response(response: str) -> dict | list:
    """Strips a Markdown code fence if the model wrapped its JSON answer in
    one despite instructions not to, then json.loads()s it. Raises on
    anything malformed - callers should catch broadly and fail open."""
    cleaned = JSON_FENCE_RE.sub("", response.strip())
    return json.loads(cleaned)
