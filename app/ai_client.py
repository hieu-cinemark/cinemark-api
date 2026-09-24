"""Unified OpenAI-compatible LLM client shared by every provider (kira,
bee, ...). Provider credentials (base_url/api_key/model) are rows in
Supabase's ai_providers table (see app/services/platform_config_db.py) -
loaded and cached here, instead of env vars, so a key can be rotated or a
new provider added from the dashboard without a redeploy.

app/kira/client.py and app/bee/client.py are thin, provider-specific
facades over call_ai() below: they keep each provider's own policy (Kira's
enabled-toggle + per-task prompt overrides live in ai_settings; Bee has
neither). This module owns what used to be duplicated between
app/kira/base.py's KiraAI class and app/bee/client.py - building the
OpenAI client, retry/backoff on 429s, one concurrency budget per provider,
and one structured log shape per call."""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from dataclasses import dataclass
from typing import Any

import openai
from openai import OpenAI

from app.core.logging import get_logger

logger = get_logger(__name__)

JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

_PROMPT_LOG_CHARS = 2000
_CONTENT_LOG_CHARS = 4000
_MAX_RATE_LIMIT_RETRIES = 3
_RETRY_BASE_SECONDS = 2.0
_PROVIDER_CACHE_TTL_SECONDS = 5.0


def _preview(text: str, limit: int) -> str:
    value = text or ""
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...<{len(value) - limit} more chars>"


@dataclass
class ProviderConfig:
    key: str
    base_url: str
    api_key: str
    model: str


@dataclass
class AIUsage:
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None

    def as_log_fields(self) -> dict[str, int | None]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass
class AIResponse:
    ok: bool
    provider: str
    task: str
    model: str
    content: str = ""
    finish_reason: str | None = None
    usage: AIUsage | None = None
    latency_ms: int = 0
    platform: str | None = None

    def as_log_fields(self) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "ok": self.ok,
            "provider": self.provider,
            "task": self.task,
            "model": self.model,
            "finish_reason": self.finish_reason,
            "latency_ms": self.latency_ms,
            "content_chars": len(self.content or ""),
            "content": _preview(self.content, _CONTENT_LOG_CHARS),
            "platform": self.platform,
        }
        if self.usage is not None:
            fields.update(self.usage.as_log_fields())
        return fields


_provider_cache: dict[str, tuple[float, ProviderConfig | None]] = {}
_openai_clients: dict[tuple[str, str], OpenAI] = {}
_concurrency: dict[str, asyncio.Semaphore] = {}


def invalidate_provider_cache(key: str | None = None) -> None:
    """Drops the cached provider config so the next call re-reads Supabase
    (and rebuilds the OpenAI client if base_url/api_key changed) - call
    this right after a dashboard edit to ai_providers. key=None clears
    every provider."""
    if key is None:
        _provider_cache.clear()
    else:
        _provider_cache.pop(key, None)


async def load_provider(key: str) -> ProviderConfig | None:
    """Reads {key, base_url, api_key, model} from ai_providers (Supabase),
    cached for a few seconds. None if the row doesn't exist yet or is
    missing base_url/api_key - callers treat that as "not configured",
    same fail-open convention as everything else in app/kira and app/bee."""
    now = time.monotonic()
    cached = _provider_cache.get(key)
    if cached is not None and now - cached[0] < _PROVIDER_CACHE_TTL_SECONDS:
        return cached[1]

    cfg: ProviderConfig | None = None
    try:
        from app.services.platform_config_db import get_ai_provider

        row = await get_ai_provider(key)
        if row and row.get("base_url") and row.get("api_key"):
            cfg = ProviderConfig(
                key=key,
                base_url=row["base_url"],
                api_key=row["api_key"],
                model=(row.get("model") or "").strip(),
            )
    except Exception as exc:
        logger.warning("ai_provider_load_failed", provider=key, error=str(exc))
    _provider_cache[key] = (now, cfg)
    return cfg


async def is_provider_configured(key: str) -> bool:
    return (await load_provider(key)) is not None


def _client_for(cfg: ProviderConfig) -> OpenAI:
    cache_key = (cfg.base_url, cfg.api_key)
    client = _openai_clients.get(cache_key)
    if client is None:
        client = OpenAI(base_url=cfg.base_url, api_key=cfg.api_key)
        _openai_clients[cache_key] = client
    return client


def _concurrency_for(key: str) -> asyncio.Semaphore:
    sem = _concurrency.get(key)
    if sem is None:
        sem = asyncio.Semaphore(2)
        _concurrency[key] = sem
    return sem


def _invoke(
    cfg: ProviderConfig, *, model: str, system_prompt: str, user_prompt: str, temperature: float, max_tokens: int | None
) -> Any:
    client = _client_for(cfg)
    params: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
    }
    if max_tokens is not None:
        params["max_tokens"] = max_tokens
    return client.chat.completions.create(**params)


async def call_ai(
    *,
    provider: str,
    task: str,
    user_prompt: str,
    system_prompt: str = "",
    model: str | None = None,
    temperature: float = 0.0,
    max_tokens: int | None = None,
    platform: str | None = None,
) -> str:
    """One chat completion against `provider`'s ai_providers row. Retries
    429s with backoff (same budget every provider used to implement on its
    own), one concurrency semaphore per provider (so a Bee burst can't
    starve Kira or vice versa - each provider still gets its own, just
    keyed here instead of as a separate module-level constant per client).
    Raises RuntimeError if the provider row is missing/incomplete, or the
    underlying OpenAI error after exhausting retries - every caller in
    app/kira and app/bee already catches broadly and fails open."""
    cfg = await load_provider(provider)
    if cfg is None:
        raise RuntimeError(f"ai_provider_not_configured:{provider}")
    resolved_model = (model or cfg.model or "").strip()
    if not resolved_model:
        raise RuntimeError(f"ai_provider_model_not_configured:{provider}")

    logger.info(
        "ai_call_started",
        provider=provider,
        task=task,
        model=resolved_model,
        platform=platform,
        temperature=temperature,
        max_tokens=max_tokens,
        system_prompt_chars=len(system_prompt or ""),
        user_prompt_chars=len(user_prompt or ""),
        system_prompt=_preview(system_prompt or "", _PROMPT_LOG_CHARS),
        user_prompt=_preview(user_prompt or "", _PROMPT_LOG_CHARS),
    )

    async with _concurrency_for(provider):
        for attempt in range(1, _MAX_RATE_LIMIT_RETRIES + 1):
            try:
                started = time.perf_counter()
                completion = await asyncio.to_thread(
                    _invoke,
                    cfg,
                    model=resolved_model,
                    system_prompt=system_prompt or "",
                    user_prompt=user_prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                latency_ms = int((time.perf_counter() - started) * 1000)

                choice = completion.choices[0] if completion.choices else None
                content = (choice.message.content if choice and choice.message else None) or ""
                finish_reason = getattr(choice, "finish_reason", None) if choice else None
                raw_usage = getattr(completion, "usage", None)
                usage = None
                if raw_usage is not None:
                    usage = AIUsage(
                        prompt_tokens=getattr(raw_usage, "prompt_tokens", None),
                        completion_tokens=getattr(raw_usage, "completion_tokens", None),
                        total_tokens=getattr(raw_usage, "total_tokens", None),
                    )
                result = AIResponse(
                    ok=bool(content),
                    provider=provider,
                    task=task,
                    model=resolved_model,
                    content=content,
                    finish_reason=str(finish_reason) if finish_reason else None,
                    usage=usage,
                    latency_ms=latency_ms,
                    platform=platform,
                )
                logger.info("ai_call_finished", **result.as_log_fields())
                return content
            except openai.RateLimitError:
                if attempt == _MAX_RATE_LIMIT_RETRIES:
                    logger.error(
                        "ai_call_failed",
                        provider=provider,
                        task=task,
                        model=resolved_model,
                        platform=platform,
                        error="rate_limited",
                        attempts=attempt,
                    )
                    raise
                delay = _RETRY_BASE_SECONDS * attempt + random.uniform(0, 1)
                logger.warning(
                    "ai_rate_limited_retrying", provider=provider, attempt=attempt, delay_seconds=round(delay, 1), task=task
                )
                await asyncio.sleep(delay)
            except Exception as exc:
                logger.error("ai_call_failed", provider=provider, task=task, model=resolved_model, platform=platform, error=str(exc))
                raise


def parse_json_response(response: str) -> dict | list:
    """Strips a Markdown code fence if the model wrapped its JSON answer in
    one despite instructions not to, then json.loads()s it. Raises on
    anything malformed - callers should catch broadly and fail open."""
    cleaned = JSON_FENCE_RE.sub("", response.strip())
    return json.loads(cleaned)
