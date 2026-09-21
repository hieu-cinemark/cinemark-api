"""Kira client + unified call result.

Every classifier (relevance/sentiment/reports/import) and every crawl-side
caller (Facebook/Threads/TikTok diagnosis, selector, hashtag BFS) should
go through KiraAI.complete() / call_kira() so logs and the returned
KiraResponse share one shape: model, task, prompts, usage, latency, error.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger
from app.kira.defaults import DEFAULT_KIRA_MODEL, default_system_prompts

logger = get_logger(__name__)

_PROMPT_LOG_CHARS = 2000
_CONTENT_LOG_CHARS = 4000


def _preview(text: str, limit: int) -> str:
    value = text or ""
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...<{len(value) - limit} more chars>"


@dataclass
class KiraUsage:
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
class KiraResponse:
    """Unified return type for every Kira call, ingest or crawl-side."""

    ok: bool
    task: str
    model: str
    content: str = ""
    finish_reason: str | None = None
    usage: KiraUsage | None = None
    latency_ms: int = 0
    error: str | None = None
    platform: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def as_log_fields(self) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "ok": self.ok,
            "task": self.task,
            "model": self.model,
            "finish_reason": self.finish_reason,
            "latency_ms": self.latency_ms,
            "content_chars": len(self.content or ""),
            "content": _preview(self.content, _CONTENT_LOG_CHARS),
            "error": self.error,
            "platform": self.platform,
        }
        if self.usage is not None:
            fields.update(self.usage.as_log_fields())
        if self.extra:
            fields.update(self.extra)
        return fields


class KiraAI:
    def __init__(self, model: str | None = None):
        self.api_key = settings.kira_api_key
        self.base_url = settings.kira_base_url
        self.model = model or DEFAULT_KIRA_MODEL

        if not self.api_key:
            raise ValueError(
                "KIRA_API_KEY is not configured. "
                "Add KIRA_API_KEY to the .env file or environment."
            )

        if not self.base_url:
            raise ValueError(
                "KIRA_BASE_URL is not configured. "
                "Add KIRA_BASE_URL to the .env file or environment."
            )

        from openai import OpenAI

        self.client = OpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
        )

    def complete(
        self,
        *,
        task: str,
        user_prompt: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int | None = None,
        platform: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> KiraResponse:
        """One chat completion. Always returns KiraResponse (never raises
        for provider/parse issues here - OpenAI errors propagate so
        call_kira can retry 429s). Logs start + finish with prompts."""
        resolved_system = system_prompt if system_prompt is not None else default_system_prompts().get(task, "")
        params: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": resolved_system},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
        }
        if max_tokens is not None:
            params["max_tokens"] = max_tokens

        logger.info(
            "kira_call_started",
            task=task,
            model=self.model,
            platform=platform,
            temperature=temperature,
            max_tokens=max_tokens,
            system_prompt_chars=len(resolved_system),
            user_prompt_chars=len(user_prompt or ""),
            system_prompt=_preview(resolved_system, _PROMPT_LOG_CHARS),
            user_prompt=_preview(user_prompt or "", _PROMPT_LOG_CHARS),
        )
        started = time.perf_counter()
        completion = self.client.chat.completions.create(**params)
        latency_ms = int((time.perf_counter() - started) * 1000)

        choice = completion.choices[0] if completion.choices else None
        content = (choice.message.content if choice and choice.message else None) or ""
        finish_reason = getattr(choice, "finish_reason", None) if choice else None
        raw_usage = getattr(completion, "usage", None)
        usage = None
        if raw_usage is not None:
            usage = KiraUsage(
                prompt_tokens=getattr(raw_usage, "prompt_tokens", None),
                completion_tokens=getattr(raw_usage, "completion_tokens", None),
                total_tokens=getattr(raw_usage, "total_tokens", None),
            )

        result = KiraResponse(
            ok=bool(content),
            task=task,
            model=self.model,
            content=content,
            finish_reason=str(finish_reason) if finish_reason else None,
            usage=usage,
            latency_ms=latency_ms,
            platform=platform,
            extra=extra or {},
        )
        logger.info("kira_call_finished", **result.as_log_fields())
        return result

    def chat(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
        max_tokens: int | None = None,
    ) -> str:
        """Backward-compatible string body - prefer complete() / call_kira()."""
        return self.complete(
            task="chat",
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        ).content


_kira_ai: KiraAI | None = None
_kira_ai_model: str | None = None


def reset_kira_ai() -> None:
    """Drop the cached client so the next call picks up a new model name."""
    global _kira_ai, _kira_ai_model
    _kira_ai = None
    _kira_ai_model = None


def get_kira_ai(model: str | None = None) -> KiraAI:
    """Lazily constructs the shared KiraAI client. Recreates it when the
    configured model changes (dashboard Settings)."""
    global _kira_ai, _kira_ai_model
    wanted = (model or DEFAULT_KIRA_MODEL).strip() or DEFAULT_KIRA_MODEL
    if _kira_ai is None or _kira_ai_model != wanted:
        _kira_ai = KiraAI(model=wanted)
        _kira_ai_model = wanted
    return _kira_ai
