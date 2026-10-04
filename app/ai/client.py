"""Client LLM tương thích OpenAI dùng chung cho mọi provider (kira, bee, ...). Thông
tin của provider (base_url/api_key/model) là các dòng trong bảng ai_providers trên
Supabase (xem app/services/platform_config_db.py) - được nạp và cache ở đây thay vì
dùng biến môi trường, nên có thể đổi key hay thêm provider mới từ dashboard mà
không cần deploy lại.

app/ai/kira.py và app/ai/bee.py là các lớp mỏng, riêng cho từng provider, bọc
call_ai() bên dưới: mỗi lớp giữ chính sách riêng của provider đó (Kira có nút
bật/tắt và prompt ghi đè theo task trong ai_settings; Bee không có cả hai). Module
này gom những gì trước đây bị lặp giữa client KiraAI và client Bee cũ - tạo
OpenAI client, retry/backoff khi gặp 429, mỗi provider một giới hạn đồng thời, và
một định dạng log thống nhất cho mỗi lời gọi."""

from __future__ import annotations

import asyncio
import contextvars
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
# Cùng đánh đổi như _AI_CFG_TTL_SECONDS của app/ai/kira.py (mỗi lần đọc Supabase
# mất khoảng 3 giây).
_PROVIDER_CACHE_TTL_SECONDS = 60.0

# Lượng token của lần call_ai() gần nhất trong asyncio task hiện tại - bên gọi đọc
# ngay sau khi await call_ai() nếu cần tính chi phí theo ngân sách (ví dụ một script
# gán nhãn hàng loạt). Dùng ContextVar để các task chạy song song mỗi task thấy
# lượng dùng của chính lời gọi của nó.
last_call_usage: contextvars.ContextVar[AIUsage | None] = contextvars.ContextVar("last_call_usage", default=None)


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
    """Xoá cấu hình provider đã cache để lần gọi sau đọc lại từ Supabase (và tạo lại
    OpenAI client nếu base_url/api_key đổi) - gọi ngay sau khi sửa ai_providers trên
    dashboard. key=None xoá cache của mọi provider."""
    if key is None:
        _provider_cache.clear()
    else:
        _provider_cache.pop(key, None)


async def load_provider(key: str) -> ProviderConfig | None:
    """Đọc {key, base_url, api_key, model} từ ai_providers (Supabase), cache vài giây.
    Trả về None nếu dòng chưa tồn tại hoặc thiếu base_url/api_key - bên gọi coi như
    "chưa cấu hình", cùng quy ước fail open như mọi chỗ khác trong app/ai."""
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
    """Một lần chat completion theo dòng ai_providers của `provider`. Retry khi gặp 429
    có backoff (cùng ngân sách retry mà trước đây mỗi provider tự cài), mỗi provider
    một semaphore riêng (để Bee gọi dồn dập cũng không làm Kira bị nghẽn và ngược
    lại - mỗi provider vẫn có semaphore riêng, chỉ là được quản lý theo key ở đây thay
    vì mỗi client một hằng số riêng ở cấp module).
    Raise RuntimeError nếu dòng provider thiếu hoặc chưa đủ thông tin, hoặc raise lỗi
    OpenAI gốc sau khi đã retry hết - mọi bên gọi trong app/kira và app/bee đều đã bắt
    rộng và fail open."""
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
                last_call_usage.set(usage)
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
                    "ai_rate_limited_retrying",
                    provider=provider,
                    attempt=attempt,
                    delay_seconds=round(delay, 1),
                    task=task,
                )
                await asyncio.sleep(delay)
            except Exception as exc:
                logger.error(
                    "ai_call_failed",
                    provider=provider,
                    task=task,
                    model=resolved_model,
                    platform=platform,
                    error=str(exc),
                )
                raise


def parse_json_response(response: str) -> dict | list:
    """Bỏ khối code Markdown nếu model lỡ bọc câu trả lời JSON trong đó dù đã được dặn
    không làm vậy, rồi json.loads(). Raise với mọi dữ liệu sai định dạng - bên gọi
    nên bắt rộng và fail open."""
    cleaned = JSON_FENCE_RE.sub("", response.strip())
    return json.loads(cleaned)
