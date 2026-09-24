"""Beeknoee (platform.beeknoee.com) LLM client - an OpenAI-compatible
proxy this product uses for Claude Sonnet 5. Thin, provider-specific
facade over app.ai_client.call_ai(provider="bee", ...): the actual HTTP
client, retry/backoff and concurrency budget live there (shared shape
with app/kira/, but Bee gets its own semaphore keyed separately, so a Bee
outage/rate-limit can't starve Kira or vice versa).

Kira still owns relevance*/import-parsing/general tasks; Bee owns comment
topic-clustering + report narrative (app/bee/report.py) and is the
sentiment fallback when PhoBERT is down (app/kira/sentiment.py).

* per-post relevance at ingest time no longer goes through either
provider - see app/services/relevance_phobert.py, which replaced the old
Kira call there with the already-trained local PhoBERT model.
"""

from __future__ import annotations

from app.ai_client import call_ai, is_provider_configured, parse_json_response

__all__ = ["DEFAULT_BEE_MODEL", "bee_is_configured", "call_bee", "parse_json_response"]

DEFAULT_BEE_MODEL = "bee/claude-sonnet-5"


async def bee_is_configured() -> bool:
    return await is_provider_configured("bee")


async def call_bee(
    *,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    temperature: float = 0.0,
    model: str | None = None,
    task: str = "chat",
) -> str:
    """One chat completion via Beeknoee. Raises on failure (including "not
    configured") - callers should catch broadly and fail open, same
    convention as every Kira classifier."""
    return await call_ai(
        provider="bee",
        task=task,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )
