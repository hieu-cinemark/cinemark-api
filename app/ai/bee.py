"""Beeknoee (platform.beeknoee.com) LLM client - an OpenAI-compatible
proxy this product uses for Claude Sonnet 5. Thin, provider-specific
facade over app.ai.client.call_ai(provider="bee", ...): the actual HTTP
client, retry/backoff and concurrency budget live there (shared shape
with app/ai/kira.py, but Bee gets its own semaphore keyed separately, so a Bee
outage/rate-limit can't starve Kira or vice versa).

Not used by default since 2026-10-03 (out of credit): reports and every
classifier run on Kira. Bee only writes the social-topic reports again
when the dashboard's report provider is set to "bee" (app/ai/tasks/
report.py), with Kira as the fallback when a Bee call fails.
"""

from __future__ import annotations

from app.ai.client import call_ai, is_provider_configured, parse_json_response

__all__ = ["bee_is_configured", "call_bee", "parse_json_response"]


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
