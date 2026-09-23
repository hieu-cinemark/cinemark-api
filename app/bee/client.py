"""Beeknoee (platform.beeknoee.com) LLM client - an OpenAI-compatible
proxy this product uses for Claude Sonnet 5. Kept as a separate provider
from app/kira/ (kiraai.vn) on purpose: Kira still owns relevance*/import-
parsing/general tasks, Bee/Sonnet owns comment topic-clustering + report
narrative (app/bee/report.py) and is the sentiment fallback when PhoBERT
is down (app/kira/sentiment.py). Own concurrency budget and on/off check -
never shares Kira's KIRA_CONCURRENCY semaphore or kira_is_enabled() toggle,
since a Bee outage/rate-limit has nothing to do with Kira's.

* per-post relevance at ingest time no longer goes through either
provider - see app/services/relevance_phobert.py, which replaced the old
Kira call there with the already-trained local PhoBERT model.
"""

from __future__ import annotations

import asyncio
import json
import re
import time

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_BEE_MODEL = "bee/claude-sonnet-5"

# Same rationale as app/kira/client.py's own KIRA_CONCURRENCY - one
# process-wide budget so a burst of report generation can't open dozens of
# concurrent Beeknoee requests at once.
BEE_CONCURRENCY = asyncio.Semaphore(2)

_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

_client = None


def bee_is_configured() -> bool:
    return bool(settings.beeknoee_api_key and settings.beeknoee_base_url)


def _get_client():
    global _client
    if _client is None:
        if not bee_is_configured():
            raise RuntimeError("BEEKNOEE_API_KEY/BEEKNOEE_BASE_URL must be set in .env to call Bee.")
        from openai import OpenAI

        _client = OpenAI(base_url=settings.beeknoee_base_url, api_key=settings.beeknoee_api_key)
    return _client


async def call_bee(
    *,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    temperature: float = 0.0,
    model: str = DEFAULT_BEE_MODEL,
    task: str = "chat",
) -> str:
    """One chat completion via Beeknoee. Runs the (sync) OpenAI SDK call
    off the event loop, same pattern as app/kira/client.py's call_kira.
    Raises on failure (including "not configured") - callers should catch
    broadly and fail open, same convention as every Kira classifier."""
    if not bee_is_configured():
        raise RuntimeError("bee_not_configured")

    def _invoke():
        client = _get_client()
        return client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )

    logger.info(
        "bee_call_started",
        task=task,
        model=model,
        system_prompt_chars=len(system_prompt or ""),
        user_prompt_chars=len(user_prompt or ""),
    )
    started = time.perf_counter()
    async with BEE_CONCURRENCY:
        completion = await asyncio.to_thread(_invoke)
    latency_ms = int((time.perf_counter() - started) * 1000)

    choice = completion.choices[0] if completion.choices else None
    content = (choice.message.content if choice and choice.message else None) or ""
    logger.info(
        "bee_call_finished",
        task=task,
        model=model,
        latency_ms=latency_ms,
        content_chars=len(content),
        finish_reason=getattr(choice, "finish_reason", None) if choice else None,
    )
    return content


def parse_json_response(response: str) -> dict | list:
    """Strips a Markdown code fence if the model wrapped its JSON answer in
    one despite instructions not to, then json.loads()s it. Raises on
    anything malformed - callers should catch broadly and fail open. Own
    copy rather than importing app.kira.client's identical helper, so
    app/bee/ never depends on app/kira/ at all."""
    cleaned = _JSON_FENCE_RE.sub("", response.strip())
    return json.loads(cleaned)
