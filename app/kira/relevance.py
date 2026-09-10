"""Kira-based relevance classification for scraped posts, using the
SYSTEM_PROMPT/DATA_PROMPT pair in app/kira/prompt.py. Called from
app/workers/ingest_consumer/main.py to decide keyword_match with semantic
understanding (synonyms, abbreviations, unaccented spelling) instead of the
exact-substring check in app/services/d1.py's _contains_keyword, which loses
posts that never literally contain the configured keyword phrase."""

from __future__ import annotations

import asyncio
import json
import random
import re
from typing import Any

import openai

from app.core.logging import get_logger
from app.kira.base import kira_ai
from app.kira.prompt import DATA_PROMPT, SYSTEM_PROMPT

logger = get_logger(__name__)

_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

# Caps concurrent Kira calls process-wide, independent of
# ingest_consumer.py's own _MESSAGE_CONCURRENCY (8) - classifying every one
# of 8 concurrent messages at once was firing 8 simultaneous Kira requests
# and tripping the provider's rate limit, regardless of total volume over
# time. 2 is deliberately conservative; raise it only after confirming the
# account's actual rate limit headroom.
_KIRA_CONCURRENCY = asyncio.Semaphore(2)
_MAX_RATE_LIMIT_RETRIES = 3
_RETRY_BASE_SECONDS = 2.0


async def _call_kira(*, system_prompt: str, user_prompt: str, max_tokens: int) -> str:
    """KiraAI.chat() uses a sync OpenAI client - run it off the event loop
    so one classification call doesn't stall every other concurrent
    message the ingest consumer's semaphore is juggling. Retries with
    backoff specifically on a 429 (RateLimitError) - anything else (bad
    JSON, auth, network) fails immediately since a retry won't help."""
    async with _KIRA_CONCURRENCY:
        for attempt in range(1, _MAX_RATE_LIMIT_RETRIES + 1):
            try:
                return await asyncio.to_thread(
                    kira_ai.chat,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.0,
                    max_tokens=max_tokens,
                )
            except openai.RateLimitError:
                if attempt == _MAX_RATE_LIMIT_RETRIES:
                    raise
                delay = _RETRY_BASE_SECONDS * attempt + random.uniform(0, 1)
                logger.warning("kira_rate_limited_retrying", attempt=attempt, delay_seconds=round(delay, 1))
                await asyncio.sleep(delay)


def _field(draft: dict[str, Any], *keys: str) -> str:
    """First non-empty value for any of `keys`, checked on the normalized
    draft first and then on its raw platform payload (draft's own fields -
    see app/services/platforms.py - only cover a subset of what DATA_PROMPT
    asks for; the rest, when a platform happens to provide it, only exists
    under draft["raw"])."""
    raw = draft.get("raw") or {}
    for key in keys:
        value = draft.get(key) or raw.get(key)
        if value:
            return str(value)
    return "N/A"


async def classify_relevance(keyword: str, draft: dict[str, Any]) -> dict[str, Any] | None:
    """Runs one scraped post through Kira's relevance classifier. Returns
    the parsed {relevant, classification, score, reason, evidence} dict, or
    None if the call or the response parsing failed - callers should fall
    back to substring matching rather than blocking ingestion on an LLM
    hiccup (bad credentials, provider outage, malformed JSON back)."""
    prompt = DATA_PROMPT.format(
        keyword=keyword,
        title=_field(draft, "title"),
        description=_field(draft, "description"),
        content=draft.get("content") or "N/A",
        caption=_field(draft, "caption"),
        hashtags=_field(draft, "hashtags"),
        comments=_field(draft, "comments"),
        metadata=json.dumps(draft.get("raw"), ensure_ascii=False) if draft.get("raw") else "N/A",
        image=_field(draft, "media_url", "image_url"),
    )
    try:
        # kira-3.5-flash is a reasoning model - it spends reasoning_content
        # tokens before ever writing the JSON answer to content, and
        # SYSTEM_PROMPT's long rule set induces more of that than a short
        # prompt does. A tight max_tokens truncates mid-thought
        # (finish_reason="length", content="") rather than saving cost -
        # confirmed directly: 500 intermittently returned "" for this same
        # prompt/input, 1500 didn't.
        response = await _call_kira(system_prompt=SYSTEM_PROMPT, user_prompt=prompt, max_tokens=1500)
        cleaned = _JSON_FENCE_RE.sub("", response.strip())
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict) or "relevant" not in parsed:
            raise ValueError(f"unexpected relevance shape: {cleaned[:200]!r}")
        return parsed
    except Exception as exc:
        logger.warning("kira_relevance_failed", keyword=keyword, error=str(exc))
        return None
