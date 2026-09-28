"""Kira as THE post-relevance classifier at ingest. The ingest consumer asks Kira about every post
that survives the deterministic rules (app/services/relevance_rules.py);
the keyword substring check is only the fallback when Kira gives no
verdict (off, over the daily cap, failed). Kira sees the target film's
facts (director/cast/distributor/release date) plus the other tracked
titles, which is what lets it separate "a post about THIS film" from a post
about a similarly named one or a foreign post sharing an unaccented
hashtag - the two error classes measured on 2026-09-28.

Prompt: task "post_relevance" (editable on the dashboard's Settings AI tab
like every other task; POST_RELEVANCE_SYSTEM_PROMPT is its default), ~950 tokens per post.

Spend control: respects the dashboard's Kira on/off toggle (call_kira
without force) and a per-day call cap (settings.kira_post_relevance_daily_cap,
counted in Redis). Fail-open: returns None on any problem, and
the caller then keeps the post rather than dropping it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger
from app.kira.client import call_kira, parse_json_response
from app.kira.post_relevance_prompt import POST_RELEVANCE_SYSTEM_PROMPT

logger = get_logger(__name__)

TASK = "post_relevance"
MAX_CONTENT_CHARS = 1500
MAX_TOKENS = 1200
_LABELS = {"relevant": "related", "irrelevant": "not_related", "uncertain": "uncertain"}


def movie_block(movie: dict[str, Any]) -> str:
    lines = [f"TARGET FILM: {movie.get('title') or ''}"]
    for key, label in (("director", "Director"), ("cast", "Cast"), ("distributor", "Distributor")):
        if movie.get(key):
            lines.append(f"{label}: {str(movie[key])[:300]}")
    if movie.get("released_at"):
        lines.append(f"Release date: {str(movie['released_at'])[:10]}")
    return "\n".join(lines)


def user_prompt(
    *, movie: dict[str, Any], keyword: str | None, platform: str | None, content: str, other_titles: list[str]
) -> str:
    others = ", ".join(t for t in other_titles if t != movie.get("title"))
    return (
        f"{movie_block(movie)}\n"
        f"Found via: {keyword or movie.get('title') or ''} (platform: {platform or 'unknown'})\n"
        f"OTHER TRACKED FILMS (not the target): {others}\n\n"
        f"POST:\n{(content or '')[:MAX_CONTENT_CHARS]}"
    )


async def _within_daily_cap() -> bool:
    cap = settings.kira_post_relevance_daily_cap
    if cap <= 0:
        return False
    from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

    key = f"{REDIS_KEY_PREFIX}kira_post_relevance:{datetime.now(tz=UTC):%Y-%m-%d}"
    try:
        client = get_redis_client()
        count = await client.incr(key)
        if count == 1:
            await client.expire(key, 2 * 24 * 3600)
    except Exception as exc:  # noqa: BLE001 - no counter, no spend
        logger.warning("kira_post_relevance_cap_check_failed", error=exc)
        return False
    if count == cap + 1:
        logger.warning("kira_post_relevance_daily_cap_reached", cap=cap)
    return count <= cap


async def classify_post_relevance_kira(
    *,
    content: str | None,
    movie: dict[str, Any],
    keyword: str | None,
    platform: str | None,
    other_titles: list[str],
) -> dict[str, Any] | None:
    """{"label": related|not_related|uncertain, "confidence": float,
    "reason": str}, or None when Kira is off, over the daily cap,
    unreachable or answered in an unexpected shape."""
    if not content or not content.strip() or not movie.get("title"):
        return None
    if not await _within_daily_cap():
        return None
    try:
        response = await call_kira(
            task=TASK,
            system_prompt=POST_RELEVANCE_SYSTEM_PROMPT,
            user_prompt=user_prompt(
                movie=movie, keyword=keyword, platform=platform, content=content, other_titles=other_titles
            ),
            max_tokens=MAX_TOKENS,
            platform=platform,
        )
        parsed = parse_json_response(response)
    except Exception as exc:  # noqa: BLE001 - fail open, the caller keeps the post
        logger.warning("kira_post_relevance_failed", error=exc)
        return None
    if not isinstance(parsed, dict) or parsed.get("classification") not in _LABELS:
        logger.warning("kira_post_relevance_bad_reply", reply=str(parsed)[:200])
        return None
    try:
        confidence = float(parsed.get("score") or 0.0)
    except TypeError, ValueError:
        confidence = 0.0
    return {
        "label": _LABELS[parsed["classification"]],
        "confidence": confidence,
        "reason": str(parsed.get("reason") or "")[:200],
    }
