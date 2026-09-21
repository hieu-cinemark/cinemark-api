"""Kira-based relevance classification for scraped posts, using the
SYSTEM_PROMPT/DATA_PROMPT pair in app/kira/prompt.py. Called from
app/workers/ingest_consumer/main.py to decide keyword_match with semantic
understanding (synonyms, abbreviations, unaccented spelling) instead of the
exact-substring check in app/services/d1.py's _contains_keyword, which loses
posts that never literally contain the configured keyword phrase."""

from __future__ import annotations

import json
from typing import Any

from app.core.logging import get_logger
from app.kira.client import call_kira, kira_is_enabled, parse_json_response
from app.kira.prompt import DATA_PROMPT, SYSTEM_PROMPT

logger = get_logger(__name__)


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


def _movie_context(keyword_row: dict[str, Any] | None) -> str:
    """Renders the disambiguating movie facts (title/director/cast/
    distributor) attached to a keyword row - see get_keyword's own SELECT
    (app/services/d1.py) - as one block for DATA_PROMPT's MOVIE INFO
    section. None/empty when the caller passes a bare keyword string
    (older call sites, or a keyword row that predates these columns) -
    "N/A" degrades to the previous keyword-only behavior rather than
    crashing on a missing key.

    Why this exists at all: TARGET KEYWORD alone is ambiguous for a movie
    whose title collides with an unrelated, common real-world phrase or
    saying (Vietnamese example that motivated this: "Mẹ Mìn" is also a
    decades-old folk term for a child-abductor, a frequent topic on its
    own with zero connection to this specific film) - SYSTEM_PROMPT's own
    semantic-tolerance rules (needed for legitimate cases like "iPhone 17
    Pro Max" written as "con 17 Pro Max") would otherwise happily approve
    any post about the generic topic, since nothing in a bare keyword
    string tells Kira "this is a movie" at all. Concrete facts only Kira's
    training data can't already know for real-world, tell-them-apart
    grounding - not padding."""
    if not keyword_row:
        return "N/A"
    facts = []
    if keyword_row.get("movie_title"):
        facts.append(f"Movie title: {keyword_row['movie_title']}")
    if keyword_row.get("movie_director"):
        facts.append(f"Director: {keyword_row['movie_director']}")
    if keyword_row.get("movie_cast"):
        facts.append(f"Cast: {keyword_row['movie_cast']}")
    if keyword_row.get("movie_distributor"):
        facts.append(f"Distributor: {keyword_row['movie_distributor']}")
    return "\n".join(facts) if facts else "N/A"


async def classify_relevance(keyword: str, draft: dict[str, Any], keyword_row: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Runs one scraped post through Kira's relevance classifier. Returns
    the parsed {relevant, classification, score, reason, evidence} dict, or
    None if the call or the response parsing failed - callers should fall
    back to substring matching rather than blocking ingestion on an LLM
    hiccup (bad credentials, provider outage, malformed JSON back).

    keyword_row is the full row from app.services.d1.get_keyword (movie_*
    columns included) when the caller has it - see _movie_context above
    for why passing just the bare `keyword` string isn't enough to reliably
    tell "this movie" apart from an unrelated real-world phrase that
    happens to share its title."""
    if not await kira_is_enabled():
        return None
    prompt = DATA_PROMPT.format(
        keyword=keyword,
        movie_context=_movie_context(keyword_row),
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
        response = await call_kira(
            task="relevance",
            system_prompt=SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=1500,
            platform=str(draft.get("platform") or "") or None,
        )
        parsed = parse_json_response(response)
        if not isinstance(parsed, dict) or "relevant" not in parsed:
            raise ValueError(f"unexpected relevance shape: {json.dumps(parsed)[:200]!r}")
        return parsed
    except Exception as exc:
        logger.warning("kira_relevance_failed", keyword=keyword, error=str(exc))
        return None
