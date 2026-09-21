"""Kira calls behind scripts/generate_social_topic_reports.py - two
separate calls per movie per run (see app/kira/report_prompt.py's module
docstring for why they're split): topic clustering + verbatim selection
over a comment sample, and a short narrative blurb fed the real,
SQL-computed sentiment percentages so it can't drift from them. Both are
fail-open (return None on any error) like every other classifier in this
package - the caller decides what "no report this run" means."""

from __future__ import annotations

import json
from typing import Any

from app.core.logging import get_logger
from app.kira.client import call_kira, kira_is_enabled, parse_json_response
from app.kira.report_prompt import (
    NARRATIVE_DATA_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    TOPICS_DATA_PROMPT,
    TOPICS_SYSTEM_PROMPT,
)

logger = get_logger(__name__)


async def generate_topics_and_verbatims(movie_title: str, comments: list[dict[str, Any]]) -> dict[str, Any] | None:
    """`comments` is get_comment_sample_for_movie()'s rows (id/message/
    reactions_count/sentiment). Returns {"top_10_topics": [...],
    "top_10_verbatims": [...]} matching the frontend's ReportData shape, or
    None on failure."""
    if not await kira_is_enabled():
        return None
    comments_payload = [
        {"id": c["id"], "message": c["message"], "likes": c.get("reactions_count") or 0, "sentiment": c["sentiment"]}
        for c in comments
    ]
    prompt = TOPICS_DATA_PROMPT.format(
        movie_title=movie_title,
        comments_json=json.dumps(comments_payload, ensure_ascii=False),
    )
    try:
        # Large structured output (up to 10 topics x evidence_comments, plus
        # up to 10 verbatims) - a tight max_tokens here would truncate mid-
        # answer the same way relevance.py documented at a much smaller
        # output size, so this needs considerably more headroom.
        response = await call_kira(
            task="topics",
            system_prompt=TOPICS_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=8000,
        )
        parsed = parse_json_response(response)
        if not isinstance(parsed, dict) or "top_10_topics" not in parsed or "top_10_verbatims" not in parsed:
            raise ValueError(f"unexpected topics shape: {json.dumps(parsed)[:200]!r}")
        return parsed
    except Exception as exc:
        logger.warning("kira_topics_failed", movie_title=movie_title, error=str(exc))
        return None


async def generate_narrative(
    movie_title: str, *, positive_percent: float, negative_percent: float, neutral_percent: float, topic_names: list[str]
) -> str | None:
    """Returns the "analysis" blurb string, or None on failure - the caller
    should fall back to an empty string rather than block the report."""
    if not await kira_is_enabled():
        return None
    prompt = NARRATIVE_DATA_PROMPT.format(
        movie_title=movie_title,
        positive_percent=positive_percent,
        negative_percent=negative_percent,
        neutral_percent=neutral_percent,
        topic_names="\n".join(f"- {name}" for name in topic_names) or "N/A",
    )
    try:
        response = await call_kira(
            task="narrative",
            system_prompt=NARRATIVE_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=1500,
        )
        parsed = parse_json_response(response)
        analysis = parsed.get("analysis") if isinstance(parsed, dict) else None
        if not isinstance(analysis, str) or not analysis.strip():
            raise ValueError(f"unexpected narrative shape: {json.dumps(parsed)[:200]!r}")
        return analysis.strip()
    except Exception as exc:
        logger.warning("kira_narrative_failed", movie_title=movie_title, error=str(exc))
        return None
