"""LLM calls behind scripts/generate_social_topic_reports.py - same
two-call split and same prompts either provider runs (see
app/kira/report_prompt.py's own module docstring for why split into two
calls). Both are fail-open (return None on any error), same convention as
every classifier in this codebase - the caller decides what "no report
this run" means.

Provider (Kira or Bee) is switchable from the dashboard's AI settings tab
(app/kira/client.py's active_report_provider(), default "bee") rather than
hardcoded - added 2026-09-25 when report generation turned out to be the
only real per-call-volume LLM task left in this product (relevance and
sentiment both moved to local PhoBERT - see app/services/relevance_phobert.py
and app/kira/sentiment.py's own docstrings), so it's worth being able to
point at either provider's budget without a code change. call_kira's own
force=True bypasses the separate ingest-classifiers enabled toggle, same
as app/kira/import_parser.py's own operator-triggered calls - picking
"kira" here is its own explicit enable signal, independent of that switch."""

from __future__ import annotations

import json
from typing import Any

from app.bee.client import call_bee
from app.core.logging import get_logger
from app.kira.client import active_report_provider, call_kira, parse_json_response
from app.kira.report_prompt import (
    NARRATIVE_DATA_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    TOPICS_DATA_PROMPT,
    TOPICS_SYSTEM_PROMPT,
)

logger = get_logger(__name__)


async def _call_report_llm(*, task: str, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float) -> str:
    provider = await active_report_provider()
    if provider == "kira":
        return await call_kira(
            task=task,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            force=True,
        )
    return await call_bee(
        task=task,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )


async def generate_topics_and_verbatims(movie_title: str, comments: list[dict[str, Any]]) -> dict[str, Any] | None:
    """`comments` is get_comment_sample_for_movie()'s rows (id/message/
    reactions_count/sentiment). Returns {"top_10_topics": [...],
    "top_10_verbatims": [...]} matching the frontend's ReportData shape, or
    None on failure."""
    comments_payload = [
        {"id": c["id"], "message": c["message"], "likes": c.get("reactions_count") or 0, "sentiment": c["sentiment"]}
        for c in comments
    ]
    prompt = TOPICS_DATA_PROMPT.format(
        movie_title=movie_title,
        comments_json=json.dumps(comments_payload, ensure_ascii=False),
    )
    try:
        # Confirmed live: 8000 (the old Kira version's own budget) truncated
        # Sonnet 5 mid-JSON (finish_reason="length") on a 146-comment movie -
        # Sonnet's output for this same up-to-10-topics x evidence_comments
        # plus up-to-10-verbatims shape runs noticeably more verbose than
        # whatever model Kira was actually running. Sonnet 5 comfortably
        # supports a much larger output window, so there's real headroom to
        # spend here rather than trimming the prompt/shape instead.
        #
        # temperature=0.3, not 0.0: confirmed live that Beeknoee caches by
        # (model, messages, temperature) but NOT max_tokens - the very
        # first (truncated, max_tokens=8000) call against this exact
        # prompt at temperature=0 got cached, and every later retry at
        # temperature=0 kept replaying that same truncated response
        # verbatim regardless of how high max_tokens was raised afterward.
        # A non-zero temperature avoids re-poisoning that cache for any
        # prompt this ever happens to again - also just a more natural
        # choice for a writing task than strict determinism. Kept the same
        # when the active provider is Kira instead - no evidence yet that
        # its backend lacks the same caching behavior, and a non-zero
        # temperature is a reasonable default for this task either way.
        response = await _call_report_llm(
            task="topics",
            system_prompt=TOPICS_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=32000,
            temperature=0.3,
        )
        parsed = parse_json_response(response)
        if not isinstance(parsed, dict) or "top_10_topics" not in parsed or "top_10_verbatims" not in parsed:
            raise ValueError(f"unexpected topics shape: {json.dumps(parsed)[:200]!r}")
        return parsed
    except Exception as exc:
        logger.warning("report_topics_failed", movie_title=movie_title, error=str(exc))
        return None


async def generate_narrative(
    movie_title: str, *, positive_percent: float, negative_percent: float, neutral_percent: float, topic_names: list[str]
) -> str | None:
    """Returns the "analysis" blurb string, or None on failure - the caller
    should fall back to an empty string rather than block the report."""
    prompt = NARRATIVE_DATA_PROMPT.format(
        movie_title=movie_title,
        positive_percent=positive_percent,
        negative_percent=negative_percent,
        neutral_percent=neutral_percent,
        topic_names="\n".join(f"- {name}" for name in topic_names) or "N/A",
    )
    try:
        # Same temperature=0.3 rationale as generate_topics_and_verbatims's
        # own call above (avoid caching a bad/truncated response forever at
        # temperature=0). max_tokens bumped from the old Kira budget for
        # the same reasoning-tokens-eat-the-budget headroom reason too,
        # though this shorter prompt hasn't been observed to need it yet.
        response = await _call_report_llm(
            task="narrative",
            system_prompt=NARRATIVE_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=4000,
            temperature=0.3,
        )
        parsed = parse_json_response(response)
        analysis = parsed.get("analysis") if isinstance(parsed, dict) else None
        if not isinstance(analysis, str) or not analysis.strip():
            raise ValueError(f"unexpected narrative shape: {json.dumps(parsed)[:200]!r}")
        return analysis.strip()
    except Exception as exc:
        logger.warning("report_narrative_failed", movie_title=movie_title, error=str(exc))
        return None
