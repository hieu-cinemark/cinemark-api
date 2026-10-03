"""LLM calls behind scripts/generate_social_topic_reports.py - same
two-call split and same prompts either provider runs (see
app/ai/prompts/report.py's own module docstring for why split into two
calls). Both are fail-open (return None on any error), same convention as
every classifier in this codebase - the caller decides what "no report
this run" means.

Reports are Beeknoee's only job (every classifier runs on Kira): Bee
writes, and when a Bee call fails or returns something unusable (an
error, a truncated or wrong-shaped JSON) the same prompt goes to Kira
instead. The dashboard's AI settings tab can still point reports straight
at Kira (app/ai/kira.py's active_report_provider(), default "bee").
call_kira's force=True bypasses the ingest-classifiers on/off switch, same
as app/ai/tasks/import_parser.py's operator-triggered calls - a report run
is its own explicit request."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, TypeVar

from app.ai.bee import call_bee
from app.ai.kira import active_report_provider, call_kira, parse_json_response
from app.ai.prompts.report import (
    NARRATIVE_DATA_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    TOPICS_DATA_PROMPT,
    TOPICS_SYSTEM_PROMPT,
)
from app.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


async def _call_provider(provider: str, *, task: str, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float) -> str:
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


async def _generate(
    *, task: str, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float, parse: Callable[[str], T]
) -> tuple[T, str]:
    """Bee first, Kira as the fallback (just Kira when the dashboard points
    reports at it). `parse` validates the response and raises when it's
    unusable, so a truncated/wrong-shaped answer falls back too, not only a
    failed call. Returns (parsed result, provider that produced it); raises
    the last error when every provider failed."""
    providers = ["kira"] if await active_report_provider() == "kira" else ["bee", "kira"]
    last_error: Exception | None = None
    for provider in providers:
        try:
            response = await _call_provider(
                provider,
                task=task,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return parse(response), provider
        except Exception as exc:
            last_error = exc
            logger.warning("report_provider_failed", task=task, provider=provider, error=str(exc))
    assert last_error is not None
    raise last_error


def _parse_topics(response: str) -> dict[str, Any]:
    parsed = parse_json_response(response)
    if not isinstance(parsed, dict) or "top_10_topics" not in parsed or "top_10_verbatims" not in parsed:
        raise ValueError(f"unexpected topics shape: {json.dumps(parsed)[:200]!r}")
    return parsed


def _parse_narrative(response: str) -> str:
    parsed = parse_json_response(response)
    analysis = parsed.get("analysis") if isinstance(parsed, dict) else None
    if not isinstance(analysis, str) or not analysis.strip():
        raise ValueError(f"unexpected narrative shape: {json.dumps(parsed)[:200]!r}")
    return analysis.strip()


async def generate_topics_and_verbatims(movie_title: str, comments: list[dict[str, Any]]) -> dict[str, Any] | None:
    """`comments` is get_comment_sample_for_movie()'s rows (id/message/
    reactions_count/sentiment). Returns {"top_10_topics": [...],
    "top_10_verbatims": [...]} matching the frontend's ReportData shape plus
    "provider" ("bee" or "kira", whichever wrote it), or None when both
    failed."""
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
        parsed, provider = await _generate(
            task="topics",
            system_prompt=TOPICS_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=45000,
            temperature=0.3,
            parse=_parse_topics,
        )
        return {**parsed, "provider": provider}
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
        analysis, _provider = await _generate(
            task="narrative",
            system_prompt=NARRATIVE_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=4000,
            temperature=0.3,
            parse=_parse_narrative,
        )
        return analysis
    except Exception as exc:
        logger.warning("report_narrative_failed", movie_title=movie_title, error=str(exc))
        return None
