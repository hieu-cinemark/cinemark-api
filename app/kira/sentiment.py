"""Comment sentiment classification - Beeknoee (see app/bee/client.py).

Batched: one Bee call classifies up to BATCH_SIZE comments. Per comment,
Bee's fixed cost (system prompt, ~14s latency, a slot in its 2-call
concurrency budget) dwarfs the comment itself, so classifying comments one
call each could not keep up with a busy crawl day (~22k comments).

Called from the ingest consumer's background sweep
(app/workers/ingest_consumer/sentiment_sweep.py) and
scripts/backfill_comment_sentiment.py - never inline per comment.
(Post relevance is Kira's job - app/kira/post_relevance.py.)

Fail-open: a failed call leaves that batch's results None, so the comment
simply stays unclassified until the next sweep.
"""

from __future__ import annotations

from app.bee.client import bee_is_configured, call_bee, parse_json_response
from app.core.logging import get_logger
from app.kira.sentiment_prompt import SENTIMENT_DATA_PROMPT, SENTIMENT_SYSTEM_PROMPT
from app.services.d1 import MIN_CONTENT_LENGTH

logger = get_logger(__name__)

VALID_SENTIMENTS = {"positive", "negative", "neutral"}
BATCH_SIZE = 25
MAX_MESSAGE_CHARS = 600
MAX_TOKENS = 6000


def _classifiable(message: str | None) -> bool:
    return bool(message) and len(message.strip()) >= MIN_CONTENT_LENGTH


def _user_prompt(messages: list[str]) -> str:
    lines = [f"{i}. {' '.join(m.split())[:MAX_MESSAGE_CHARS]}" for i, m in enumerate(messages, 1)]
    return SENTIMENT_DATA_PROMPT.format(count=len(messages), comments="\n".join(lines))


async def _classify_batch(messages: list[str]) -> list[str | None]:
    try:
        response = await call_bee(
            task="sentiment",
            system_prompt=SENTIMENT_SYSTEM_PROMPT,
            user_prompt=_user_prompt(messages),
            # Generous: the Bee model spends hidden reasoning tokens before the
            # JSON (a 25-comment batch hit 1,700 with ~250 tokens of output).
            max_tokens=MAX_TOKENS,
        )
        parsed = parse_json_response(response)
        results = parsed.get("results") if isinstance(parsed, dict) else None
        if not isinstance(results, list):
            raise TypeError(f"unexpected sentiment shape: {str(parsed)[:200]}")
    except Exception as exc:  # noqa: BLE001 - fail open, the comments stay unclassified
        logger.warning("bee_sentiment_failed", error=exc, batch_size=len(messages))
        return [None] * len(messages)

    labels: list[str | None] = [None] * len(messages)
    for entry in results:
        if not isinstance(entry, dict):
            continue
        index, sentiment = entry.get("i"), entry.get("sentiment")
        if isinstance(index, int) and 1 <= index <= len(messages) and sentiment in VALID_SENTIMENTS:
            labels[index - 1] = sentiment
    missing = labels.count(None)
    if missing:
        logger.warning("bee_sentiment_incomplete", missing=missing, batch_size=len(messages))
    return labels


async def classify_sentiments(messages: list[str | None]) -> list[str | None]:
    """Same length/order as `messages`: "positive"/"negative"/"neutral", or
    None for a message too short to classify, when Bee isn't configured, or
    when its batch's call failed."""
    labels: list[str | None] = [None] * len(messages)
    todo = [(i, m.strip()) for i, m in enumerate(messages) if m is not None and _classifiable(m)]
    if not todo or not await bee_is_configured():
        return labels
    for start in range(0, len(todo), BATCH_SIZE):
        chunk = todo[start : start + BATCH_SIZE]
        for (i, _), label in zip(chunk, await _classify_batch([m for _, m in chunk]), strict=True):
            labels[i] = label
    return labels


async def classify_sentiment(message: str | None) -> str | None:
    """Single-comment convenience wrapper around classify_sentiments()."""
    return (await classify_sentiments([message]))[0]
