"""Comment sentiment classification.

Primary path is the local PhoBERT HTTP service (phobert-classifier/serve.py).
Kira remains available as an optional fallback when SENTIMENT_BACKEND=kira
or auto+PhoBERT is down. Kira's main job in this product is topic/narrative
summaries (app/kira/report.py), not per-comment labels.

Called from app/workers/ingest_consumer/main.py at ingest time, and from
scripts/backfill_comment_sentiment.py for rows that predate classification.
Fail-open: returns None on any error so persistence is never blocked.
"""

from __future__ import annotations

import httpx

from app.core.config import settings
from app.core.logging import get_logger
from app.kira.client import call_kira, kira_is_enabled, parse_json_response
from app.kira.sentiment_prompt import SENTIMENT_DATA_PROMPT, SENTIMENT_SYSTEM_PROMPT
from app.services.d1 import MIN_CONTENT_LENGTH

logger = get_logger(__name__)

VALID_SENTIMENTS = {"positive", "negative", "neutral"}


async def _classify_phobert(message: str) -> str | None:
    base = (settings.phobert_url or "").rstrip("/")
    if not base:
        return None
    try:
        async with httpx.AsyncClient(timeout=settings.phobert_timeout_seconds) as client:
            resp = await client.post(f"{base}/predict", json={"text": message})
        if resp.status_code >= 400:
            logger.warning(
                "phobert_sentiment_http_error",
                status=resp.status_code,
                body=resp.text[:200],
            )
            return None
        data = resp.json()
        sentiment = data.get("label") if isinstance(data, dict) else None
        if sentiment not in VALID_SENTIMENTS:
            logger.warning("phobert_sentiment_bad_label", payload=data)
            return None
        return sentiment
    except Exception as exc:
        logger.warning("phobert_sentiment_failed", error=str(exc))
        return None


async def _classify_kira(message: str) -> str | None:
    if not await kira_is_enabled():
        return None
    prompt = SENTIMENT_DATA_PROMPT.format(message=message)
    try:
        response = await call_kira(
            task="sentiment",
            system_prompt=SENTIMENT_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=1000,
        )
        parsed = parse_json_response(response)
        sentiment = parsed.get("sentiment") if isinstance(parsed, dict) else None
        if sentiment not in VALID_SENTIMENTS:
            raise ValueError(f"unexpected sentiment shape: {parsed!r}")
        return sentiment
    except Exception as exc:
        logger.warning("kira_sentiment_failed", error=str(exc))
        return None


async def classify_sentiment(message: str | None) -> str | None:
    """Returns "positive"/"negative"/"neutral", or None if the message is
    too short / backend unavailable / call failed."""
    if not message or len(message.strip()) < MIN_CONTENT_LENGTH:
        return None

    text = message.strip()
    backend = (settings.sentiment_backend or "phobert").strip().lower()

    if backend == "phobert":
        return await _classify_phobert(text)

    if backend == "kira":
        return await _classify_kira(text)

    if backend == "auto":
        # Prefer PhoBERT; fall back to Kira only if local model is down.
        result = await _classify_phobert(text)
        if result is not None:
            return result
        return await _classify_kira(text)

    logger.warning("sentiment_unknown_backend", backend=backend)
    return None
