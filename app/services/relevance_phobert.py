"""Per-post relevance classification at ingest time - replaces the old
Kira-based check (app/kira/relevance.py, now unused) with the same
already-trained local PhoBERT model scripts/label_posts_relevance.py
(in the sibling phobert-classifier repo) uses for the batch backfill, via
its serve.py's /relevance endpoint. One model, one decision boundary,
whether a post got classified live at ingest or later in a batch sweep.

Same 3-bucket scheme as the batch script: "related"/"not_related" are the
model's raw call when confident (>= CONFIDENCE_THRESHOLD), "uncertain"
otherwise - low confidence must never be treated as a confident
"not_related" verdict, since that bucket is what tells the caller it's
safe to drop the post outright (see app/workers/ingest_consumer/main.py).

Fail-open like every other classifier in this codebase: returns None if
the PhoBERT service is unreachable/misconfigured, so an outage never
blocks or drops ingestion - the caller treats None the same as
"uncertain" (keep the post, don't claim a relevance verdict for it)."""

from __future__ import annotations

import httpx

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Same threshold as phobert-classifier/label_posts_relevance.py's own
# CONFIDENCE_THRESHOLD - keep these in sync; they're classifying with the
# exact same model and should draw the "uncertain" line in the same place.
CONFIDENCE_THRESHOLD = 0.65

VALID_LABELS = {"related", "not_related"}


async def classify_post_relevance(
    content: str | None, movie_title: str | None, keyword: str | None
) -> dict[str, object] | None:
    """Returns {"label": "related"|"not_related"|"uncertain", "confidence":
    float}, or None if the PhoBERT service is unreachable/misconfigured."""
    base = (settings.phobert_url or "").rstrip("/")
    if not base or not content:
        return None
    try:
        async with httpx.AsyncClient(timeout=settings.phobert_timeout_seconds) as client:
            resp = await client.post(
                f"{base}/relevance",
                json={"text": content, "movie_title": movie_title or "", "keyword": keyword or ""},
            )
        if resp.status_code >= 400:
            logger.warning("phobert_relevance_http_error", status=resp.status_code, body=resp.text[:200])
            return None
        data = resp.json()
        label = data.get("label") if isinstance(data, dict) else None
        confidence = data.get("confidence") if isinstance(data, dict) else None
        if label not in VALID_LABELS or not isinstance(confidence, (int, float)):
            logger.warning("phobert_relevance_bad_payload", payload=data)
            return None
        if confidence < CONFIDENCE_THRESHOLD:
            label = "uncertain"
        return {"label": label, "confidence": float(confidence)}
    except Exception as exc:
        logger.warning("phobert_relevance_failed", error=str(exc))
        return None
