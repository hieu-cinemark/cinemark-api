"""Registry of social platforms this service knows how to trigger crawls
for and ingest posts from - the same registry pattern spider-hub's
SPIDER_BY_PLATFORM (crawl_request_consumer.py) and cinemark-scraper's
scrapers/registry.ts already use, kept in sync by convention rather than
by any shared code across the three services.

Adding a new platform here means:
1. Add its mapper below (raw Kafka payload -> PostDraft).
2. Make sure spider-hub's crawl_request_consumer.py has a matching
   SPIDER_BY_PLATFORM entry, or triggered crawls for it will silently
   vanish (see app/services/d1.py's get_keyword docstring).
3. Add its router (app/api/routes/<platform>.py, same shape as facebook.py)
   and register it in app/main.py.
Nothing else in this service - app/services/d1.py, app/workers/ingest_consumer,
app/api/routes/platform_scraper.py - needs to change."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

# The normalized shape every platform mapper below must produce - same
# fields as cinemark-scraper's own PostDraft type (src/scrapers/types.ts),
# so app/services/d1.py's persist_post() has no platform-specific field
# knowledge of its own. `media`/`raw` are dicts here, JSON-encoded only at
# the point of writing to D1.
PostDraft = dict[str, Any]


def _map_facebook_post(payload: dict[str, Any]) -> PostDraft:
    """spider-hub's FacebookPostItem field names -> PostDraft. Same mapping
    cinemark-scraper's own facebook.ts scraper uses (reactions = likes,
    comments = replies, shares = reposts) - Facebook has no separate
    quote/reshare concept the way Threads does, so those stay 0."""
    timestamp = payload.get("timestamp")
    return {
        "external_id": payload.get("post_id"),
        "url": payload.get("url"),
        "author": payload.get("author_name"),
        "content": payload.get("message"),
        "media": {
            "media_type": payload.get("media_type"),
            "media_url": payload.get("media_url"),
            "duration_seconds": payload.get("duration_seconds"),
        },
        "like_count": payload.get("reactions_count") or 0,
        "reply_count": payload.get("comments_count") or 0,
        "repost_count": payload.get("shares_count") or 0,
        "quote_count": 0,
        "reshare_count": 0,
        "view_count": 0,
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


PLATFORM_POST_MAPPERS: dict[str, Callable[[dict[str, Any]], PostDraft]] = {
    "facebook": _map_facebook_post,
}


def get_post_mapper(platform: str) -> Callable[[dict[str, Any]], PostDraft] | None:
    return PLATFORM_POST_MAPPERS.get(platform)


def registered_platforms() -> set[str]:
    return set(PLATFORM_POST_MAPPERS)
