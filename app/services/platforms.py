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


def _map_threads_post(payload: dict[str, Any]) -> PostDraft:
    """spider-hub's ThreadsPostItem field names -> PostDraft. Unlike
    Facebook, Threads has real repost/quote counts of its own (not
    collapsed into a single "shares" figure), so those map directly instead
    of defaulting to 0."""
    timestamp = payload.get("timestamp")
    return {
        "external_id": payload.get("post_id"),
        "url": payload.get("url"),
        "author": payload.get("author_username") or payload.get("author_name"),
        "content": payload.get("message"),
        "media": {
            "media_type": payload.get("media_type"),
            "media_url": payload.get("media_url"),
        },
        "like_count": payload.get("like_count") or 0,
        "reply_count": payload.get("reply_count") or 0,
        "repost_count": payload.get("repost_count") or 0,
        "quote_count": payload.get("quote_count") or 0,
        "reshare_count": 0,
        "view_count": 0,
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


def _map_tiktok_post(payload: dict[str, Any]) -> PostDraft:
    """spider-hub's TikTokVideoItem field names -> PostDraft. Unlike
    Facebook/Threads, TikTok exposes a real play/view count directly (not
    collapsed into likes the way Facebook's "reactions" is), so view_count
    maps to it instead of defaulting to 0. TikTok has no repost/quote
    concept distinct from its own share count, so repost_count takes that
    and quote_count/reshare_count stay 0, same rationale as
    _map_facebook_post's shares -> repost_count."""
    timestamp = payload.get("create_time")
    return {
        "external_id": payload.get("video_id"),
        "url": payload.get("url"),
        "author": payload.get("author_username") or payload.get("author_name"),
        "content": payload.get("desc"),
        "media": {
            "media_type": "video",
            "media_url": payload.get("play_url"),
            "duration_seconds": payload.get("duration"),
        },
        "like_count": payload.get("like_count") or 0,
        "reply_count": payload.get("comment_count") or 0,
        "repost_count": payload.get("share_count") or 0,
        "quote_count": 0,
        "reshare_count": 0,
        "view_count": payload.get("play_count") or 0,
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


PLATFORM_POST_MAPPERS: dict[str, Callable[[dict[str, Any]], PostDraft]] = {
    "facebook": _map_facebook_post,
    "threads": _map_threads_post,
    "tiktok": _map_tiktok_post,
}


def get_post_mapper(platform: str) -> Callable[[dict[str, Any]], PostDraft] | None:
    return PLATFORM_POST_MAPPERS.get(platform)


def registered_platforms() -> set[str]:
    return set(PLATFORM_POST_MAPPERS)


# The normalized shape every comment mapper below must produce - mirrors
# cinemark-scraper's comments table (src/db/schema.ts) the same way
# PostDraft mirrors posts.
CommentDraft = dict[str, Any]


def _map_facebook_comment(payload: dict[str, Any]) -> CommentDraft:
    """spider-hub's FacebookCommentItem field names -> CommentDraft (see
    social_crawler/spiders/facebook/items.py there)."""
    timestamp = payload.get("timestamp")
    return {
        "external_id": payload.get("comment_id"),
        "message": payload.get("message"),
        "author_name": payload.get("author_name"),
        "author_id": payload.get("author_id"),
        "author_url": payload.get("author_url"),
        "author_profile_picture": payload.get("author_profile_picture"),
        "reactions_count": payload.get("reactions_count") or 0,
        "replies_count": payload.get("replies_count") or 0,
        "parent_external_id": payload.get("parent_comment_id"),
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


def _map_threads_comment(payload: dict[str, Any]) -> CommentDraft:
    """spider-hub's ThreadsCommentItem field names -> CommentDraft (see
    social_crawler/spiders/threads/items.py there). Threads has no
    separate reactions/replies split the way Facebook's payload names
    them - "like_count"/"reply_count" map directly to the same
    reactions_count/replies_count columns. author_url isn't a field
    spider-hub captures for Threads (only username/name/id/avatar), so
    it's built here from the username the same way ThreadsPostItem's own
    author_url is constructed."""
    timestamp = payload.get("timestamp")
    username = payload.get("author_username")
    return {
        "external_id": payload.get("reply_id"),
        "message": payload.get("message"),
        "author_name": payload.get("author_name") or username,
        "author_id": payload.get("author_id"),
        "author_url": f"https://www.threads.com/@{username}" if username else None,
        "author_profile_picture": payload.get("author_profile_picture"),
        "reactions_count": payload.get("like_count") or 0,
        "replies_count": payload.get("reply_count") or 0,
        "parent_external_id": payload.get("parent_reply_id"),
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


def _map_tiktok_comment(payload: dict[str, Any]) -> CommentDraft:
    """spider-hub's TikTokCommentItem field names -> CommentDraft (see
    social_crawler/spiders/tiktok/items.py there)."""
    timestamp = payload.get("timestamp")
    username = payload.get("author_username")
    return {
        "external_id": payload.get("comment_id"),
        "message": payload.get("message"),
        "author_name": payload.get("author_name") or username,
        "author_id": payload.get("author_id"),
        "author_url": f"https://www.tiktok.com/@{username}" if username else None,
        "author_profile_picture": payload.get("author_avatar_url"),
        "reactions_count": payload.get("like_count") or 0,
        "replies_count": payload.get("reply_count") or 0,
        "parent_external_id": payload.get("parent_comment_id"),
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


PLATFORM_COMMENT_MAPPERS: dict[str, Callable[[dict[str, Any]], CommentDraft]] = {
    "facebook": _map_facebook_comment,
    "threads": _map_threads_comment,
    "tiktok": _map_tiktok_comment,
}


def get_comment_mapper(platform: str) -> Callable[[dict[str, Any]], CommentDraft] | None:
    return PLATFORM_COMMENT_MAPPERS.get(platform)
