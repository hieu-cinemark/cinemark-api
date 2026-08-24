"""Upsert logic for scraped posts, ported from cinemark-scraper's
src/jobs/persist-post.ts: new post -> insert + engagement snapshot;
existing post -> update, only append a new snapshot if an engagement count
actually changed since the last known values (not on every scrape).

Media archival is *not* done here - see app/workers/ingest_consumer/main.py,
which awaits this function only for the DB write, then fires the R2
download+upload off as a bounded-concurrency background task. Doing it
inline here used to block each message's whole processing on two sequential
network round trips (download from Facebook's CDN, upload to R2), which was
the dominant cost of ingestion - far slower than spider-hub's local JSON
feed export, which does no network I/O at all."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repository import BaseRepository
from app.models.engagement import PostEngagementSnapshot
from app.models.post import Post

ENGAGEMENT_FIELDS = ("like_count", "reply_count", "repost_count", "quote_count", "reshare_count", "view_count")


class PostRepository(BaseRepository[Post]):
    model = Post

    async def get_by_external_id(self, *, platform: str, external_id: str) -> Post | None:
        result = await self.session.execute(
            select(Post).where(Post.platform == platform, Post.external_id == external_id)
        )
        return result.scalar_one_or_none()


def _map_facebook_post(payload: dict[str, Any]) -> dict[str, Any]:
    """spider-hub's FacebookPostItem field names -> Post model column names.
    Same semantic mapping cinemark-scraper's mapPost() used (reactions=likes,
    comments=replies, shares=reposts) - Facebook has no separate quote/reshare
    concept the way Threads does, so those stay 0."""
    timestamp = payload.get("timestamp")
    return {
        "platform": payload["platform"],
        "external_id": payload["post_id"],
        "url": payload.get("url"),
        "author": payload.get("author_name"),
        "content": payload.get("message"),
        "like_count": payload.get("reactions_count") or 0,
        "reply_count": payload.get("comments_count") or 0,
        "repost_count": payload.get("shares_count") or 0,
        "quote_count": 0,
        "reshare_count": 0,
        "view_count": 0,
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp else None,
        "media": {
            "media_type": payload.get("media_type"),
            "media_url": payload.get("media_url"),
            "duration_seconds": payload.get("duration_seconds"),
        },
        "raw": payload,
    }


async def persist_post(
    session: AsyncSession, *, movie_id: uuid.UUID, keyword_id: uuid.UUID | None, payload: dict[str, Any]
) -> tuple[Post, str | None]:
    """Returns (post, media_url) - media_url is the CDN URL the caller
    should archive in the background, or None if there's nothing to archive
    (no media on this post, or it was already archived on a previous
    ingest of the same post_id - a re-scrape's media dict never carries an
    r2_key, so without this check every re-scrape would re-download and
    re-upload the same image)."""
    repo = PostRepository(session)
    data = _map_facebook_post(payload)
    engagement = {field: data[field] for field in ENGAGEMENT_FIELDS}
    media_url = data["media"].get("media_url")

    existing = await repo.get_by_external_id(platform=data["platform"], external_id=data["external_id"])

    if existing is None:
        post = await repo.create(movie_id=movie_id, keyword_id=keyword_id, **data)
        session.add(PostEngagementSnapshot(post=post, **engagement))
        return post, media_url

    already_archived = bool((existing.media or {}).get("r2_key"))
    if already_archived:
        data["media"]["r2_key"] = existing.media["r2_key"]

    changed = any(getattr(existing, field) != engagement[field] for field in ENGAGEMENT_FIELDS)
    update_fields = {k: v for k, v in data.items() if k not in ("platform", "external_id")}
    await repo.update(existing, **update_fields)
    if changed:
        session.add(PostEngagementSnapshot(post=existing, **engagement))
    return existing, (None if already_archived else media_url)
