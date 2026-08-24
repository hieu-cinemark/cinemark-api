"""Upsert logic for scraped comments - same (platform, external_id) upsert
shape as posts.py, but simpler: no engagement-snapshot history for
comments (only posts get trend charts), so this is a plain upsert.

Media archival is *not* done here - same reasoning as posts.py: the caller
(app/workers/ingest_consumer/main.py) awaits this only for the DB write,
then archives whichever media URLs are new in a background task instead of
blocking this message's processing on N sequential download+upload round
trips (a comment can carry up to 4 media fields at once)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repository import BaseRepository
from app.models.comment import Comment
from app.models.post import Post

# Comment field -> media_archive dict key, and which of these actually carry
# a URL worth archiving (is_sticker/is_gif are just booleans, not URLs).
MEDIA_URL_FIELDS = {"sticker": "sticker_url", "gif": "gif_url", "image": "image_url", "video": "video_url"}


class CommentRepository(BaseRepository[Comment]):
    model = Comment

    async def get_by_external_id(self, *, platform: str, external_id: str) -> Comment | None:
        result = await self.session.execute(
            select(Comment).where(Comment.platform == platform, Comment.external_id == external_id)
        )
        return result.scalar_one_or_none()


def _map_facebook_comment(payload: dict[str, Any]) -> dict[str, Any]:
    """spider-hub's FacebookCommentItem field names -> Comment model column
    names (gif/image/video renamed *_url for naming consistency with
    sticker_url, which spider-hub already names that way)."""
    timestamp = payload.get("timestamp")
    return {
        "platform": payload["platform"],
        "external_id": payload["comment_id"],
        "legacy_external_id": payload.get("legacy_comment_id"),
        "content": payload.get("message"),
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp else None,
        "author_name": payload.get("author_name"),
        "author_id": payload.get("author_id"),
        "author_url": payload.get("author_url"),
        "author_gender": payload.get("author_gender"),
        "author_profile_picture": payload.get("author_profile_picture"),
        "reply_count": payload.get("replies_count") or 0,
        "reaction_count": payload.get("reactions_count") or 0,
        "is_sticker": bool(payload.get("is_sticker")),
        "sticker_url": payload.get("sticker_url"),
        "is_gif": bool(payload.get("is_gif")),
        "gif_url": payload.get("gif"),
        "image_url": payload.get("image"),
        "video_url": payload.get("video"),
    }


async def persist_comment(session: AsyncSession, *, post: Post, payload: dict[str, Any]) -> tuple[Comment, dict[str, str]]:
    """Returns (comment, media_to_archive) - media_to_archive maps
    {archive_key: source_url} for whichever of sticker/gif/image/video are
    present on this comment and not already archived (a re-scrape of the
    same comment_id never re-archives what's already in media_archive)."""
    repo = CommentRepository(session)
    data = _map_facebook_comment(payload)

    existing = await repo.get_by_external_id(platform=data["platform"], external_id=data["external_id"])
    existing_archive = (existing.media_archive if existing else None) or {}

    media_to_archive = {
        name: data[field]
        for name, field in MEDIA_URL_FIELDS.items()
        if data.get(field) and name not in existing_archive
    }
    data["media_archive"] = existing_archive or None

    if existing is None:
        comment = await repo.create(post_id=post.id, **data)
    else:
        update_fields = {k: v for k, v in data.items() if k not in ("platform", "external_id")}
        comment = await repo.update(existing, **update_fields)

    return comment, media_to_archive
