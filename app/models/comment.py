from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPrimaryKeyMixin, utcnow
from app.models.enums import Platform

if TYPE_CHECKING:
    from app.models.post import Post


class Comment(UUIDPrimaryKeyMixin, Base):
    """A scraped comment on a post, upserted by (platform, external_id) -
    same pattern as Post. Not present in cinemark-scraper's original schema
    (it only ever stored a comment *count* on posts.reply_count) - added
    because spider-hub's facebook_comments spider fetches real comment
    content (see FacebookCommentItem), which had nowhere to land before."""

    __tablename__ = "comments"
    __table_args__ = (
        UniqueConstraint("platform", "external_id", name="comments_platform_external_id_unique"),
        Index("comments_post_id_idx", "post_id"),
    )

    post_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("posts.id", ondelete="CASCADE"), nullable=False
    )
    platform: Mapped[Platform] = mapped_column(String, nullable=False)
    external_id: Mapped[str] = mapped_column(String, nullable=False)
    # Facebook's older, deprecated comment id format - kept only for
    # reference/debugging, never used to key anything.
    legacy_external_id: Mapped[str | None] = mapped_column(String, nullable=True)

    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    author_name: Mapped[str | None] = mapped_column(String, nullable=True)
    author_id: Mapped[str | None] = mapped_column(String, nullable=True)
    author_url: Mapped[str | None] = mapped_column(String, nullable=True)
    author_gender: Mapped[str | None] = mapped_column(String, nullable=True)
    author_profile_picture: Mapped[str | None] = mapped_column(String, nullable=True)

    reply_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reaction_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    is_sticker: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sticker_url: Mapped[str | None] = mapped_column(String, nullable=True)
    is_gif: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    gif_url: Mapped[str | None] = mapped_column(String, nullable=True)
    image_url: Mapped[str | None] = mapped_column(String, nullable=True)
    video_url: Mapped[str | None] = mapped_column(String, nullable=True)

    # {"sticker": "<r2 key>", "gif": "<r2 key>", ...} for whichever of the
    # *_url fields above got archived to R2 (see app/services/storage.py) -
    # only the fields that were actually present and archived successfully
    # appear here, so this can be partial or entirely absent (None).
    media_archive: Mapped[Any | None] = mapped_column(JSONB, nullable=True)

    post: Mapped["Post"] = relationship(back_populates="comments")
