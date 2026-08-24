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
    from app.models.comment import Comment
    from app.models.engagement import PostEngagementSnapshot
    from app.models.movie import Movie


class Post(UUIDPrimaryKeyMixin, Base):
    """A scraped post, upserted by (platform, external_id). Engagement
    counts here always hold the latest known values - see
    PostEngagementSnapshot for the timestamped history used for trend
    charts (mirrors cinemark-scraper's persist-post.ts upsert logic)."""

    __tablename__ = "posts"
    __table_args__ = (
        UniqueConstraint("platform", "external_id", name="posts_platform_external_id_unique"),
        Index("posts_movie_id_idx", "movie_id"),
        Index("posts_keyword_id_idx", "keyword_id"),
        Index("posts_posted_at_idx", "posted_at"),
        Index("posts_like_count_idx", "like_count"),
    )

    movie_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("movies.id", ondelete="CASCADE"), nullable=False
    )
    keyword_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("keywords.id", ondelete="SET NULL"), nullable=True
    )
    platform: Mapped[Platform] = mapped_column(String, nullable=False)
    external_id: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str | None] = mapped_column(String, nullable=True)
    author: Mapped[str | None] = mapped_column(String, nullable=True)
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    # JSONB (not text-encoded JSON like the old SQLite schema) - queryable
    # and indexable directly, no manual json.loads() on every read.
    media: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    raw: Mapped[Any | None] = mapped_column(JSONB, nullable=True)

    like_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reply_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    repost_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quote_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reshare_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    view_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    posted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    keyword_match: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    movie: Mapped["Movie"] = relationship(back_populates="posts")
    engagement_snapshots: Mapped[list["PostEngagementSnapshot"]] = relationship(
        back_populates="post", cascade="all, delete-orphan"
    )
    comments: Mapped[list["Comment"]] = relationship(back_populates="post", cascade="all, delete-orphan")
