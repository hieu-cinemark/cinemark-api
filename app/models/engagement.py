from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Index, Integer
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPrimaryKeyMixin, utcnow

if TYPE_CHECKING:
    from app.models.post import Post


class PostEngagementSnapshot(UUIDPrimaryKeyMixin, Base):
    """Timestamped engagement sample, used for trend charts. Posts.* holds
    only the latest values - a new row here is only inserted when at least
    one engagement count actually changed since the last snapshot, not on
    every scrape (same rule as cinemark-scraper's persist-post.ts)."""

    __tablename__ = "post_engagement_snapshots"
    __table_args__ = (
        Index("post_engagement_snapshots_post_id_idx", "post_id"),
        Index("post_engagement_snapshots_recorded_at_idx", "recorded_at"),
    )

    post_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("posts.id", ondelete="CASCADE"), nullable=False
    )
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    like_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reply_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    repost_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quote_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reshare_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    view_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    post: Mapped["Post"] = relationship(back_populates="engagement_snapshots")
