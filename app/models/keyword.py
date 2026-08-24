from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPrimaryKeyMixin, utcnow
from app.models.enums import Platform

if TYPE_CHECKING:
    from app.models.movie import Movie


class Keyword(UUIDPrimaryKeyMixin, Base):
    """A search term tracked for one movie on one platform. Supports two
    match modes at the app layer (see app.services.keyword_match, ported
    from cinemark-scraper's src/lib/keyword-match.ts): phrase match as
    written, or AND mode ('+'-separated, all parts must appear)."""

    __tablename__ = "keywords"
    __table_args__ = (
        UniqueConstraint("movie_id", "platform", "keyword", name="keywords_movie_platform_keyword_unique"),
        Index("keywords_platform_idx", "platform"),
    )

    movie_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("movies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    platform: Mapped[Platform] = mapped_column(String, nullable=False)
    keyword: Mapped[str] = mapped_column(String, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    movie: Mapped["Movie"] = relationship(back_populates="keywords")
