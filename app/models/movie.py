from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Date, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPrimaryKeyMixin, utcnow

if TYPE_CHECKING:
    from app.models.keyword import Keyword
    from app.models.post import Post


class Movie(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "movies"

    title: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    released_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    keywords: Mapped[list["Keyword"]] = relationship(back_populates="movie", cascade="all, delete-orphan")
    posts: Mapped[list["Post"]] = relationship(back_populates="movie", cascade="all, delete-orphan")
