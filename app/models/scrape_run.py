from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPrimaryKeyMixin, utcnow
from app.models.enums import Platform, RunItemStatus, RunStatus, RunTrigger

if TYPE_CHECKING:
    from app.models.keyword import Keyword
    from app.models.movie import Movie


class ScrapeRun(UUIDPrimaryKeyMixin, Base):
    """Top-level record for one scrape job invocation (one cron tick, or one
    manual trigger) - covers every keyword/platform combination it touched,
    each tracked individually in ScrapeRunItem."""

    __tablename__ = "scrape_runs"

    status: Mapped[RunStatus] = mapped_column(String, nullable=False)
    triggered_by: Mapped[RunTrigger] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    items: Mapped[list["ScrapeRunItem"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    request_logs: Mapped[list["ScrapeRequestLog"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    log_lines: Mapped[list["ScrapeRunLog"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="ScrapeRunLog.created_at"
    )


class ScrapeRunItem(UUIDPrimaryKeyMixin, Base):
    """One (movie, platform, keyword) result within a ScrapeRun."""

    __tablename__ = "scrape_run_items"
    __table_args__ = (
        Index("scrape_run_items_run_id_idx", "run_id"),
        Index("scrape_run_items_movie_id_idx", "movie_id"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("scrape_runs.id", ondelete="CASCADE"), nullable=False
    )
    movie_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("movies.id", ondelete="CASCADE"), nullable=False
    )
    keyword_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("keywords.id", ondelete="SET NULL"), nullable=True
    )
    platform: Mapped[Platform] = mapped_column(String, nullable=False)
    posts_found: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    keyword_matches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[RunItemStatus] = mapped_column(String, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped["ScrapeRun"] = relationship(back_populates="items")
    movie: Mapped["Movie"] = relationship()
    keyword: Mapped["Keyword | None"] = relationship()
    request_logs: Mapped[list["ScrapeRequestLog"]] = relationship(back_populates="run_item")


class ScrapeRequestLog(UUIDPrimaryKeyMixin, Base):
    """One API page fetched during a run - the finest-grained audit trail
    (cursor, how many results came back, how many were new/keyword-matched).
    run_item_id/movie_id/keyword_id are nullable because a request can be
    logged before a ScrapeRunItem exists for it, or for ad-hoc/debug fetches
    not tied to a movie at all."""

    __tablename__ = "scrape_request_logs"
    __table_args__ = (
        Index("scrape_request_logs_run_id_idx", "run_id"),
        Index("scrape_request_logs_run_item_id_idx", "run_item_id"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("scrape_runs.id", ondelete="CASCADE"), nullable=False
    )
    run_item_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("scrape_run_items.id", ondelete="CASCADE"), nullable=True
    )
    movie_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("movies.id", ondelete="SET NULL"), nullable=True
    )
    keyword_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("keywords.id", ondelete="SET NULL"), nullable=True
    )
    platform: Mapped[Platform] = mapped_column(String, nullable=False)
    keyword: Mapped[str] = mapped_column(String, nullable=False)
    sort: Mapped[str] = mapped_column(String, nullable=False)
    strategy: Mapped[str] = mapped_column(String, nullable=False, default="")
    page_index: Mapped[int] = mapped_column(Integer, nullable=False)
    cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    results_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    new_posts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    keyword_matches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    run: Mapped["ScrapeRun"] = relationship(back_populates="request_logs")
    run_item: Mapped["ScrapeRunItem | None"] = relationship(back_populates="request_logs")
    movie: Mapped["Movie | None"] = relationship()
    # No `keyword` relationship here (only keyword_id) - this table already
    # has a `keyword` TEXT column (the literal search text at request time,
    # denormalized on purpose so the log stays readable even after the
    # Keyword row is edited/deleted) - a relationship of the same name would
    # collide with that column.


class ScrapeRunLog(UUIDPrimaryKeyMixin, Base):
    """One raw stdout/stderr line captured from the `scrapy crawl` subprocess
    spider-hub's crawl_request_consumer.py runs for this ScrapeRun - written
    directly by spider-hub (it already has DATABASE_URL - see
    social_crawler/services/db_settings.py) as each line arrives, so
    cinemark-web's Platforms page can poll GET /scraper/runs/{id}/logs and
    show something close to a live tail without any new Kafka topic or
    consumer. Deliberately just opaque text, unlike ScrapeRequestLog's
    structured per-page fields - this is a progress feed for a human
    watching the run, not an audit trail queried for numbers."""

    __tablename__ = "scrape_run_logs"
    __table_args__ = (Index("scrape_run_logs_run_id_created_at_idx", "run_id", "created_at"),)

    run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("scrape_runs.id", ondelete="CASCADE"), nullable=False
    )
    line: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    run: Mapped["ScrapeRun"] = relationship(back_populates="log_lines")
