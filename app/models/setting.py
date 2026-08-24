from __future__ import annotations

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Setting(Base):
    """Plain key-value config store (scraper_enabled, cron_enabled,
    threads_max_pages, ...) - values are always stored as strings, parsed
    by whichever caller reads them (matches cinemark-scraper's settings
    table exactly, no schema change needed here)."""

    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[str] = mapped_column(String, nullable=False)
