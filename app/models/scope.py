from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPrimaryKeyMixin, utcnow


class Scope(UUIDPrimaryKeyMixin, Base):
    """A role a User can be assigned (admin, member, ...) - a real table
    rather than a StrEnum column (the pattern the rest of this codebase
    uses for fixed sets like Platform/RunStatus) because scopes are meant
    to be administered as data, not redeployed as code every time one is
    added or its label changes."""

    __tablename__ = "scopes"

    key: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    label: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
