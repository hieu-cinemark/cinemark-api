from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPrimaryKeyMixin, utcnow

if TYPE_CHECKING:
    from app.models.scope import Scope


class User(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "users"

    username: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    email: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(String, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_superuser: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Bumped to invalidate every refresh token issued before the bump (e.g.
    # on password change) without needing a revocation table - a refresh
    # token embeds the version it was issued under (see create_refresh_token
    # / POST /auth/refresh) and is only honored while that still matches.
    token_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # The jti of the one refresh token currently considered valid for this
    # user. POST /auth/refresh rejects any refresh token whose jti doesn't
    # match this - not just an expiry check but reuse detection: presenting
    # an already-rotated-out refresh token (e.g. a stolen one replayed after
    # the legitimate client already refreshed past it) trips this mismatch,
    # which bumps token_version and clears this field, revoking every
    # refresh token issued so far rather than silently honoring the replay.
    current_refresh_jti: Mapped[str | None] = mapped_column(String, nullable=True)
    scope_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("scopes.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    scope: Mapped["Scope | None"] = relationship()
