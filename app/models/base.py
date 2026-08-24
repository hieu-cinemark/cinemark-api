"""Shared declarative base + mixins every model in app/models/ builds on."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class UUIDPrimaryKeyMixin:
    """Every table uses an app-generated UUIDv4 as its primary key rather
    than an auto-increment int - lets application code create and reference
    a row's id before it's committed (e.g. building related rows in the
    same batch), and doesn't leak row counts through sequential ids."""

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
