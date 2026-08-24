"""Plain Python enums for platform/status fields - stored as VARCHAR, not a
Postgres native ENUM type. A native ENUM makes adding a new platform later
an ALTER TYPE migration (and Alembic's autogenerate doesn't handle enum
value changes well); a string column validated at the app layer (here +
Pydantic schemas) doesn't have that friction. cinemark-scraper's Drizzle
schema made the same tradeoff (text column + a TS union type)."""

from __future__ import annotations

from enum import StrEnum


class Platform(StrEnum):
    FACEBOOK = "facebook"
    TIKTOK = "tiktok"
    THREADS = "threads"
    INSTAGRAM = "instagram"


class RunTrigger(StrEnum):
    CRON = "cron"
    MANUAL = "manual"


class RunStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunItemStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
