"""Bản D1 của nhóm bảng vận hành: filter_keywords, crawl_schedules, comment_crawl_schedules, cleanup_settings, cleanup_run_history,
discovery_settings, omar_api_settings. Cùng cách làm với d1_store.py (SQLAlchemy Core -> d1_exec database="config"), hàm trả đúng dạng
dữ liệu của bản Postgres (bool, dict, date, datetime). Bật bằng CONFIG_DB_BACKEND=d1."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from sqlalchemy import Column, Index, Integer, Table, Text, UniqueConstraint, delete, select, text, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateIndex, CreateTable

from app.core.errors import ConflictError, NotFoundError
from app.services.accounts.config_db.d1_store import _EMPTY, _EMPTY_JSON, _json, _metadata, _now, _run, _when

_ZERO = text("0")
_ONE = text("1")

filter_keywords = Table(
    "filter_keywords",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("keyword", Text, nullable=False),
    Column("category", Text, nullable=False),
    Column("enabled", Integer, nullable=False, server_default=_ONE),
    Column("created_at", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    UniqueConstraint("category", "keyword"),
)

crawl_schedules = Table(
    "crawl_schedules",
    _metadata,
    Column("platform", Text, primary_key=True),
    Column("run_time", Text, nullable=False, server_default=text("'07:00'")),
    Column("enabled", Integer, nullable=False, server_default=_ONE),
    Column("last_triggered_date", Text),
    Column("nurture_before", Integer, nullable=False, server_default=_ZERO),
    Column("nurture_after", Integer, nullable=False, server_default=_ZERO),
    Column("updated_at", Text, nullable=False),
)

comment_crawl_schedules = Table(
    "comment_crawl_schedules",
    _metadata,
    Column("platform", Text, primary_key=True),
    Column("run_time", Text, nullable=False, server_default=text("'08:00'")),
    Column("enabled", Integer, nullable=False, server_default=_ZERO),
    Column("top_n", Integer, nullable=False, server_default=text("15")),
    Column("last_triggered_date", Text),
    Column("updated_at", Text, nullable=False),
)

cleanup_settings = Table(
    "cleanup_settings",
    _metadata,
    Column("id", Integer, primary_key=True),
    Column("settings", Text, nullable=False, server_default=_EMPTY_JSON),
    Column("updated_at", Text, nullable=False),
)

cleanup_run_history = Table(
    "cleanup_run_history",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", Text, nullable=False),
    Column("finished_at", Text),
    Column("dry_run", Integer, nullable=False, server_default=_ZERO),
    Column("triggered_by", Text, nullable=False, server_default=text("'schedule'")),
    Column("posts_deleted", Integer, nullable=False, server_default=_ZERO),
    Column("comments_deleted", Integer, nullable=False, server_default=_ZERO),
    Column("snapshots_deleted", Integer, nullable=False, server_default=_ZERO),
    Column("batches", Integer, nullable=False, server_default=_ZERO),
    Column("remaining_posts", Integer),
    Column("error", Text),
    Index("cleanup_run_history_started_at_idx", "started_at"),
)

discovery_settings = Table(
    "discovery_settings",
    _metadata,
    Column("id", Integer, primary_key=True),
    Column("enabled", Integer, nullable=False, server_default=_ONE),
    Column("updated_at", Text, nullable=False),
)

omar_api_settings = Table(
    "omar_api_settings",
    _metadata,
    Column("id", Integer, primary_key=True),
    Column("api_key", Text, nullable=False, server_default=_EMPTY),
    Column("requests_per_minute", Integer, nullable=False, server_default=_ZERO),
    Column("daily_requests", Integer, nullable=False, server_default=_ZERO),
    Column("concurrency", Integer, nullable=False, server_default=_ZERO),
    Column("updated_at", Text, nullable=False),
)

_TABLES = (
    filter_keywords,
    crawl_schedules,
    comment_crawl_schedules,
    cleanup_settings,
    cleanup_run_history,
    discovery_settings,
    omar_api_settings,
)
_ready = False


async def ensure_tables() -> None:
    global _ready
    if _ready:
        return
    for table in _TABLES:
        await _run(CreateTable(table, if_not_exists=True))
    for index in cleanup_run_history.indexes:
        await _run(CreateIndex(index, if_not_exists=True))
    _ready = True


def reset_for_tests() -> None:
    global _ready
    _ready = False


def _day(value: Any) -> date | None:
    return date.fromisoformat(str(value)[:10]) if value else None


# --- filter_keywords -----------------------------------------------------------------------------------------------

_FK = filter_keywords.c


def _keyword_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "enabled": bool(row["enabled"]),
        "created_at": _when(row["created_at"]),
        "updated_at": _when(row["updated_at"]),
    }


async def list_filter_keywords(category: str | None) -> list[dict[str, Any]]:
    await ensure_tables()
    stmt = (
        select(*_FK).where(_FK.category == category).order_by(_FK.id)
        if category
        else select(*_FK).order_by(_FK.category, _FK.id)
    )
    return [_keyword_row(r) for r in await _run(stmt)]


async def create_filter_keyword(fields: dict[str, Any]) -> dict[str, Any]:
    await ensure_tables()
    category, keyword = fields.get("category"), fields.get("keyword")
    if await _run(select(_FK.id).where(_FK.category == category, _FK.keyword == keyword)):
        raise ConflictError(f"A {category} keyword {keyword!r} already exists")
    now = _now()
    values = {
        **{k: fields[k] for k in ("keyword", "category", "enabled") if k in fields},
        "created_at": now,
        "updated_at": now,
    }
    if "enabled" in values:
        values["enabled"] = int(bool(values["enabled"]))
    rows = await _run(insert(filter_keywords).values(**values).returning(*_FK))
    return _keyword_row(rows[0])


async def update_filter_keyword(keyword_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    await ensure_tables()
    values = {k: fields[k] for k in ("keyword", "category", "enabled") if k in fields}
    if not values:
        raise NotFoundError("No fields to update")
    current = await _run(select(_FK.category, _FK.keyword).where(_FK.id == keyword_id))
    if not current:
        raise NotFoundError(f"Filter keyword {keyword_id} not found")
    category, keyword = values.get("category", current[0]["category"]), values.get("keyword", current[0]["keyword"])
    clash = await _run(select(_FK.id).where(_FK.category == category, _FK.keyword == keyword, _FK.id != keyword_id))
    if clash:
        raise ConflictError("A keyword with that category/text already exists")
    if "enabled" in values:
        values["enabled"] = int(bool(values["enabled"]))
    rows = await _run(
        update(filter_keywords).where(_FK.id == keyword_id).values(**values, updated_at=_now()).returning(*_FK)
    )
    return _keyword_row(rows[0])


async def delete_filter_keyword(keyword_id: int) -> None:
    await ensure_tables()
    if not await _run(delete(filter_keywords).where(_FK.id == keyword_id).returning(_FK.id)):
        raise NotFoundError(f"Filter keyword {keyword_id} not found")


# --- crawl_schedules -----------------------------------------------------------------------------------------------

_CS = crawl_schedules.c
_CS_COLUMNS = (
    _CS.platform,
    _CS.run_time,
    _CS.enabled,
    _CS.last_triggered_date,
    _CS.nurture_before,
    _CS.nurture_after,
    _CS.updated_at,
)


def _crawl_schedule_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "enabled": bool(row["enabled"]),
        "nurture_before": bool(row["nurture_before"]),
        "nurture_after": bool(row["nurture_after"]),
        "last_triggered_date": _day(row["last_triggered_date"]),
        "updated_at": _when(row["updated_at"]),
    }


async def list_crawl_schedules() -> list[dict[str, Any]]:
    await ensure_tables()
    return [_crawl_schedule_row(r) for r in await _run(select(*_CS_COLUMNS).order_by(_CS.platform))]


async def ensure_default_crawl_schedules(platforms: set[str]) -> None:
    await ensure_tables()
    for platform in platforms:
        await _run(
            insert(crawl_schedules)
            .values(platform=platform, updated_at=_now())
            .on_conflict_do_nothing(index_elements=["platform"])
        )


async def upsert_crawl_schedule(
    platform: str, *, run_time: str, enabled: bool, nurture_before: bool, nurture_after: bool
) -> dict[str, Any]:
    await ensure_tables()
    now = _now()
    stmt = insert(crawl_schedules).values(
        platform=platform,
        run_time=run_time,
        enabled=int(enabled),
        nurture_before=int(nurture_before),
        nurture_after=int(nurture_after),
        updated_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["platform"],
        set_={name: stmt.excluded[name] for name in ("run_time", "enabled", "nurture_before", "nurture_after")}
        | {"updated_at": now},
    ).returning(*_CS_COLUMNS)
    return _crawl_schedule_row((await _run(stmt))[0])


async def mark_crawl_schedule_triggered(platform: str, triggered_date: str) -> None:
    await ensure_tables()
    await _run(update(crawl_schedules).where(_CS.platform == platform).values(last_triggered_date=str(triggered_date)))


# --- comment_crawl_schedules ---------------------------------------------------------------------------------------

_CC = comment_crawl_schedules.c
_CC_COLUMNS = (_CC.platform, _CC.run_time, _CC.enabled, _CC.top_n, _CC.last_triggered_date, _CC.updated_at)


def _comment_schedule_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "enabled": bool(row["enabled"]),
        "last_triggered_date": _day(row["last_triggered_date"]),
        "updated_at": _when(row["updated_at"]),
    }


async def list_comment_crawl_schedules() -> list[dict[str, Any]]:
    await ensure_tables()
    return [_comment_schedule_row(r) for r in await _run(select(*_CC_COLUMNS).order_by(_CC.platform))]


async def ensure_default_comment_crawl_schedules(platforms: set[str]) -> None:
    await ensure_tables()
    for platform in platforms:
        await _run(
            insert(comment_crawl_schedules)
            .values(platform=platform, updated_at=_now())
            .on_conflict_do_nothing(index_elements=["platform"])
        )


async def upsert_comment_crawl_schedule(platform: str, *, run_time: str, enabled: bool, top_n: int) -> dict[str, Any]:
    await ensure_tables()
    now = _now()
    stmt = insert(comment_crawl_schedules).values(
        platform=platform, run_time=run_time, enabled=int(enabled), top_n=top_n, updated_at=now
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["platform"],
        set_={name: stmt.excluded[name] for name in ("run_time", "enabled", "top_n")} | {"updated_at": now},
    ).returning(*_CC_COLUMNS)
    return _comment_schedule_row((await _run(stmt))[0])


# --- cleanup -------------------------------------------------------------------------------------------------------


async def get_cleanup_settings() -> dict[str, Any]:
    await ensure_tables()
    rows = await _run(
        select(cleanup_settings.c.settings, cleanup_settings.c.updated_at).where(cleanup_settings.c.id == 1)
    )
    if not rows:
        return {"settings": {}, "updated_at": None}
    return {"settings": _json(rows[0]["settings"]), "updated_at": _when(rows[0]["updated_at"])}


async def upsert_cleanup_settings(values: dict[str, Any]) -> dict[str, Any]:
    """Chỉ ghi các khoá có giá trị - khoá thiếu giữ giá trị đã lưu (cập nhật một phần)."""
    merged: dict[str, Any] = dict((await get_cleanup_settings())["settings"])
    merged.update({k: v for k, v in values.items() if v is not None})
    now = _now()
    stmt = insert(cleanup_settings).values(id=1, settings=json.dumps(merged, ensure_ascii=False), updated_at=now)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"], set_={"settings": stmt.excluded.settings, "updated_at": now}
    ).returning(cleanup_settings.c.settings, cleanup_settings.c.updated_at)
    row = (await _run(stmt))[0]
    return {"settings": _json(row["settings"]), "updated_at": _when(row["updated_at"])}


async def record_cleanup_run_start(*, dry_run: bool, triggered_by: str) -> int:
    await ensure_tables()
    stmt = (
        insert(cleanup_run_history)
        .values(started_at=_now(), dry_run=int(dry_run), triggered_by=triggered_by)
        .returning(cleanup_run_history.c.id)
    )
    return int((await _run(stmt))[0]["id"])


async def record_cleanup_run_finish(run_id: int, *, summary: dict[str, Any], error: str | None = None) -> None:
    await ensure_tables()
    await _run(
        update(cleanup_run_history)
        .where(cleanup_run_history.c.id == run_id)
        .values(
            finished_at=_now(),
            posts_deleted=int(summary.get("posts") or 0),
            comments_deleted=int(summary.get("comments") or 0),
            snapshots_deleted=int(summary.get("snapshots") or 0),
            batches=int(summary.get("batches") or 0),
            remaining_posts=summary.get("remaining_posts"),
            error=error,
        )
    )


async def list_cleanup_run_history(limit: int) -> list[dict[str, Any]]:
    await ensure_tables()
    c = cleanup_run_history.c
    rows = await _run(
        select(
            c.id,
            c.started_at,
            c.finished_at,
            c.dry_run,
            c.triggered_by,
            c.posts_deleted,
            c.comments_deleted,
            c.snapshots_deleted,
            c.batches,
            c.remaining_posts,
            c.error,
        )
        .order_by(c.started_at.desc())
        .limit(limit)
    )
    return [
        {
            **r,
            "dry_run": bool(r["dry_run"]),
            "started_at": _when(r["started_at"]),
            "finished_at": _when(r["finished_at"]),
        }
        for r in rows
    ]


# --- discovery_settings / omar_api_settings ------------------------------------------------------------------------


async def get_discovery_settings() -> dict[str, Any]:
    await ensure_tables()
    rows = await _run(
        select(discovery_settings.c.enabled, discovery_settings.c.updated_at).where(discovery_settings.c.id == 1)
    )
    if not rows:
        return {"enabled": True, "updated_at": None}
    return {"enabled": bool(rows[0]["enabled"]), "updated_at": _when(rows[0]["updated_at"])}


async def set_discovery_enabled(enabled: bool) -> dict[str, Any]:
    await ensure_tables()
    now = _now()
    stmt = insert(discovery_settings).values(id=1, enabled=int(enabled), updated_at=now)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"], set_={"enabled": stmt.excluded.enabled, "updated_at": now}
    ).returning(discovery_settings.c.enabled, discovery_settings.c.updated_at)
    row = (await _run(stmt))[0]
    return {"enabled": bool(row["enabled"]), "updated_at": _when(row["updated_at"])}


async def get_omar_api_settings() -> dict[str, Any] | None:
    await ensure_tables()
    c = omar_api_settings.c
    rows = await _run(
        select(c.api_key, c.requests_per_minute, c.daily_requests, c.concurrency, c.updated_at).where(c.id == 1)
    )
    return {**rows[0], "updated_at": _when(rows[0]["updated_at"])} if rows else None


async def set_omar_limits(changes: dict[str, int]) -> None:
    await ensure_tables()
    now = _now()
    stmt = insert(omar_api_settings).values(id=1, updated_at=now, **changes)
    await _run(
        stmt.on_conflict_do_update(
            index_elements=["id"], set_={**{name: stmt.excluded[name] for name in changes}, "updated_at": now}
        )
    )


async def upsert_omar_api_key(api_key: str) -> dict[str, Any]:
    await ensure_tables()
    now = _now()
    stmt = insert(omar_api_settings).values(id=1, api_key=api_key, updated_at=now)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"], set_={"api_key": stmt.excluded.api_key, "updated_at": now}
    ).returning(omar_api_settings.c.api_key, omar_api_settings.c.updated_at)
    row = (await _run(stmt))[0]
    return {**row, "updated_at": _when(row["updated_at"])}
