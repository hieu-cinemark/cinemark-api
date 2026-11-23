"""Bản D1 (database cinemark-config-db) của các bảng cấu hình: ai_settings, ai_providers,
ai_accuracy_runs, proxy_settings. Query dựng bằng SQLAlchemy Core (dialect SQLite) rồi gửi qua d1_exec; hàm ở đây trả dữ liệu dạng
Python (bool, dict, datetime) nên chỗ gọi giữ nguyên kiểu dữ liệu.

Bảng tự tạo (IF NOT EXISTS) ở lần dùng đầu."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Column, Integer, MetaData, Table, Text, delete, select, text, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateTable

from app.core.errors import UpstreamError
from app.repositories.d1.sql import d1_exec

config_metadata = MetaData()
EMPTY_TEXT = text("''")
EMPTY_JSON_TEXT = text("'{}'")

ai_settings = Table(
    "ai_settings",
    config_metadata,
    Column("id", Integer, primary_key=True),
    Column("enabled", Integer, nullable=False, server_default=text("0")),
    Column("model", Text, nullable=False, server_default=EMPTY_TEXT),
    Column("prompts", Text, nullable=False, server_default=EMPTY_JSON_TEXT),
    Column("active_provider", Text, nullable=False, server_default=EMPTY_TEXT),
    Column("updated_at", Text, nullable=False),
)

ai_providers = Table(
    "ai_providers",
    config_metadata,
    Column("key", Text, primary_key=True),
    Column("base_url", Text, nullable=False, server_default=EMPTY_TEXT),
    Column("api_key", Text, nullable=False, server_default=EMPTY_TEXT),
    Column("model", Text, nullable=False, server_default=EMPTY_TEXT),
    Column("reasoning_effort", Text, nullable=False, server_default=EMPTY_TEXT),
    Column("updated_at", Text, nullable=False),
)

ai_accuracy_runs = Table(
    "ai_accuracy_runs",
    config_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("labeled_at", Text, nullable=False),
    Column("sample_size", Integer, nullable=False),
    Column("metrics", Text, nullable=False),
    Column("note", Text),
    Column("created_at", Text, nullable=False),
)

proxy_settings = Table(
    "proxy_settings",
    config_metadata,
    Column("id", Integer, primary_key=True),
    Column("settings", Text, nullable=False, server_default=EMPTY_JSON_TEXT),
    Column("updated_at", Text, nullable=False),
)

_TABLES = (ai_settings, ai_providers, ai_accuracy_runs, proxy_settings)
_ready = False


async def _run(stmt: Any) -> list[dict[str, Any]]:
    rows = await d1_exec(stmt, database="config", timeout=15.0)
    if rows is None:
        raise UpstreamError("Could not reach the config database (D1)")
    return rows


def _now() -> str:
    return datetime.now(UTC).isoformat()


def to_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return {}
    return value if value is not None else {}


async def ensure_tables() -> None:
    """Tạo bảng nếu chưa có và gieo dòng ai_settings (id=1)."""
    global _ready
    if _ready:
        return
    for table in _TABLES:
        await _run(CreateTable(table, if_not_exists=True))
    seed = insert(ai_settings).values(id=1, enabled=0, model="", prompts="{}", updated_at=_now())
    await _run(seed.on_conflict_do_nothing(index_elements=["id"]))
    _ready = True


def reset_for_tests() -> None:
    global _ready
    _ready = False


# --- ai_settings ---------------------------------------------------------------------------------------------------


def _settings_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "enabled": bool(row["enabled"]),
        "prompts": _json(row["prompts"]),
        "active_provider": row["active_provider"] or "",
        "updated_at": to_datetime(row["updated_at"]),
    }


_SETTINGS_COLUMNS = (
    ai_settings.c.id,
    ai_settings.c.enabled,
    ai_settings.c.prompts,
    ai_settings.c.active_provider,
    ai_settings.c.updated_at,
)


async def get_ai_settings() -> dict[str, Any] | None:
    await ensure_tables()
    rows = await _run(select(*_SETTINGS_COLUMNS).where(ai_settings.c.id == 1))
    return _settings_row(rows[0]) if rows else None


async def set_active_ai_provider(key: str) -> None:
    await ensure_tables()
    await _run(update(ai_settings).where(ai_settings.c.id == 1).values(active_provider=key, updated_at=_now()))


async def upsert_ai_settings(*, enabled_flag: bool, prompts: dict[str, str]) -> dict[str, Any]:
    await ensure_tables()
    now = _now()
    stmt = insert(ai_settings).values(
        id=1, enabled=int(enabled_flag), prompts=json.dumps(prompts, ensure_ascii=False), updated_at=now
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={"enabled": stmt.excluded.enabled, "prompts": stmt.excluded.prompts, "updated_at": now},
    ).returning(*_SETTINGS_COLUMNS)
    return _settings_row((await _run(stmt))[0])


# --- ai_providers --------------------------------------------------------------------------------------------------

_PROVIDER_COLUMNS = (
    ai_providers.c.key,
    ai_providers.c.base_url,
    ai_providers.c.api_key,
    ai_providers.c.model,
    ai_providers.c.reasoning_effort,
    ai_providers.c.updated_at,
)


def _provider_row(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "updated_at": to_datetime(row["updated_at"])}


async def list_ai_providers() -> list[dict[str, Any]]:
    await ensure_tables()
    return [_provider_row(r) for r in await _run(select(*_PROVIDER_COLUMNS).order_by(ai_providers.c.key))]


async def get_ai_provider(key: str) -> dict[str, Any] | None:
    await ensure_tables()
    rows = await _run(select(*_PROVIDER_COLUMNS).where(ai_providers.c.key == key))
    return _provider_row(rows[0]) if rows else None


async def upsert_ai_provider(key: str, fields: dict[str, str]) -> dict[str, Any]:
    """fields: chỉ các cột cần ghi (base_url, model, và api_key/reasoning_effort khi có) - cột vắng giữ nguyên."""
    await ensure_tables()
    now = _now()
    stmt = insert(ai_providers).values(key=key, updated_at=now, **fields)
    stmt = stmt.on_conflict_do_update(
        index_elements=["key"], set_={**{name: stmt.excluded[name] for name in fields}, "updated_at": now}
    ).returning(*_PROVIDER_COLUMNS)
    return _provider_row((await _run(stmt))[0])


async def delete_ai_provider(key: str) -> bool:
    await ensure_tables()
    rows = await _run(delete(ai_providers).where(ai_providers.c.key == key).returning(ai_providers.c.key))
    return bool(rows)


# --- ai_accuracy_runs ----------------------------------------------------------------------------------------------


async def insert_accuracy_run(*, labeled_at: str, sample_size: int, metrics: dict[str, Any], note: str | None) -> None:
    await ensure_tables()
    await _run(
        insert(ai_accuracy_runs).values(
            labeled_at=labeled_at,
            sample_size=sample_size,
            metrics=json.dumps(metrics, ensure_ascii=False),
            note=note,
            created_at=_now(),
        )
    )


async def get_latest_accuracy_run() -> dict[str, Any] | None:
    await ensure_tables()
    cols = ai_accuracy_runs.c
    rows = await _run(
        select(cols.labeled_at, cols.sample_size, cols.metrics, cols.note, cols.created_at)
        .order_by(cols.id.desc())
        .limit(1)
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "labeled_at": date.fromisoformat(str(row["labeled_at"])[:10]),
        "sample_size": row["sample_size"],
        "metrics": _json(row["metrics"]),
        "note": row["note"],
        "created_at": to_datetime(row["created_at"]),
    }


# --- proxy_settings ------------------------------------------------------------------------------------------------


async def get_proxy_settings() -> dict[str, Any] | None:
    await ensure_tables()
    rows = await _run(select(proxy_settings.c.settings, proxy_settings.c.updated_at).where(proxy_settings.c.id == 1))
    if not rows:
        return None
    return {"settings": _json(rows[0]["settings"]), "updated_at": to_datetime(rows[0]["updated_at"])}


async def upsert_proxy_settings(values: dict[str, Any]) -> dict[str, Any]:
    await ensure_tables()
    now = _now()
    stmt = insert(proxy_settings).values(id=1, settings=json.dumps(values, ensure_ascii=False), updated_at=now)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"], set_={"settings": stmt.excluded.settings, "updated_at": now}
    ).returning(proxy_settings.c.settings, proxy_settings.c.updated_at)
    row = (await _run(stmt))[0]
    return {"settings": _json(row["settings"]), "updated_at": to_datetime(row["updated_at"])}
