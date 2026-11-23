"""Bản D1 của platform_accounts, platform_proxies, proxy_providers, account_events (database cinemark-config-db). Cùng cách làm với
d1_store.py: SQLAlchemy Core (dialect SQLite) -> d1_exec; hàm trả đúng dạng dữ liệu bản Postgres (bool, datetime, nhãn pool_status...).

Thời gian lưu dạng TEXT ISO-8601 UTC độ rộng cố định (microsecond, +00:00) để so sánh chuỗi đúng thứ tự; spider-hub ghi cùng định dạng.
Schema ở đây là nguồn tạo bảng (ensure_tables); spider-hub (social_crawler/db) chỉ đọc/ghi dữ liệu và giữ tên cột khớp.
Bảng Postgres cũ còn các cột không code nào dùng (assigned_proxy_id, session_client..., proxy_providers.kind/ip_allowlist) - không chép."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Column, Index, Integer, Table, Text, and_, delete, func, insert, not_, or_, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.schema import CreateIndex, CreateTable

from app.core.errors import ConflictError, NotFoundError, UpstreamError
from app.core.logging import get_logger
from app.services.accounts.config_db.d1_store import _EMPTY, _EMPTY_JSON, _metadata, _run, _when

logger = get_logger(__name__)

_ZERO = text("0")
_ONE = text("1")
_ACTIVE = text("'active'")

platform_accounts = Table(
    "platform_accounts",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("platform", Text, nullable=False),
    Column("account_id", Text, nullable=False),
    Column("password", Text, nullable=False, server_default=_EMPTY),
    Column("totp_secret", Text, nullable=False, server_default=_EMPTY),
    Column("cookie", Text, nullable=False, server_default=_EMPTY),
    Column("token", Text, nullable=False, server_default=_EMPTY),
    Column("email", Text, nullable=False, server_default=_EMPTY),
    Column("email_password", Text, nullable=False, server_default=_EMPTY),
    Column("enabled", Integer, nullable=False, server_default=_ONE),
    Column("created_at", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    Column("last_checked_at", Text),
    Column("last_check_status", Text),
    Column("last_check_note", Text),
    Column("status", Text, nullable=False, server_default=_ACTIVE),
    Column("cooldown_until", Text),
    Column("consecutive_failures", Integer, nullable=False, server_default=_ZERO),
    Column("last_used_at", Text),
    Column("nurture_only", Integer, nullable=False, server_default=_ZERO),
    Index("platform_accounts_platform_idx", "platform"),
)

platform_proxies = Table(
    "platform_proxies",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("platform", Text, nullable=False, server_default=text("'all'")),
    Column("proxy_url", Text, nullable=False),
    Column("username", Text, nullable=False, server_default=_EMPTY),
    Column("password", Text, nullable=False, server_default=_EMPTY),
    Column("login_use_proxy", Integer, nullable=False, server_default=_ZERO),
    Column("enabled", Integer, nullable=False, server_default=_ONE),
    Column("created_at", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    Column("status", Text, nullable=False, server_default=_ACTIVE),
    Column("cooldown_until", Text),
    Column("consecutive_failures", Integer, nullable=False, server_default=_ZERO),
    Column("last_used_at", Text),
    Column("ip_type", Text, nullable=False, server_default=text("'unknown'")),
    Column("provider_key", Text),
    Column("provider_ref", Text),
    Index("platform_proxies_platform_idx", "platform"),
)

proxy_providers = Table(
    "proxy_providers",
    _metadata,
    Column("key", Text, primary_key=True),
    Column("api_url", Text, nullable=False, server_default=_EMPTY),
    Column("token", Text, nullable=False, server_default=_EMPTY),
    Column("updated_at", Text, nullable=False),
    Column("last_used_at", Text),
    Column("last_status", Text),
)

account_events = Table(
    "account_events",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("at", Text, nullable=False),
    Column("platform", Text, nullable=False),
    Column("account_key", Text, nullable=False),
    Column("event", Text, nullable=False),
    Column("details", Text, nullable=False, server_default=_EMPTY_JSON),
    Index("account_events_account_at", "platform", "account_key", "at"),
)

_TABLES = (platform_accounts, platform_proxies, proxy_providers, account_events)
_ready = False

_A = platform_accounts.c
_P = platform_proxies.c
_V = proxy_providers.c


def now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


async def ensure_tables() -> None:
    global _ready
    if _ready:
        return
    for table in _TABLES:
        await _run(CreateTable(table, if_not_exists=True))
        for index in table.indexes:
            await _run(CreateIndex(index, if_not_exists=True))
    _ready = True


def reset_for_tests() -> None:
    global _ready
    _ready = False


def _times(row: dict[str, Any], *names: str) -> None:
    for name in names:
        row[name] = _when(row.get(name))


# --- tài khoản ---------------------------------------------------------------------------------------------------

_ACCOUNT_SECRETS = ("password", "totp_secret", "cookie", "token", "email_password")
_ACCOUNT_COLUMNS = (
    _A.id,
    _A.platform,
    _A.account_id,
    _A.password,
    _A.totp_secret,
    _A.cookie,
    _A.token,
    _A.email,
    _A.email_password,
    _A.enabled,
    _A.created_at,
    _A.updated_at,
    _A.last_checked_at,
    _A.last_check_status,
    _A.last_check_note,
    _A.status.label("pool_status"),
    _A.cooldown_until,
    _A.consecutive_failures,
    _A.last_used_at,
)
_ACCOUNT_WRITABLE = (
    "platform",
    "account_id",
    "password",
    "totp_secret",
    "cookie",
    "token",
    "email",
    "email_password",
    "enabled",
)


def _account_row(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    row["enabled"] = bool(row["enabled"])
    _times(row, "created_at", "updated_at", "last_checked_at", "cooldown_until", "last_used_at")
    return row


async def count_crawl_ready_accounts(platform: str) -> int:
    await ensure_tables()
    stmt = (
        select(func.count().label("n"))
        .select_from(platform_accounts)
        .where(
            _A.platform == platform,
            _A.enabled == 1,
            _A.status != "checkpoint",
            or_(_A.cooldown_until.is_(None), _A.cooldown_until <= now_text()),
            or_(_A.platform == "facebook", _A.nurture_only == 0),
        )
    )
    rows = await _run(stmt)
    return int(rows[0]["n"]) if rows else 0


async def record_account_event(account: dict[str, Any], event: str, **details: Any) -> None:
    key = str(account.get("email") or account.get("account_id") or "").strip().lower()
    try:
        await ensure_tables()
        await _run(
            insert(account_events).values(
                at=now_text(),
                platform=account["platform"],
                account_key=key,
                event=event,
                details=json.dumps(details, default=str),
            )
        )
    except UpstreamError as exc:
        logger.warning("account_event_write_failed", event_name=event, error_type=type(exc).__name__)


async def list_accounts(platform: str | None = None, *, with_secrets: bool = True) -> list[dict[str, Any]]:
    await ensure_tables()
    stmt = select(*_ACCOUNT_COLUMNS)
    stmt = stmt.where(_A.platform == platform).order_by(_A.id) if platform else stmt.order_by(_A.platform, _A.id)
    rows = [_account_row(r) for r in await _run(stmt)]
    if not with_secrets:
        # Danh sách trên dashboard che hết bí mật: giá trị rỗng + cờ has_<cột> (cùng hợp đồng với bản Postgres).
        for row in rows:
            for name in _ACCOUNT_SECRETS:
                row[f"has_{name}"] = bool(row[name])
                row[name] = ""
    return rows


async def get_account(account_id: int) -> dict[str, Any] | None:
    await ensure_tables()
    rows = await _run(select(*_ACCOUNT_COLUMNS).where(_A.id == account_id))
    return _account_row(rows[0]) if rows else None


def _account_values(fields: dict[str, Any]) -> dict[str, Any]:
    values = {c: fields[c] for c in _ACCOUNT_WRITABLE if c in fields}
    if "enabled" in values:
        values["enabled"] = int(bool(values["enabled"]))
    return values


async def create_account(fields: dict[str, Any]) -> dict[str, Any]:
    await ensure_tables()
    now = now_text()
    stmt = (
        insert(platform_accounts)
        .values(**_account_values(fields), created_at=now, updated_at=now)
        .returning(*_ACCOUNT_COLUMNS)
    )
    return _account_row((await _run(stmt))[0])


async def update_account(account_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    values = _account_values(fields)
    if not values:
        raise NotFoundError("No fields to update")
    # Chỉ đổi cookie mới làm updated_at mới hơn lần kiểm tra (spider-hub coi đó là "đã có cookie mới").
    if "cookie" in values:
        values["updated_at"] = now_text()
    await ensure_tables()
    rows = await _run(
        update(platform_accounts).where(_A.id == account_id).values(**values).returning(*_ACCOUNT_COLUMNS)
    )
    if not rows:
        raise NotFoundError(f"Account {account_id} not found")
    return _account_row(rows[0])


async def update_account_check_result(account_id: int, *, status: str) -> dict[str, Any]:
    await ensure_tables()
    still_dead = and_(
        _A.last_check_status.is_("dead"),
        or_(_A.last_checked_at.is_(None), _A.updated_at.is_(None), _A.last_checked_at >= _A.updated_at),
    )
    stmt = (
        update(platform_accounts)
        .where(_A.id == account_id, not_(still_dead))
        .values(last_checked_at=now_text(), last_check_status=status)
        .returning(*_ACCOUNT_COLUMNS)
    )
    rows = await _run(stmt)
    row = _account_row(rows[0]) if rows else await get_account(account_id)
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def set_account_enabled(account_id: int, enabled: bool) -> dict[str, Any]:
    await ensure_tables()
    values: dict[str, Any] = {"enabled": int(enabled)}
    if enabled:
        values |= {"status": "active", "consecutive_failures": 0, "cooldown_until": None}
    rows = await _run(
        update(platform_accounts).where(_A.id == account_id).values(**values).returning(*_ACCOUNT_COLUMNS)
    )
    if not rows:
        raise NotFoundError(f"Account {account_id} not found")
    return _account_row(rows[0])


async def delete_account(account_id: int) -> None:
    account = await get_account(account_id)
    if account is not None:
        await record_account_event(account, "account_deleted", last_check_status=account.get("last_check_status"))
    if not await _run(delete(platform_accounts).where(_A.id == account_id).returning(_A.id)):
        raise NotFoundError(f"Account {account_id} not found")


# --- proxy -------------------------------------------------------------------------------------------------------

_PROXY_COLUMNS = (
    _P.id,
    _P.platform,
    _P.proxy_url,
    _P.username,
    _P.password,
    _P.login_use_proxy,
    _P.ip_type,
    _P.enabled,
    _P.created_at,
    _P.updated_at,
    _P.status.label("pool_status"),
    _P.cooldown_until,
    _P.consecutive_failures,
    _P.last_used_at,
    _P.provider_key,
    _P.provider_ref,
)
_PROXY_WRITABLE = (
    "platform",
    "proxy_url",
    "username",
    "password",
    "login_use_proxy",
    "ip_type",
    "enabled",
    "provider_key",
    "provider_ref",
)


def _proxy_row(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    row["enabled"] = bool(row["enabled"])
    row["login_use_proxy"] = bool(row["login_use_proxy"])
    _times(row, "created_at", "updated_at", "cooldown_until", "last_used_at", "provider_last_used_at")
    return row


def _proxy_values(fields: dict[str, Any]) -> dict[str, Any]:
    values = {c: fields[c] for c in _PROXY_WRITABLE if c in fields}
    for name in ("enabled", "login_use_proxy"):
        if name in values:
            values[name] = int(bool(values[name]))
    return values


async def list_proxies(platform: str | None = None) -> list[dict[str, Any]]:
    await ensure_tables()
    source = platform_proxies.outerjoin(proxy_providers, _V.key == _P.provider_key)
    stmt = select(
        *_PROXY_COLUMNS, _V.last_used_at.label("provider_last_used_at"), _V.last_status.label("provider_last_status")
    ).select_from(source)
    stmt = stmt.where(_P.platform == platform).order_by(_P.id) if platform else stmt.order_by(_P.platform, _P.id)
    return [_proxy_row(r) for r in await _run(stmt)]


async def create_proxy(fields: dict[str, Any]) -> dict[str, Any]:
    await ensure_tables()
    now = now_text()
    stmt = (
        insert(platform_proxies)
        .values(**_proxy_values(fields), created_at=now, updated_at=now)
        .returning(*_PROXY_COLUMNS)
    )
    return _proxy_row((await _run(stmt))[0])


async def update_proxy(proxy_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    values = _proxy_values(fields)
    if not values:
        raise NotFoundError("No fields to update")
    # Sửa địa chỉ/đăng nhập hoặc bật lại: gỡ cờ degraded + cooldown + chuỗi lỗi do cấu hình cũ (cùng quy tắc bản Postgres).
    if any(c in values for c in ("proxy_url", "username", "password")) or fields.get("enabled") is True:
        values |= {"status": "active", "consecutive_failures": 0, "cooldown_until": None}
    await ensure_tables()
    rows = await _run(
        update(platform_proxies)
        .where(_P.id == proxy_id)
        .values(**values, updated_at=now_text())
        .returning(*_PROXY_COLUMNS)
    )
    if not rows:
        raise NotFoundError(f"Proxy {proxy_id} not found")
    return _proxy_row(rows[0])


async def delete_proxy(proxy_id: int) -> None:
    await ensure_tables()
    if not await _run(delete(platform_proxies).where(_P.id == proxy_id).returning(_P.id)):
        raise NotFoundError(f"Proxy {proxy_id} not found")


# --- nguồn proxy ---------------------------------------------------------------------------------------------------

_PROVIDER_COLUMNS = (_V.key, _V.api_url, _V.token, _V.updated_at, _V.last_used_at, _V.last_status)


def _provider_row(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    _times(row, "updated_at", "last_used_at")
    return row


async def list_proxy_providers() -> list[dict[str, Any]]:
    await ensure_tables()
    count = select(func.count()).where(_P.provider_key == _V.key).correlate(proxy_providers).scalar_subquery()
    rows = await _run(select(*_PROVIDER_COLUMNS, count.label("proxy_count")).order_by(_V.key))
    return [_provider_row(r) for r in rows]


async def upsert_proxy_provider(key: str, *, api_url: str, token: str | None) -> dict[str, Any]:
    await ensure_tables()
    now = now_text()
    fields: dict[str, Any] = {"api_url": api_url}
    if token is not None:
        fields["token"] = token
    stmt = sqlite_insert(proxy_providers).values(key=key, updated_at=now, **fields)
    stmt = stmt.on_conflict_do_update(
        index_elements=["key"], set_={**{name: stmt.excluded[name] for name in fields}, "updated_at": now}
    ).returning(*_PROVIDER_COLUMNS)
    return _provider_row((await _run(stmt))[0])


async def delete_proxy_provider(key: str, tiktok_synthetic_provider: str | None) -> None:
    """Chặn xoá khi còn proxy trỏ tới hoặc là nguồn của tiktok_synthetic_provider (người gọi đã đọc proxy_settings)."""
    await ensure_tables()
    used = (await _run(select(func.count().label("n")).select_from(platform_proxies).where(_P.provider_key == key)))[0][
        "n"
    ]
    if used:
        raise ConflictError(f"Provider {key} is still used by {used} proxy row(s)")
    if tiktok_synthetic_provider == key:
        raise ConflictError(f"Provider {key} is the tiktok_synthetic_provider - point it elsewhere first")
    if not await _run(delete(proxy_providers).where(_V.key == key).returning(_V.key)):
        raise NotFoundError(f"Proxy provider {key} not found")
