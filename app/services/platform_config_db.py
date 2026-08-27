"""Async Postgres client (Supabase) for the platform_accounts /
platform_proxies tables - same tables spider-hub's own
social_crawler/services/db.py reads from (see that module there for the
full rationale: these change too often for env vars + process restarts to
be worth it). This side has read AND write access, backing the dashboard's
Settings page; spider-hub only ever reads.

Every write here builds its SQL from a fixed, whitelisted column list plus
values coming from a Pydantic model (never a raw request dict) - the model
strips unknown fields by default, so a caller can never smuggle an
arbitrary column name into the query string."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.core.config import settings
from app.core.errors import NotFoundError, UpstreamError
from app.core.logging import get_logger

logger = get_logger(__name__)

ACCOUNT_COLUMNS = (
    "id, platform, account_id, password, totp_secret, cookie, token, email, enabled, created_at, updated_at"
)
PROXY_COLUMNS = "id, platform, proxy_url, username, password, login_use_proxy, enabled, created_at, updated_at"

_ACCOUNT_CREATE_COLUMNS = ("platform", "account_id", "password", "totp_secret", "cookie", "token", "email", "enabled")
_PROXY_CREATE_COLUMNS = ("platform", "proxy_url", "username", "password", "login_use_proxy", "enabled")


async def _connect() -> psycopg.AsyncConnection[Any]:
    if not settings.database_url:
        raise UpstreamError("DATABASE_URL is not configured on cinemark-api")
    try:
        return await psycopg.AsyncConnection.connect(settings.database_url, row_factory=dict_row, connect_timeout=5)
    except psycopg.Error as exc:
        logger.error("db_connect_failed", error=str(exc))
        raise UpstreamError("Could not connect to the settings database") from exc


# --- accounts ---------------------------------------------------------


async def list_accounts(platform: str | None = None) -> list[dict[str, Any]]:
    async with await _connect() as conn, conn.cursor() as cur:
        if platform:
            await cur.execute(
                f"SELECT {ACCOUNT_COLUMNS} FROM platform_accounts WHERE platform = %s ORDER BY id", (platform,)
            )
        else:
            await cur.execute(f"SELECT {ACCOUNT_COLUMNS} FROM platform_accounts ORDER BY platform, id")
        return await cur.fetchall()


async def create_account(fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"INSERT INTO platform_accounts ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
            f"RETURNING {ACCOUNT_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
        return row  # type: ignore[return-value]


async def update_account(account_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [account_id]
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET {set_clause}, updated_at = now() WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def delete_account(account_id: int) -> None:
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_accounts WHERE id = %s", (account_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Account {account_id} not found")


async def list_proxies(platform: str | None = None) -> list[dict[str, Any]]:
    async with await _connect() as conn, conn.cursor() as cur:
        if platform:
            await cur.execute(
                f"SELECT {PROXY_COLUMNS} FROM platform_proxies WHERE platform = %s ORDER BY id", (platform,)
            )
        else:
            await cur.execute(f"SELECT {PROXY_COLUMNS} FROM platform_proxies ORDER BY platform, id")
        return await cur.fetchall()


async def create_proxy(fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"INSERT INTO platform_proxies ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
            f"RETURNING {PROXY_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
        return row  # type: ignore[return-value]


async def update_proxy(proxy_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [proxy_id]
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_proxies SET {set_clause}, updated_at = now() WHERE id = %s RETURNING {PROXY_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Proxy {proxy_id} not found")
    return row


async def delete_proxy(proxy_id: int) -> None:
    async with await _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_proxies WHERE id = %s", (proxy_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Proxy {proxy_id} not found")
