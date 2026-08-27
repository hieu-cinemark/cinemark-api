"""Dashboard Settings page: CRUD over the platform_accounts /
platform_proxies Supabase tables (see app/services/platform_config_db.py) -
the same tables spider-hub reads from for login credentials and proxy
config. Read-only elsewhere; this is the only place that writes them."""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.schemas.settings import AccountCreate, AccountOut, AccountUpdate, ProxyCreate, ProxyOut, ProxyUpdate
from app.services import platform_config_db as db

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/accounts", response_model=list[AccountOut])
async def list_accounts(platform: str | None = Query(default=None)) -> list[AccountOut]:
    rows = await db.list_accounts(platform)
    return [AccountOut(**row) for row in rows]


@router.post("/accounts", response_model=AccountOut)
async def create_account(payload: AccountCreate) -> AccountOut:
    row = await db.create_account(payload.model_dump())
    return AccountOut(**row)


@router.patch("/accounts/{account_id}", response_model=AccountOut)
async def update_account(account_id: int, payload: AccountUpdate) -> AccountOut:
    row = await db.update_account(account_id, payload.model_dump(exclude_unset=True))
    return AccountOut(**row)


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: int) -> dict[str, bool]:
    await db.delete_account(account_id)
    return {"ok": True}


@router.get("/proxies", response_model=list[ProxyOut])
async def list_proxies(platform: str | None = Query(default=None)) -> list[ProxyOut]:
    rows = await db.list_proxies(platform)
    return [ProxyOut(**row) for row in rows]


@router.post("/proxies", response_model=ProxyOut)
async def create_proxy(payload: ProxyCreate) -> ProxyOut:
    row = await db.create_proxy(payload.model_dump())
    return ProxyOut(**row)


@router.patch("/proxies/{proxy_id}", response_model=ProxyOut)
async def update_proxy(proxy_id: int, payload: ProxyUpdate) -> ProxyOut:
    row = await db.update_proxy(proxy_id, payload.model_dump(exclude_unset=True))
    return ProxyOut(**row)


@router.delete("/proxies/{proxy_id}")
async def delete_proxy(proxy_id: int) -> dict[str, bool]:
    await db.delete_proxy(proxy_id)
    return {"ok": True}
