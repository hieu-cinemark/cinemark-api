"""Dashboard Settings page: CRUD over the platform_accounts /
platform_proxies Supabase tables (see app/services/platform_config_db.py) -
the same tables spider-hub reads from for login credentials and proxy
config. Read-only elsewhere; this is the only place that writes them."""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.core.errors import NotFoundError, ValidationError
from app.schemas.settings import AccountCreate, AccountOut, AccountUpdate, ProxyCreate, ProxyOut, ProxyUpdate
from app.services import platform_config_db as db
from app.services.account_health import evaluate_account_health
from app.services.kafka import publish_tiktok_identity_reset

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


@router.post("/accounts/{account_id}/check", response_model=AccountOut)
async def check_account(account_id: int) -> AccountOut:
    """Passive health check - reads signals spider-hub already maintains
    (Redis block counters, session cache) instead of sending a fresh
    request to the platform. See app/services/account_health.py."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    status = await evaluate_account_health(account)
    row = await db.update_account_check_result(account_id, status=status)
    return AccountOut(**row)


@router.post("/accounts/{account_id}/reset-cookies")
async def reset_tiktok_cookies(account_id: int) -> dict[str, bool]:
    """Triggers spider-hub's headless TikTok identity re-capture
    (device_id/odinId) for this one account row - see
    app/services/kafka.py's publish_tiktok_identity_reset. TikTok-only:
    Facebook/Threads use the platform-wide token_refresh routes instead
    (see app/api/routes/token_refresh.py), which re-run their own
    password/2FA browser-bootstrap flow rather than targeting one row."""
    account = await db.get_account(account_id)
    if account is None:
        raise NotFoundError(f"Account {account_id} not found")
    if account["platform"] != "tiktok":
        raise ValidationError("Cookie reset is only supported for tiktok accounts")
    ok = await publish_tiktok_identity_reset(account_id)
    return {"ok": ok}


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
