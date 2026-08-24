"""Exposes the `settings` table (see app/models/setting.py) to
cinemark-web's Settings page - admin-only (see require_admin) since this
table holds real Facebook account passwords/cookies/tokens and proxy
credentials, not just toggles. Real values, not redacted: the one user who
can reach this endpoint at all is the one who's supposed to be able to read
and update their own scraper credentials here instead of editing the DB by
hand. NON_SECRET_KEYS only controls whether the FE masks a value by default
(is_secret), not whether it's returned.

Update-only (no create route) - the FE UI is for editing rows that already
exist in the table, not adding new config keys ad hoc."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_admin
from app.core.errors import NotFoundError
from app.db.session import get_db
from app.schemas.setting import SettingRead, SettingUpdate
from app.services.settings import SettingRepository

router = APIRouter(prefix="/settings", tags=["settings"])

NON_SECRET_KEYS = {"facebook_login_use_proxy", "api_direct_url"}


def _to_read(key: str, value: str) -> SettingRead:
    return SettingRead(key=key, value=value, is_secret=key not in NON_SECRET_KEYS)


@router.get("", response_model=list[SettingRead])
async def list_settings(
    db: AsyncSession = Depends(get_db), _admin=Depends(require_admin)
) -> list[SettingRead]:
    repo = SettingRepository(db)
    settings = await repo.list(limit=100)
    return [_to_read(setting.key, setting.value) for setting in settings]


@router.put("/{key}", response_model=SettingRead)
async def update_setting(
    key: str, payload: SettingUpdate, db: AsyncSession = Depends(get_db), _admin=Depends(require_admin)
) -> SettingRead:
    repo = SettingRepository(db)
    setting = await repo.get(key)
    if setting is None:
        raise NotFoundError(f"Setting {key!r} not found")
    await repo.update(setting, value=payload.value)
    return _to_read(setting.key, setting.value)
