"""Cấu hình cào: cinemark-api là nguồn sự thật, ghi đủ khoá vào proxy_settings (spider-hub chỉ đọc)."""

import asyncio

from app.schemas.settings import ProxySettings
from app.services.accounts.config_db import proxy_settings as db


def test_merge_fills_defaults_keeps_valid_values_and_renames():
    merged = db.merge_proxy_settings(
        {"youtube_lanes": 4, "public_comments_proxy_id": "bad", "provider_min_get_new_interval_seconds": 90, "lạ": 1}
    )
    assert set(merged) == set(ProxySettings().model_dump())
    assert merged["youtube_lanes"] == 4
    assert merged["public_comments_proxy_id"] == ProxySettings().public_comments_proxy_id
    assert merged["provider_min_call_interval_seconds"] == 90


def test_ensure_complete_writes_only_when_needed(monkeypatch):
    written = []

    async def stored_row():
        return {"settings": stored}

    async def upsert(values):
        written.append(values)
        return {"settings": values}

    monkeypatch.setattr(db, "get_proxy_settings", stored_row)
    monkeypatch.setattr(db, "upsert_proxy_settings", upsert)
    stored = {"youtube_lanes": 4}
    assert asyncio.run(db.ensure_proxy_settings_complete()) is True
    assert written[-1]["youtube_lanes"] == 4 and len(written[-1]) == len(ProxySettings().model_dump())
    stored = written[-1]
    assert asyncio.run(db.ensure_proxy_settings_complete()) is False
    assert len(written) == 1
