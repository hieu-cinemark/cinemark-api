"""get_ai_settings cache ngắn trong tiến trình: đọc lặp không chạm DB, ghi thì xoá cache, hết hạn thì đọc lại."""

import asyncio

from app.services.accounts.config_db import ai_config, d1_store


class _Db:
    def __init__(self):
        self.reads = 0
        self.row = {"id": 1, "enabled": True, "prompts": {}, "active_provider": "bee", "updated_at": None}


def _setup(monkeypatch):
    db = _Db()

    async def get_ai_settings():
        db.reads += 1
        return dict(db.row)

    async def set_active_ai_provider(key):
        db.row = {**db.row, "active_provider": key}

    monkeypatch.setattr(d1_store, "get_ai_settings", get_ai_settings)
    monkeypatch.setattr(d1_store, "set_active_ai_provider", set_active_ai_provider)
    monkeypatch.setattr(ai_config, "_ai_settings_cache", None)
    return db


def test_repeat_reads_hit_db_once(monkeypatch):
    db = _setup(monkeypatch)

    async def go():
        for _ in range(5):
            assert (await ai_config.get_ai_settings())["active_provider"] == "bee"

    asyncio.run(go())
    assert db.reads == 1


def test_write_clears_cache(monkeypatch):
    db = _setup(monkeypatch)

    async def go():
        await ai_config.get_ai_settings()
        await ai_config.set_active_ai_provider("other")
        return await ai_config.get_ai_settings()

    assert asyncio.run(go())["active_provider"] == "other"
    assert db.reads == 2


def test_expired_cache_reads_again(monkeypatch):
    db = _setup(monkeypatch)

    async def go():
        await ai_config.get_ai_settings()
        t, row = ai_config._ai_settings_cache
        monkeypatch.setattr(ai_config, "_ai_settings_cache", (t - ai_config._AI_SETTINGS_TTL - 1, row))
        await ai_config.get_ai_settings()

    asyncio.run(go())
    assert db.reads == 2
