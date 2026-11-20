"""get_ai_settings cache ngắn trong tiến trình: đọc lặp không chạm DB, ghi thì xoá cache, hết hạn thì đọc lại."""

import asyncio
from contextlib import asynccontextmanager

from app.services.accounts.config_db import ai_config


class _Cursor:
    def __init__(self, db):
        self.db = db

    async def execute(self, sql, params=None):
        if sql.lstrip().upper().startswith("SELECT"):
            self.db.reads += 1
        elif "UPDATE ai_settings" in sql:
            self.db.row = {**self.db.row, "active_provider": params[0]}

    async def fetchone(self):
        return dict(self.db.row)


class _Conn:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        db = self.db

        @asynccontextmanager
        async def _cm():
            yield _Cursor(db)

        return _cm()

    async def commit(self):
        pass


class _Db:
    def __init__(self):
        self.reads = 0
        self.row = {"id": 1, "enabled": True, "prompts": {}, "active_provider": "bee", "updated_at": None}


def _setup(monkeypatch):
    db = _Db()

    @asynccontextmanager
    async def connect():
        yield _Conn(db)

    async def ready():
        return None

    monkeypatch.setattr(ai_config, "_connect", connect)
    monkeypatch.setattr(ai_config, "_ensure_ai_settings_table", ready)
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
