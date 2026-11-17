"""Upsert channel_profiles (follower từ ShortVid, niche từ AI) dựng bằng SQLAlchemy - chạy thật trên SQLite trong bộ nhớ:
không ghi đè số nhập tay, giữ tên đã có."""

import asyncio

from sqlalchemy import create_engine, insert, select

from app.repositories.d1.tables import channel_profiles as cp
from app.services.ai_jobs import channel_niches
from app.services.crawl import channel_profiles


def _engine():
    engine = create_engine("sqlite://")
    cp.create(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(cp),
            [
                {"platform": "tiktok", "handle": "tay", "name": "Kênh Tay", "followers": 999, "followers_source": "manual",
                 "niche": "review", "niche_source": "manual"},
                {"platform": "tiktok", "handle": "cu", "name": "Kênh Cũ", "followers": 10, "followers_source": None,
                 "niche": None, "niche_source": None},
            ],
        )  # fmt: skip
    return engine


def _run_on(engine, monkeypatch, module, coro):
    async def fake_exec(stmt, **kwargs):
        with engine.begin() as conn:
            conn.execute(stmt)
        return []

    monkeypatch.setattr(module, "d1_exec", fake_exec)
    asyncio.run(coro)


def _row(engine, handle):
    with engine.connect() as conn:
        return dict(conn.execute(select(cp).where(cp.c.handle == handle)).mappings().one())


def test_followers_upsert_keeps_manual_numbers(monkeypatch):
    engine = _engine()
    rows = [
        {"handle": "tay", "name": "Tên Mới", "followers": 5},
        {"handle": "cu", "name": None, "followers": 50},
        {"handle": "moi", "name": "Kênh Mới", "followers": None},
    ]
    _run_on(engine, monkeypatch, channel_profiles, channel_profiles._upsert("tiktok", rows, "2026-11-17T00:00:00"))
    assert _row(engine, "tay")["followers"] == 999  # nhập tay: không đụng
    cu = _row(engine, "cu")
    assert (cu["followers"], cu["name"], cu["followers_at"]) == (50, "Kênh Cũ", "2026-11-17T00:00:00")
    moi = _row(engine, "moi")
    assert (moi["name"], moi["followers"], moi["platform"]) == ("Kênh Mới", None, "tiktok")


def test_niche_upsert_keeps_manual_niche_and_existing_name(monkeypatch):
    engine = _engine()
    rows = [
        {"platform": "tiktok", "handle": "tay", "name": "X", "niche": "giai_tri", "confidence": 90, "reason": "r"},
        {"platform": "tiktok", "handle": "cu", "name": "Tên AI", "niche": "review", "confidence": 80, "reason": "phim"},
    ]
    _run_on(engine, monkeypatch, channel_niches, channel_niches.save_niches(rows, "2026-11-17T01:00:00"))
    tay = _row(engine, "tay")
    assert (tay["niche"], tay["niche_source"]) == ("review", "manual")  # nhập tay: không đụng
    cu = _row(engine, "cu")
    assert (cu["niche"], cu["niche_confidence"], cu["niche_source"], cu["name"]) == ("review", 80, "ai", "Kênh Cũ")
