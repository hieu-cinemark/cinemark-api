"""Quy mô hashtag TikTok qua search_hashtags: đọc kết quả, tách mục khớp chính xác, tìm cho tab Omar, chụp hằng ngày, và bảng
bản chụp chạy thật trên SQLite (upsert ghi đè trong ngày, mới nhất mỗi từ khoá)."""

import asyncio
import sqlite3
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import omar as omar_routes
from app.clients.omar import OmarStop, hashtag_search_results
from app.core.errors import register_exception_handlers
from app.repositories.d1 import hashtag_stats
from app.repositories.d1.sql import compile_sql
from app.services.omar import omar_hashtag_stats as stats

# Trích từ phản hồi thật của search_hashtags (format=clean).
SAMPLE = {
    "count": 4,
    "has_more": False,
    "next_cursor": "",
    "query": "traigiamhanhphuc",
    "results": [
        {
            "description": "",
            "hashtag_id": "7252326687039733766",
            "name": "traigiamhanhphuc",
            "user_count": 0,
            "video_count": 13615,
            "view_count": 643423877,
        },
        {
            "description": "",
            "hashtag_id": "7654828718589739026",
            "name": "traigiamhanhphucbymoli",
            "user_count": 0,
            "video_count": 258,
            "view_count": 60628513,
        },
        {
            "description": "",
            "hashtag_id": "7670022576068001799",
            "name": "traigiamhanhphuc🎬",
            "user_count": 0,
            "video_count": 236,
            "view_count": 3496009,
        },
        {
            "description": "",
            "hashtag_id": "7664951611063533588",
            "name": "traigiamhanhphu",
            "user_count": 0,
            "video_count": 5,
            "view_count": 8989,
        },
    ],
}


def run(coro):
    return asyncio.run(coro)


def test_results_are_parsed_and_junk_is_skipped():
    items = hashtag_search_results(SAMPLE)
    assert [i["name"] for i in items] == [
        "traigiamhanhphuc",
        "traigiamhanhphucbymoli",
        "traigiamhanhphuc🎬",
        "traigiamhanhphu",
    ]
    assert items[0] == {
        "hashtag_id": "7252326687039733766",
        "name": "traigiamhanhphuc",
        "video_count": 13615,
        "view_count": 643423877,
        "description": "",
    }
    messy = {
        "results": [
            {"name": "#Ok", "video_count": "12", "view_count": None},
            {"name": "  "},
            {"video_count": 5},
            {"name": "x", "video_count": "abc"},
        ]
    }
    assert hashtag_search_results(messy) == [
        {"hashtag_id": None, "name": "Ok", "video_count": 12, "view_count": 0, "description": ""},
        {"hashtag_id": None, "name": "x", "video_count": 0, "view_count": 0, "description": ""},
    ]
    assert hashtag_search_results(None) == [] and hashtag_search_results({}) == []


def test_exact_match_ignores_case_and_the_rest_is_sorted_by_videos():
    exact, related = stats.split_matches("#TraiGiamHanhPhuc", hashtag_search_results(SAMPLE))
    assert exact["video_count"] == 13615
    assert [r["name"] for r in related] == ["traigiamhanhphucbymoli", "traigiamhanhphuc🎬", "traigiamhanhphu"]
    none_exact, rest = stats.split_matches("khac", hashtag_search_results(SAMPLE))
    assert none_exact is None and len(rest) == 4


def _fake_omar(monkeypatch, *, key="k", tracked=("#traigiamhanhphuc",)):
    monkeypatch.setattr(stats, "get_omar_api_settings", AsyncMock(return_value={"api_key": key}))
    monkeypatch.setattr(
        stats,
        "get_enabled_keywords",
        AsyncMock(return_value=[{"id": "kw1", "keyword": t, "movie_id": "m1"} for t in tracked]),
    )
    search = AsyncMock(return_value=hashtag_search_results(SAMPLE))
    monkeypatch.setattr(stats, "search_hashtags", search)
    return search


def test_search_marks_tracked_hashtags_and_needs_a_key(monkeypatch):
    search = _fake_omar(monkeypatch)
    result = run(stats.search("#traigiamhanhphuc"))
    assert search.await_args.args[3] == "traigiamhanhphuc"
    assert result["exact"]["tracked"] is True and result["exact"]["video_count"] == 13615
    assert [r["tracked"] for r in result["related"]] == [False, False, False]
    _fake_omar(monkeypatch, key="")
    assert run(stats.search("abc")) is None


def test_daily_snapshot_saves_exact_matches_and_survives_failures(monkeypatch):
    _fake_omar(monkeypatch, tracked=("#traigiamhanhphuc", "#khongco", "#loi", "plain"))
    saved = []

    async def fake_search(client, pacer, key, name):
        if name == "loi":
            raise RuntimeError("boom")
        return hashtag_search_results(SAMPLE) if name == "traigiamhanhphuc" else []

    monkeypatch.setattr(stats, "search_hashtags", fake_search)
    monkeypatch.setattr(stats.hashtag_stats, "upsert_snapshot", AsyncMock(side_effect=lambda **kw: saved.append(kw)))
    result = run(stats.snapshot_all(datetime(2026, 11, 23, 20, 0, tzinfo=UTC)))
    assert result == {"hashtags": 3, "saved": 1, "missing": 1, "failed": 1}
    assert saved[0]["day"] == "2026-11-24"  # 20:00 UTC = 03:00 ngày hôm sau ở Việt Nam
    assert (
        saved[0]["keyword_id"] == "kw1"
        and saved[0]["video_count"] == 13615
        and saved[0]["hashtag"] == "traigiamhanhphuc"
    )


def test_snapshot_stops_when_omar_refuses(monkeypatch):
    _fake_omar(monkeypatch, tracked=("#a1", "#b2"))
    calls = []

    async def stop(client, pacer, key, name):
        calls.append(name)
        raise OmarStop("daily_limit: Daily limit reached")

    monkeypatch.setattr(stats, "search_hashtags", stop)
    result = run(stats.snapshot_all())
    assert calls == ["a1"] and result["saved"] == 0


class FakeRedis:
    def __init__(self):
        self.data = {}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, value, ex=None):
        self.data[key] = value


def test_snapshot_is_due_once_per_day_after_the_morning_hour(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(stats, "get_redis_client", lambda: redis)
    monkeypatch.setattr(stats, "get_pause", AsyncMock(return_value=None))
    assert run(stats.snapshot_due(datetime(2026, 11, 24, 5, 59, tzinfo=UTC))) is False
    assert run(stats.snapshot_due(datetime(2026, 11, 24, 6, 0, tzinfo=UTC))) is True
    assert run(stats.snapshot_due(datetime(2026, 11, 24, 12, 0, tzinfo=UTC))) is False  # đã chụp hôm nay
    assert run(stats.snapshot_due(datetime(2026, 11, 25, 6, 0, tzinfo=UTC))) is True
    monkeypatch.setattr(stats, "get_pause", AsyncMock(return_value={"reason": "quota"}))
    assert run(stats.snapshot_due(datetime(2026, 11, 26, 7, 0, tzinfo=UTC))) is False  # đang dừng vì hạn mức


@pytest.fixture
def d1db(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row

    async def fake_exec(stmt, **kwargs):
        sql, params = compile_sql(stmt)
        cursor = db.execute(sql, params)
        rows = [] if cursor.description is None else [dict(r) for r in cursor.fetchall()]
        db.commit()
        return rows

    monkeypatch.setattr(hashtag_stats, "d1_exec", fake_exec)
    monkeypatch.setattr(hashtag_stats, "_ready", False)
    return db


def _snap(day, kw, videos, views):
    return hashtag_stats.upsert_snapshot(
        day=day,
        keyword_id=kw,
        movie_id="m1",
        hashtag="tag",
        hashtag_id="h",
        video_count=videos,
        view_count=views,
        captured_at="x",
    )


def test_snapshot_table_overwrites_within_a_day_and_returns_the_latest(d1db):
    async def go():
        await _snap("2026-11-22", "kw1", 100, 1000)
        await _snap("2026-11-23", "kw1", 120, 1500)
        await _snap("2026-11-23", "kw1", 125, 1600)  # chạy lại trong ngày: ghi đè
        await _snap("2026-11-21", "kw2", 7, 70)
        return await hashtag_stats.latest_by_keyword()

    latest = run(go())
    assert latest["kw1"]["day"] == "2026-11-23" and latest["kw1"]["video_count"] == 125
    assert latest["kw2"]["video_count"] == 7
    assert d1db.execute("SELECT count(*) FROM tiktok_hashtag_stats").fetchone()[0] == 3
    assert d1db.execute("SELECT name FROM sqlite_master WHERE name = 'tiktok_hashtag_stats_movie_day'").fetchone()


def _client(monkeypatch, search):
    monkeypatch.setattr(omar_routes.omar_hashtag_stats, "search", search)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(omar_routes.router)
    return TestClient(app)


def test_search_route_returns_the_result_and_maps_failures(monkeypatch):
    payload = {"query": "abc", "exact": None, "related": []}
    assert _client(monkeypatch, AsyncMock(return_value=payload)).get("/omar/hashtags/search?q=abc").json() == payload
    no_key = _client(monkeypatch, AsyncMock(return_value=None)).get("/omar/hashtags/search?q=abc")
    assert no_key.status_code == 400 and no_key.json()["error"] == "Omar API key is not set"
    refused = _client(monkeypatch, AsyncMock(side_effect=OmarStop("http_401: bad key"))).get(
        "/omar/hashtags/search?q=abc"
    )
    assert refused.status_code == 502 and "refused" in refused.json()["error"]
    assert _client(monkeypatch, AsyncMock()).get("/omar/hashtags/search?q=a").status_code == 422
