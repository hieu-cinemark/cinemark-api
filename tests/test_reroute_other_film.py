"""Bài tìm ra bằng từ khoá phim A nhưng nói về phim B: chuyển sang từ khoá CÙNG NỀN TẢNG của phim B."""

import asyncio

import pytest

from app.workers.ingest_consumer import main

MOVIES = {
    "a": {"id": "a", "title": "Án Mạng Xém Hoàn Hảo", "keywords": ["Án Mạng Xém Hoàn Hảo"]},
    "b": {"id": "b", "title": "Hộ Linh Tráng Sĩ", "keywords": ["Hộ Linh Tráng Sĩ", "#holinhtrangsi"]},
}


@pytest.fixture
def world(monkeypatch):
    main._film_keyword_cache.clear()
    created: list[tuple] = []
    queries: list[str] = []
    rows_by_platform = {"facebook": [{"id": "kw_b_fb"}], "tiktok": []}

    async def tracked():
        return MOVIES

    async def fake_exec(stmt, **_):
        from app.repositories.d1.sql import compile_sql

        sql, params = compile_sql(stmt)
        queries.append(sql)
        return rows_by_platform["tiktok" if "tiktok" in params else "facebook"]

    async def fake_add(movie_id, platform, keyword, keyword_id=None, *, enabled=True):
        created.append((movie_id, platform, keyword, enabled))
        return "kw_b_tiktok_new"

    monkeypatch.setattr(main, "_tracked_movies", tracked)
    monkeypatch.setattr(main, "d1_exec", fake_exec)
    monkeypatch.setattr(main, "add_keyword", fake_add)
    return created


def test_same_platform_keyword_is_chosen(world):
    kid = asyncio.run(
        main._route_post_to_film("review Hộ Linh Tráng Sĩ hay", platform="facebook", exclude_movie_id="a")
    )
    assert kid == "kw_b_fb" and world == []


def test_platform_without_keyword_gets_a_disabled_one(world):
    kid = asyncio.run(main._route_post_to_film("#holinhtrangsi đỉnh quá", platform="tiktok", exclude_movie_id="a"))
    assert kid == "kw_b_tiktok_new"
    assert world == [("b", "tiktok", "Hộ Linh Tráng Sĩ", False)]  # tắt: chỉ để gắn bài, không cào


def test_never_routes_back_to_the_keyword_film(world):
    assert asyncio.run(main._route_post_to_film("Hộ Linh Tráng Sĩ", platform="facebook", exclude_movie_id="b")) is None


def test_reroute_hands_the_post_to_film_b_once(world, monkeypatch):
    handled: list[dict] = []

    async def fake_handle(payload):
        handled.append(payload)

    monkeypatch.setattr(main, "handle_post", fake_handle)
    payload = {"platform": "tiktok", "keyword_id": "kw_a_tiktok"}
    draft = {"content": "#holinhtrangsi", "posted_at": None}
    keyword = {"movie_id": "a"}
    moved = asyncio.run(
        main._reroute_to_other_film(payload, draft, platform="tiktok", post_id="p1", keyword=keyword, why="test")
    )
    assert moved and handled == [{"platform": "tiktok", "keyword_id": "kw_b_tiktok_new", "_rerouted": True}]
    # Đã chuyển một lần thì không chuyển nữa (chống vòng lặp A -> B -> A).
    again = asyncio.run(
        main._reroute_to_other_film(handled[0], draft, platform="tiktok", post_id="p1", keyword=keyword, why="test")
    )
    assert again is False
