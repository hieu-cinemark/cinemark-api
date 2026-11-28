"""Trang chi tiết Trakt: rút gọn diễn viên / bình luận, phần phụ lỗi thì để trống, thiếu phim thì 404."""

import pytest

from app.clients import trakt, trakt_detail
from app.core.errors import NotFoundError, UpstreamError


def test_people_of_keeps_names_roles_headshots_and_crew():
    raw = {
        "cast": [
            {"characters": ["Mia"], "person": {"name": "A", "ids": {"trakt": 1}, "images": {"headshot": ["h.webp"]}}},
            {"character": "B role", "person": {"name": "B", "ids": {"trakt": 2}, "images": {}}},
            {"person": {"ids": {"trakt": 3}}},
        ],
        "crew": {
            "directing": [{"jobs": ["Director"], "person": {"name": "D"}}, {"jobs": ["Assistant Director"], "person": {"name": "X"}}],
            "writing": [{"job": "Screenplay", "person": {"name": "W"}}, {"jobs": ["Writer"], "person": {"name": "W"}}],
        },
    }
    result = trakt_detail.people_of(raw)
    assert [(p["name"], p["character"], p["headshot"]) for p in result["cast"]] == [("A", "Mia", "h.webp"), ("B", "B role", None)]
    assert result["directors"] == ["D"]
    assert result["writers"] == ["W"]
    assert trakt_detail.people_of(None) == {"cast": [], "directors": [], "writers": []}


def test_comments_of_drops_spoilers_and_profile_data():
    raw = [
        {"id": 1, "comment": "hay", "spoiler": False, "likes": 3, "user_rating": 8, "user": {"username": "u", "location": "secret", "age": 30}},
        {"id": 2, "comment": "spoil", "spoiler": True, "user": {"username": "v"}},
    ]
    result = trakt_detail.comments_of(raw)
    assert [c["id"] for c in result] == [1]
    assert set(result[0]) == {"id", "comment", "created_at", "likes", "rating", "language", "author"}


@pytest.mark.asyncio
async def test_detail_survives_failing_optional_parts(monkeypatch):
    async def movie(_id):
        return {"title": "T"}

    async def get(path, params=None):
        if path.endswith("/people"):
            return {"cast": [{"characters": ["R"], "person": {"name": "A", "ids": {}}}]}
        raise UpstreamError("Trakt returned HTTP 500")

    monkeypatch.setattr(trakt, "movie", movie)
    monkeypatch.setattr(trakt, "_get", get)
    result = await trakt_detail.detail("t")
    assert result["movie"] == {"title": "T"}
    assert result["people"]["cast"][0]["name"] == "A"
    assert result["ratings"] == {} and result["comments"] == [] and result["videos"] == [] and result["related"] == []


@pytest.mark.asyncio
async def test_detail_missing_movie_is_not_found(monkeypatch):
    async def movie(_id):
        return None

    monkeypatch.setattr(trakt, "movie", movie)
    with pytest.raises(NotFoundError):
        await trakt_detail.detail("missing")
