"""OMDb (app/clients/omdb.py): chuẩn hoá điểm, "N/A" thành null, ID sai bị chặn, lỗi hết lượt rõ ràng."""

import pytest

from app.clients import omdb
from app.core.errors import NotFoundError, UpstreamError

FULL = {
    "Response": "True",
    "imdbID": "tt3896198",
    "Title": "Guardians of the Galaxy Vol. 2",
    "Year": "2017",
    "imdbRating": "7.6",
    "imdbVotes": "849,165",
    "Metascore": "67",
    "BoxOffice": "$389,813,101",
    "Ratings": [
        {"Source": "Internet Movie Database", "Value": "7.6/10"},
        {"Source": "Rotten Tomatoes", "Value": "85%"},
    ],
}


def test_normalize_full():
    out = omdb.normalize(FULL)
    assert out["ratings"]["imdb"] == {"value": 7.6, "votes": 849165}
    assert out["ratings"]["rotten_tomatoes"] == {"value": 85, "text": "85%"}
    assert out["ratings"]["metacritic"] == {"value": 67}
    assert out["box_office"] == "$389,813,101"


def test_normalize_vietnamese_without_votes():
    out = omdb.normalize(
        {
            "Response": "True",
            "imdbID": "tt46658565",
            "Title": "Trai Giam Hanh Phuc",
            "imdbRating": "N/A",
            "imdbVotes": "N/A",
            "Metascore": "N/A",
            "Ratings": [],
        }
    )
    assert out["ratings"] == {"imdb": None, "rotten_tomatoes": None, "metacritic": None}


class _Redis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = value


@pytest.fixture
def fake(monkeypatch):
    redis = _Redis()
    calls = []
    reply = {"body": FULL}

    async def get_json(source, url, params=None, headers=None):
        calls.append(params)
        return reply["body"]

    async def get_key(name):
        return "k"

    monkeypatch.setattr(omdb, "get_redis_client", lambda: redis)
    monkeypatch.setattr(omdb, "get_json", get_json)
    monkeypatch.setattr(omdb.external_keys, "get_key", get_key)
    return reply, calls


async def test_cached_after_first_call(fake):
    await omdb.ratings("tt3896198")
    await omdb.ratings("TT3896198")
    assert len(fake[1]) == 1


async def test_bad_id_rejected(fake):
    with pytest.raises(NotFoundError):
        await omdb.ratings("Mai")


async def test_limit_error(fake):
    fake[0]["body"] = {"Response": "False", "Error": "Request limit reached!"}
    with pytest.raises(UpstreamError):
        await omdb.ratings("tt0000001")
