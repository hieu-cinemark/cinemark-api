import pytest

from app.clients import external_keys, tmdb, trakt, wikidata
from app.core.errors import NotFoundError


async def test_tmdb_and_trakt_require_keys(monkeypatch):
    async def no_key(name):
        return ""

    monkeypatch.setattr(external_keys, "get_key", no_key)
    with pytest.raises(tmdb.TmdbNotConfigured):
        await tmdb.movie_list("now_playing")
    with pytest.raises(trakt.TraktNotConfigured):
        await trakt.box_office()


async def test_tmdb_validates_inputs():
    with pytest.raises(NotFoundError):
        await tmdb.movie_list("not_a_list")


async def test_tmdb_resolves_imdb_ids_through_find(monkeypatch):
    calls = []

    async def fake_get(path, params=None):
        calls.append(path)
        if path.startswith("/find/"):
            return {"movie_results": [{"id": 1591972}]}
        return {"id": 1591972, "title": "Đại Tiệc Trăng Máu 8"}

    monkeypatch.setattr(tmdb, "_get", fake_get)
    movie = await tmdb.movie("tt38934461")
    assert movie["title"] == "Đại Tiệc Trăng Máu 8"
    assert calls == ["/find/tt38934461", "/movie/1591972"]


async def test_wikidata_details_are_mapped_and_split(monkeypatch):
    async def fake_sparql(query):
        assert "wd:Q1" in query
        return [
            {
                "m": "http://www.wikidata.org/entity/Q1",
                "mLabel": "Phim A",
                "released": "2026-04-30T00:00:00Z",
                "duration": "118",
                "imdb": "tt1",
                "directors": "Đạo diễn 1|Đạo diễn 2",
                "cast": "",
                "genres": "kinh dị",
            }
        ]

    monkeypatch.setattr(wikidata, "sparql", fake_sparql)
    [item] = await wikidata.movie_details(["Q1"], lang="vi,en")
    assert item["wikidata_id"] == "Q1" and item["released_at"] == "2026-04-30" and item["duration_minutes"] == 118
    assert item["directors"] == ["Đạo diễn 1", "Đạo diễn 2"] and item["cast"] == [] and item["genres"] == ["kinh dị"]
    assert item["tmdb_id"] is None


async def test_wikidata_list_keeps_page_order(monkeypatch):
    async def fake_sparql(query):
        return [{"m": "http://www.wikidata.org/entity/Q2"}, {"m": "http://www.wikidata.org/entity/Q1"}]

    async def fake_details(ids, lang):
        return [{"wikidata_id": i} for i in sorted(ids)]

    monkeypatch.setattr(wikidata, "sparql", fake_sparql)
    monkeypatch.setattr(wikidata, "movie_details", fake_details)
    items = await wikidata.movies_by_country("VN", year_from=2025, year_to=None, limit=2, offset=0, lang="vi")
    assert [i["wikidata_id"] for i in items] == ["Q2", "Q1"]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("tt4154796", "haswbstatement:P345=tt4154796 haswbstatement:P31=Q11424"),
        ("Mission: Impossible", "Mission  Impossible haswbstatement:P31=Q11424"),
    ],
)
async def test_wikidata_search_builds_a_film_only_query(monkeypatch, query, expected):
    seen = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"query": {"search": [{"title": "Q1"}, {"title": "Property:P31"}]}}

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, params=None, headers=None):
            seen.update(params)
            return FakeResponse()

    monkeypatch.setattr(wikidata.httpx, "AsyncClient", FakeClient)
    assert await wikidata.search_ids(query, 5) == ["Q1"]
    assert seen["srsearch"] == expected
