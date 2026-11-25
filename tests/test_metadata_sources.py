import pytest

from app.clients import kinocheck, trailerix, wikidata
from app.core.errors import NotFoundError


@pytest.mark.parametrize("path", ["movies", "shows", "trailers", "trailers/latest", "trailers/trending"])
def test_kinocheck_allows_documented_paths(path):
    assert kinocheck.ALLOWED_PATHS.match(path)


@pytest.mark.parametrize("path", ["", "movies/../x", "http://evil", "trailers/other", "users"])
async def test_kinocheck_rejects_other_paths(path):
    with pytest.raises(NotFoundError):
        await kinocheck.get(path, {})


def test_kinocheck_list_shape_orders_numeric_keys():
    data = {"1": {"id": "b"}, "0": {"id": "a"}, "10": {"id": "k"}, "_metadata": {"page": 1}}
    assert kinocheck._list_shape(data) == {"items": [{"id": "a"}, {"id": "b"}, {"id": "k"}], "metadata": {"page": 1}}


async def test_trailerix_requires_a_key(monkeypatch):
    monkeypatch.setattr(trailerix.settings, "trailerix_api_key", "")
    with pytest.raises(trailerix.TrailerixNotConfigured):
        await trailerix.movie_by_imdb("tt0499549")
    with pytest.raises(NotFoundError):
        await trailerix.movie_by_imdb("not-an-id")


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
