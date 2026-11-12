import pytest

from app.core.errors import NotFoundError, UpstreamError
from app.services import press_paging


def _snapshot(organic, press, sources=None):
    """Bản tin trong D1 trả về dict có khoá "data"."""
    return {
        "data": {
            "organic_results": organic,
            "news_results": press,
            "sources": sources if sources is not None else organic + press,
            "search_parameters": {"q": "phim X đánh giá", "gl": "vn", "hl": "vi", "device": "desktop", "start": 0},
            "news_search_parameters": {"q": "phim X review", "gl": "vn", "hl": "vi", "device": "desktop", "start": 0},
        }
    }


@pytest.fixture
def setup(monkeypatch):
    state = {"pages": {}, "calls": []}

    async def load(movie_id):
        return state.get("snapshot")

    async def search_web(payload):
        state["calls"].append(("web", payload.start))
        return state["pages"].get(("web", payload.start), {"organic_results": []})

    async def search(payload):
        state["calls"].append(("news", payload.start))
        return state["pages"].get(("news", payload.start), {"news_results": []})

    monkeypatch.setattr(press_paging.press_reviews, "load", load)
    monkeypatch.setattr(press_paging.google_news, "search_web", search_web)
    monkeypatch.setattr(press_paging.google_news, "search", search)
    return state


async def test_requires_an_existing_snapshot(setup):
    setup["snapshot"] = None
    with pytest.raises(NotFoundError):
        await press_paging.more_pages("film", page=2)


async def test_returns_only_new_articles_and_continues_source_numbering(setup):
    setup["snapshot"] = _snapshot(
        [{"link": "https://a.vn/1", "source_index": 1}],
        [{"link": "https://b.vn/1", "source_index": 2}],
    )
    setup["pages"][("web", 10)] = {"organic_results": [{"link": "https://c.vn/2"}]}
    setup["pages"][("news", 10)] = {"news_results": [{"link": "https://d.vn/2"}]}
    result = await press_paging.more_pages("film", page=2)
    # Chỉ trả phần MỚI, không lặp lại bài của trang đầu đã lưu.
    assert [item["link"] for item in result["organic_results"]] == ["https://c.vn/2"]
    assert [item["link"] for item in result["news_results"]] == ["https://d.vn/2"]
    # Số nguồn tiếp nối bản đang lưu, không đánh số lại từ đầu.
    assert result["organic_results"][0]["source_index"] == 3
    assert result["news_results"][0]["source_index"] == 4


async def test_stops_when_a_page_has_no_next_page(setup):
    setup["snapshot"] = _snapshot([], [])
    setup["pages"][("web", 10)] = {"organic_results": [{"link": "https://c.vn/2"}]}
    result = await press_paging.more_pages("film", page=2)
    # Không có serpapi_pagination.next_link nên dừng sau một trang.
    assert result["pagesRead"] == 1
    assert result["hasMore"] is False
    assert len(result["organic_results"]) == 1
    assert ("web", 20) not in setup["calls"]


async def test_reads_several_pages_while_more_remain(setup):
    setup["snapshot"] = _snapshot([], [])
    setup["pages"][("web", 10)] = {
        "organic_results": [{"link": "https://c.vn/2"}],
        "serpapi_pagination": {"next_link": "x"},
    }
    setup["pages"][("news", 10)] = {
        "news_results": [{"link": "https://d.vn/2"}],
        "serpapi_pagination": {"next_link": "x"},
    }
    setup["pages"][("web", 20)] = {"organic_results": [{"link": "https://c.vn/3"}], "serpapi_pagination": {}}
    setup["pages"][("news", 20)] = {"news_results": [{"link": "https://d.vn/3"}], "serpapi_pagination": {}}
    result = await press_paging.more_pages("film", page=2)
    assert result["pagesRead"] == 2
    assert result["hasMore"] is False
    assert len(result["organic_results"]) == 2


async def test_keeps_has_more_true_while_a_next_page_remains(setup):
    setup["snapshot"] = _snapshot([], [])
    setup["pages"][("web", 10)] = {
        "organic_results": [{"link": "https://c.vn/2"}],
        "serpapi_pagination": {"next_link": "x"},
    }
    # max_pages=1: đọc đúng một trang rồi dừng, còn trang kế tiếp thì hasMore=True.
    result = await press_paging.more_pages("film", page=2, max_pages=1)
    assert result["hasMore"] is True


async def test_duplicates_across_pages_are_kept_once(setup):
    setup["snapshot"] = _snapshot([{"link": "https://a.vn/1", "source_index": 1}], [])
    setup["pages"][("web", 10)] = {"organic_results": [{"link": "https://a.vn/1#section"}, {"link": "https://c.vn/2"}]}
    result = await press_paging.more_pages("film", page=2)
    assert [item["link"] for item in result["organic_results"]] == ["https://c.vn/2"]


async def test_source_offset_continues_numbering_across_calls(setup):
    setup["snapshot"] = _snapshot([], [])
    setup["pages"][("web", 10)] = {"organic_results": [{"link": "https://c.vn/2"}]}
    result = await press_paging.more_pages("film", page=2, max_pages=1, source_offset=5)
    # Số nguồn tiếp nối phần đã giao ở lượt trước (5 bài) cộng bản tin đang lưu.
    assert result["organic_results"][0]["source_index"] == 6


async def test_snapshot_without_saved_queries_is_rejected(setup):
    setup["snapshot"] = {"data": {"organic_results": [], "news_results": [], "sources": []}}
    with pytest.raises(UpstreamError):
        await press_paging.more_pages("film", page=2)
