"""Kết quả web từ SearXNG tự host (google_news.search_searxng) - 26/11/2026 thay SerpApi cho phần Google Search."""

import httpx
import pytest

from app.core.config import settings
from app.core.errors import UpstreamError
from app.schemas.research import GoogleNewsRequest
from app.services import google_news

REPLY = {
    "results": [
        {
            "url": "https://www.cgv.vn/phim/abc",
            "title": "Phim ABC - CGV",
            "content": "Lịch chiếu",
            "engines": ["google"],
        },
        {"title": "không có link"},
        {
            "url": "https://kenh14.vn/review",
            "title": "Review",
            "content": "",
            "thumbnail": "https://img/x.jpg",
            "engines": ["bing"],
        },
    ],
    "unresponsive_engines": [["duckduckgo", "CAPTCHA"]],
}


@pytest.fixture
def searx(monkeypatch):
    monkeypatch.setattr(settings, "web_search_provider", "searxng")
    monkeypatch.setattr(settings, "searxng_url", "http://searx.local/")
    observed: list[httpx.Request] = []
    reply = {"status": 200, "body": REPLY}

    def handle(request: httpx.Request) -> httpx.Response:
        observed.append(request)
        return httpx.Response(reply["status"], json=reply["body"])

    real = httpx.AsyncClient
    monkeypatch.setattr(
        google_news.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handle), **kw)
    )
    return reply, observed


async def test_maps_to_serpapi_shape(searx):
    data = await google_news.search_web(GoogleNewsRequest(q="phim ABC đánh giá", start=20))
    request = searx[1][0]
    assert str(request.url).startswith("http://searx.local/search?")
    assert request.url.params["format"] == "json"
    assert request.url.params["pageno"] == "3"
    assert request.url.params["language"] == "vi"
    rows = data["organic_results"]
    assert [row["link"] for row in rows] == ["https://www.cgv.vn/phim/abc", "https://kenh14.vn/review"]
    assert rows[0]["position"] == 21 and rows[0]["source"] == "cgv.vn" and rows[0]["snippet"] == "Lịch chiếu"
    assert rows[1]["thumbnail"] == "https://img/x.jpg"
    assert data["search_parameters"]["q"] == "phim ABC đánh giá"
    assert data["serpapi_pagination"]["next_link"]
    assert data["search_metadata"]["unresponsive_engines"] == ["duckduckgo"]


async def test_no_results_means_no_next_page(searx):
    searx[0]["body"] = {"results": []}
    data = await google_news.search_web(GoogleNewsRequest(q="x"))
    assert data["organic_results"] == [] and "serpapi_pagination" not in data


async def test_http_error_is_upstream_error(searx):
    searx[0]["status"] = 500
    with pytest.raises(UpstreamError):
        await google_news.search_web(GoogleNewsRequest(q="x"))


def test_available_without_serpapi_key(monkeypatch):
    monkeypatch.setattr(settings, "web_search_provider", "searxng")
    monkeypatch.setattr(settings, "searxng_url", "http://127.0.0.1:8888")
    monkeypatch.setattr(settings, "serpapi_api_key", None)
    assert google_news.web_search_available()
