from copy import deepcopy

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes.research import router
from app.core.config import settings
from app.core.errors import UpstreamError, register_exception_handlers
from app.schemas.research import GoogleNewsRequest
from app.services import google_news

RESULT = {
    "search_metadata": {"status": "Success", "id": "search-1"},
    "search_parameters": {"q": "Phim Út Lan 2", "engine": "google_news_light"},
    "news_results": [
        {
            "position": 1,
            "title": "Review",
            "link": "https://example.com/review",
            "source": "Báo",
            "snippet": "Nhận xét",
            "date": "1 day ago",
        }
    ],
    "serpapi_pagination": {"current": 1, "next": "https://serpapi.com/search?start=10"},
}


@pytest.fixture
def upstream(monkeypatch):
    monkeypatch.setattr(settings, "serpapi_api_key", "test-secret")
    observed = []
    reply = {"body": deepcopy(RESULT), "status": 200, "error": None}

    def handle(request):
        observed.append(request)
        if reply["error"]:
            raise reply["error"]
        return httpx.Response(reply["status"], json=reply["body"])

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        google_news.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handle), **kwargs),
    )
    return reply, observed


def client():
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    return TestClient(app)


def test_route_preserves_nested_results_and_sends_fixed_engine(upstream):
    response = client().post("/research/google-news", json={"q": " Phim Út Lan 2 "})
    assert response.status_code == 200
    assert response.json() == RESULT
    request = upstream[1][0]
    assert str(request.url).startswith("https://serpapi.com/search?")
    assert dict(request.url.params) == {
        "q": "Phim Út Lan 2",
        "gl": "vn",
        "hl": "vi",
        "device": "desktop",
        "no_cache": "false",
        "start": "0",
        "engine": "google_news_light",
        "api_key": "test-secret",
    }


async def test_pagination_and_localization(upstream):
    await google_news.search(
        GoogleNewsRequest(
            q="Doanh thu?",
            gl="us",
            hl="en",
            location="Austin, Texas",
            start=10,
            no_cache=True,
        )
    )
    params = upstream[1][0].url.params
    assert params["start"] == "10"
    assert params["location"] == "Austin, Texas"
    assert params["gl"] == "us" and params["hl"] == "en" and params["no_cache"] == "true"


def test_missing_key_is_503_without_http(monkeypatch, upstream):
    monkeypatch.setattr(settings, "serpapi_api_key", " ")
    response = client().post("/research/google-news", json={"q": "Phim"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "serpapi_not_configured"
    assert not upstream[1]


@pytest.mark.parametrize("body", [{"q": " "}, {"q": "Phim", "api_key": "client-key"}, {"q": "Phim", "engine": "other"}])
def test_invalid_input_does_not_call_provider(body, upstream):
    assert client().post("/research/google-news", json=body).status_code == 422
    assert not upstream[1]


@pytest.mark.parametrize("status", [401, 429, 500])
def test_http_errors_are_generic_without_retry_or_key(status, upstream):
    upstream[0].update(status=status, body={"error": "secret upstream details test-secret"})
    response = client().post("/research/google-news", json={"q": "Phim"})
    assert response.status_code == 502
    assert "test-secret" not in response.text
    assert len(upstream[1]) == 1


@pytest.mark.parametrize(
    "body", [{"error": "test-secret"}, [], {"search_metadata": None}, {"search_metadata": {"status": "Processing"}}]
)
async def test_unsuccessful_or_malformed_body(upstream, body):
    upstream[0]["body"] = body
    with pytest.raises(UpstreamError):
        await google_news.search(GoogleNewsRequest(q="Phim"))


async def test_timeout_does_not_retry_or_expose_key(upstream):
    upstream[0]["error"] = httpx.ReadTimeout("https://serpapi.com/search?api_key=test-secret")
    with pytest.raises(UpstreamError) as exc:
        await google_news.search(GoogleNewsRequest(q="Phim"))
    assert "test-secret" not in str(exc.value)
    assert len(upstream[1]) == 1


async def test_echoed_key_is_removed(upstream):
    upstream[0]["body"]["search_parameters"]["api_key"] = "test-secret"
    result = await google_news.search(GoogleNewsRequest(q="Phim"))
    assert "api_key" not in result["search_parameters"]


async def test_httpx_request_log_redacts_key(upstream, caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="httpx"):
        await google_news.search(GoogleNewsRequest(q="Phim"))
    assert "test-secret" not in caplog.text
    assert "api_key=[REDACTED]" in caplog.text
