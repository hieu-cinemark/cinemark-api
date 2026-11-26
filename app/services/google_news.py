"""Tìm tin Google cho "Góc nhìn sau công chiếu", giữ dạng kết quả của SerpApi (news_results / organic_results,
serpapi_pagination) để press_groups / press_paging không phải đổi.

- search (Google News): mặc định Google News RSS miễn phí (app/clients/google.py, settings.google_news_provider="rss"),
  10 bài mỗi trang, link đã giải về bài gốc; "serpapi" = Google News Light qua SerpApi như cũ.
- search_web (kết quả web): mặc định SearXNG tự host (settings.web_search_provider="searxng", settings.searxng_url) - gom
  Google/Bing/Brave..., trả về cùng dạng organic_results của SerpApi; "serpapi" = Google Search qua SerpApi như cũ.
"""

import logging
import re
from typing import Any

import httpx

from app.clients import google as google_free
from app.core.config import settings
from app.core.errors import AppError, UpstreamError
from app.schemas.research import GoogleNewsRequest

SEARCH_URL = "https://serpapi.com/search"


class _RedactSerpApiKey(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "serpapi.com" in message and "api_key=" in message:
            record.msg = re.sub(r"api_key=[^&\s\"']+", "api_key=[REDACTED]", message)
            record.args = ()
        return True


# httpx tự log URL ở INFO, kể cả khi ứng dụng không log request.
logging.getLogger("httpx").addFilter(_RedactSerpApiKey())


class SerpApiNotConfigured(AppError):
    status_code = 503
    code = "serpapi_not_configured"


RSS_PAGE_SIZE = 10


async def search(payload: GoogleNewsRequest) -> dict[str, Any]:
    if settings.google_news_provider == "rss":
        return await search_rss(payload)
    return await _search(payload, engine="google_news_light")


def web_search_available() -> bool:
    if settings.web_search_provider == "searxng":
        return bool((settings.searxng_url or "").strip())
    return bool((settings.serpapi_api_key or "").strip())


async def search_rss(payload: GoogleNewsRequest) -> dict[str, Any]:
    """Một trang (payload.start) của Google News RSS, dạng như Google News Light của SerpApi. RSS trả cả lô (~100 bài)
    một lần nên mỗi trang đọc lại RSS rồi cắt; chỉ giải link gốc cho 10 bài của trang."""
    items = await google_free.news_search(payload.q, hl=payload.hl, gl=payload.gl)
    page = items[payload.start : payload.start + RSS_PAGE_SIZE]
    await google_free.resolve_links(page)
    results = [
        {
            "position": payload.start + i + 1,
            "title": item.get("title"),
            "link": item.get("link"),
            "google_link": item.get("google_link"),
            "source": item.get("source"),
            "source_url": item.get("source_url"),
            "iso_date": item.get("published_at"),
            "date": item.get("published_at"),
        }
        for i, item in enumerate(page)
        if item.get("link")
    ]
    data: dict[str, Any] = {
        "search_metadata": {"status": "Success", "provider": "google_news_rss"},
        "search_parameters": {"engine": "google_news_rss", "q": payload.q, "gl": payload.gl, "hl": payload.hl},
        "news_results": results,
    }
    if payload.start + RSS_PAGE_SIZE < len(items):
        data["serpapi_pagination"] = {"next_link": f"rss:start={payload.start + RSS_PAGE_SIZE}"}
    return data


async def search_web(payload: GoogleNewsRequest) -> dict[str, Any]:
    if settings.web_search_provider == "searxng":
        return await search_searxng(payload)
    return await _search(payload, engine="google")


SEARXNG_PAGE_SIZE = 10
# SearXNG lật được tới khoảng trang này; xa hơn các công cụ nguồn gần như không trả thêm.
SEARXNG_MAX_PAGE = 10


async def search_searxng(payload: GoogleNewsRequest) -> dict[str, Any]:
    """Một trang kết quả web từ SearXNG tự host (GET /search?format=json), đổi sang dạng organic_results của SerpApi.
    payload.start (bội số của 10, như SerpApi) -> pageno. SearXNG gộp nhiều công cụ nên một trang có thể hơn 10 kết quả;
    các trang chồng nhau thì press_paging bỏ trùng theo link."""
    base = (settings.searxng_url or "").strip().rstrip("/")
    if not base:
        raise UpstreamError("SEARXNG_URL is not configured")
    pageno = payload.start // SEARXNG_PAGE_SIZE + 1
    params = {"q": payload.q, "format": "json", "language": payload.hl, "categories": "general", "pageno": pageno}
    try:
        async with httpx.AsyncClient(timeout=settings.searxng_timeout_seconds) as client:
            response = await client.get(f"{base}/search", params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.TimeoutException:
        raise UpstreamError("SearXNG search timed out") from None
    except httpx.HTTPError, ValueError:
        raise UpstreamError("Could not call SearXNG search") from None
    rows = data.get("results") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise UpstreamError("SearXNG returned an unexpected result")
    results = []
    for row in rows:
        link = row.get("url") if isinstance(row, dict) else None
        if not link:
            continue
        host = httpx.URL(link).host.removeprefix("www.") if link.startswith("http") else ""
        results.append(
            {
                "position": payload.start + len(results) + 1,
                "title": row.get("title") or "",
                "link": link,
                "snippet": row.get("content") or "",
                "displayed_link": host,
                "source": host,
                "thumbnail": row.get("thumbnail") or row.get("img_src") or None,
                "date": row.get("publishedDate") or None,
                "engines": row.get("engines") or [],
            }
        )
    out: dict[str, Any] = {
        "search_metadata": {"status": "Success", "provider": "searxng"},
        "search_parameters": {"engine": "searxng", "q": payload.q, "gl": payload.gl, "hl": payload.hl},
        "organic_results": results,
    }
    if results and pageno < SEARXNG_MAX_PAGE:
        out["serpapi_pagination"] = {"next_link": f"searxng:pageno={pageno + 1}"}
    unresponsive = data.get("unresponsive_engines") or []
    if unresponsive:
        out["search_metadata"]["unresponsive_engines"] = [item[0] for item in unresponsive if item][:10]
    return out


async def _search(payload: GoogleNewsRequest, *, engine: str) -> dict[str, Any]:
    key = (settings.serpapi_api_key or "").strip()
    if not key:
        raise SerpApiNotConfigured("SERPAPI_API_KEY is not configured")
    params = payload.model_dump(exclude_none=True)
    params.update(engine=engine, api_key=key)
    params["no_cache"] = "true" if payload.no_cache else "false"
    # Không retry tự động: mỗi request có thể dùng quota. Không log URL/exception
    # httpx vì query string có chứa api_key.
    try:
        async with httpx.AsyncClient(timeout=settings.serpapi_timeout_seconds, follow_redirects=False) as client:
            response = await client.get(SEARCH_URL, params=params)
            response.raise_for_status()
    except httpx.TimeoutException:
        raise UpstreamError("SerpApi Google Search timed out") from None
    except httpx.HTTPError:
        raise UpstreamError("Could not call SerpApi Google Search") from None
    try:
        data = response.json()
    except ValueError:
        raise UpstreamError("SerpApi returned invalid JSON") from None
    metadata = data.get("search_metadata") if isinstance(data, dict) else None
    if (
        not isinstance(data, dict)
        or data.get("error")
        or not isinstance(metadata, dict)
        or metadata.get("status") != "Success"
    ):
        raise UpstreamError("SerpApi did not return a successful Google News Light result")
    # Giữ danh sách tin và metadata/pagination do SerpApi trả về.
    # Không trả lại credential nếu nhà cung cấp echo nó trong search_parameters.
    if isinstance(data.get("search_parameters"), dict):
        data["search_parameters"].pop("api_key", None)
    return data
