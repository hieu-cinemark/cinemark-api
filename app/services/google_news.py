"""Tìm tin Google cho "Góc nhìn sau công chiếu", giữ dạng kết quả của SerpApi (news_results / organic_results,
serpapi_pagination) để press_groups / press_paging không phải đổi.

- search (Google News): mặc định Google News RSS miễn phí (app/clients/google.py, settings.google_news_provider="rss"),
  10 bài mỗi trang, link đã giải về bài gốc; "serpapi" = Google News Light qua SerpApi như cũ.
- search_web (Google Search): vẫn SerpApi - trang kết quả Google cần trình duyệt + giải captcha.
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
    return await _search(payload, engine="google")


async def _search(payload: GoogleNewsRequest, *, engine: str) -> dict[str, Any]:
    key = (settings.serpapi_api_key or "").strip()
    if not key:
        raise SerpApiNotConfigured("Chưa cấu hình SERPAPI_API_KEY.")
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
        raise UpstreamError("SerpApi Google Search hết thời gian chờ.") from None
    except httpx.HTTPError:
        raise UpstreamError("Không gọi được SerpApi Google Search.") from None
    try:
        data = response.json()
    except ValueError:
        raise UpstreamError("SerpApi trả về JSON không hợp lệ.") from None
    metadata = data.get("search_metadata") if isinstance(data, dict) else None
    if (
        not isinstance(data, dict)
        or data.get("error")
        or not isinstance(metadata, dict)
        or metadata.get("status") != "Success"
    ):
        raise UpstreamError("SerpApi chưa trả về kết quả Google News Light thành công.")
    # Giữ danh sách tin và metadata/pagination do SerpApi trả về.
    # Không trả lại credential nếu nhà cung cấp echo nó trong search_parameters.
    if isinstance(data.get("search_parameters"), dict):
        data["search_parameters"].pop("api_key", None)
    return data
