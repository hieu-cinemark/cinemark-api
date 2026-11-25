"""KinoCheck (api.kinocheck.com), không key (25/11/2026): trailer / teaser / clip chính thức của hãng phim, kèm IMDb / TMDB ID và
ID video YouTube. Truy vấn cơ bản không cần key; mặc định trả bản tiếng Đức, gửi language=en để lấy bản tiếng Anh (chỉ có en / de).
Chưa có phim Việt (thử các IMDb ID phim Việt 2026 đều "movie not found").

Danh sách (/trailers, /trailers/latest, /trailers/trending) trả object khoá "0", "1"... kèm "_metadata" (limit, page,
total_pages, total_count) - ở đây đổi thành {items, metadata}. Chỉ cho các đường dẫn trong ALLOWED_PATHS để không thành proxy mở.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx

from app.core.errors import NotFoundError, UpstreamError

BASE_URL = "https://api.kinocheck.com"
TIMEOUT_SECONDS = 20.0
ALLOWED_PATHS = re.compile(r"^(movies|shows|trailers|trailers/latest|trailers/trending)$")
ALLOWED_PARAMS = {"imdb_id", "tmdb_id", "id", "language", "categories", "genres", "limit", "page"}

_slots = asyncio.Semaphore(4)


def _list_shape(data: dict[str, Any]) -> dict[str, Any]:
    """{"0": {...}, "1": {...}, "_metadata": {...}} -> {"items": [...], "metadata": {...}}."""
    items = [
        value
        for key, value in sorted(data.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else -1)
        if key.isdigit()
    ]
    return {"items": items, "metadata": data.get("_metadata") or {}}


async def get(path: str, params: dict[str, Any]) -> Any:
    path = path.strip("/")
    if not ALLOWED_PATHS.match(path):
        raise NotFoundError(f"KinoCheck path '{path}' is not supported")
    query = {key: value for key, value in params.items() if key in ALLOWED_PARAMS}
    query.setdefault("language", "en")
    async with _slots:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SECONDS), base_url=BASE_URL) as client:
                response = await client.get(f"/{path}", params=query, headers={"Accept": "application/json"})
        except httpx.TimeoutException:
            raise UpstreamError("KinoCheck timed out") from None
        except httpx.HTTPError:
            raise UpstreamError("Could not reach KinoCheck") from None
    if response.status_code == 429:
        raise UpstreamError("KinoCheck is rate limiting requests, try again shortly")
    try:
        data = response.json()
    except ValueError:
        raise UpstreamError("KinoCheck returned an invalid response") from None
    # Không thấy phim: KinoCheck trả HTTP 200 kèm {"error": ..., "message": "movie not found"}.
    if isinstance(data, dict) and data.get("error"):
        if "not found" in str(data.get("message", "")).lower():
            return None
        raise UpstreamError(f"KinoCheck error: {data.get('message') or data.get('error')}")
    if response.status_code >= 400:
        raise UpstreamError(f"KinoCheck returned HTTP {response.status_code}")
    if path.startswith("trailers") and isinstance(data, dict):
        return _list_shape(data)
    return data
