"""Gọi GET tới API ngoài (Trakt, TMDB...) với cùng cách xử lý lỗi: hết giờ / mất mạng / 401 / 429 / 4xx-5xx -> lỗi rõ ràng,
404 -> None."""

from __future__ import annotations

from typing import Any

import httpx

from app.core.errors import UpstreamError

TIMEOUT_SECONDS = 20.0


async def get_json(
    source: str, url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None
) -> Any:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SECONDS)) as client:
            response = await client.get(url, params=params, headers=headers)
    except httpx.TimeoutException:
        raise UpstreamError(f"{source} timed out") from None
    except httpx.HTTPError:
        raise UpstreamError(f"Could not reach {source}") from None
    if response.status_code == 404:
        return None
    if response.status_code in (401, 403):
        raise UpstreamError(f"{source} rejected the API key")
    if response.status_code == 429:
        raise UpstreamError(f"{source} is rate limiting requests, try again shortly")
    if response.status_code >= 400:
        raise UpstreamError(f"{source} returned HTTP {response.status_code}")
    try:
        return response.json()
    except ValueError:
        raise UpstreamError(f"{source} returned an invalid response") from None
