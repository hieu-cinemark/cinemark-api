"""Trailerix (trailerix.com) (25/11/2026): metadata phim (poster, diễn viên, thể loại, mô tả) + trailer YouTube chính theo IMDb ID.
Cần key (Bearer) - miễn phí, đăng ký bằng email + URL website; điều khoản yêu cầu hiển thị link ghi nguồn Trailerix. Quyền dùng
thương mại và nguồn gốc metadata chưa xác minh.
"""

from __future__ import annotations

import re
from typing import Any

import httpx

from app.core.config import settings
from app.core.errors import AppError, NotFoundError, UpstreamError

BASE_URL = "https://trailerix.com/api/v1"
TIMEOUT_SECONDS = 20.0
ATTRIBUTION = "Data from Trailerix (trailerix.com)"
_IMDB_ID = re.compile(r"^tt\d{5,10}$")


class TrailerixNotConfigured(AppError):
    status_code = 503
    code = "trailerix_not_configured"


def configured() -> bool:
    return bool((settings.trailerix_api_key or "").strip())


async def movie_by_imdb(imdb_id: str) -> dict[str, Any] | None:
    if not _IMDB_ID.match(imdb_id):
        raise NotFoundError("IMDb id must look like tt1234567")
    key = (settings.trailerix_api_key or "").strip()
    if not key:
        raise TrailerixNotConfigured("TRAILERIX_API_KEY is not configured")
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SECONDS)) as client:
            response = await client.get(f"{BASE_URL}/movie/imdb/{imdb_id}", headers={"Authorization": f"Bearer {key}"})
    except httpx.TimeoutException:
        raise UpstreamError("Trailerix timed out") from None
    except httpx.HTTPError:
        raise UpstreamError("Could not reach Trailerix") from None
    if response.status_code == 404:
        return None
    if response.status_code in (401, 403):
        raise UpstreamError("Trailerix rejected the API key")
    if response.status_code == 429:
        raise UpstreamError("Trailerix is rate limiting requests, try again shortly")
    if response.status_code >= 400:
        raise UpstreamError(f"Trailerix returned HTTP {response.status_code}")
    try:
        return response.json()
    except ValueError:
        raise UpstreamError("Trailerix returned an invalid response") from None
