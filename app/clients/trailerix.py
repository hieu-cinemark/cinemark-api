"""Trailerix (trailerix.com) (25/11/2026): metadata phim (poster, diễn viên, thể loại, mô tả) + trailer YouTube chính theo IMDb ID.
Cần key (Bearer) - miễn phí, đăng ký bằng email + URL website; key chỉ còn hiệu lực khi trang có link "Fresh trailers by
Trailerix" (cinemark apps/web/index.html + tab Trailerix). Quyền dùng thương mại và nguồn gốc metadata chưa xác minh.

Key chỉ lưu ở cinemark-config-db (bảng external_api_keys, name="trailerix") - không đọc .env. Đổi key:
    python -m scripts.tools.set_external_api_key trailerix <key>
"""

from __future__ import annotations

import re
import time
from typing import Any

import httpx

from app.core.errors import AppError, NotFoundError, UpstreamError

BASE_URL = "https://trailerix.com/api/v1"
TIMEOUT_SECONDS = 20.0
ATTRIBUTION = "Data from Trailerix (trailerix.com)"
_IMDB_ID = re.compile(r"^tt\d{5,10}$")


class TrailerixNotConfigured(AppError):
    status_code = 503
    code = "trailerix_not_configured"


KEY_NAME = "trailerix"
_CACHE_SECONDS = 60
_cache: tuple[float, str] | None = None


async def api_key() -> str:
    """Key từ cinemark-config-db (cache 60 giây)."""
    global _cache
    now = time.monotonic()
    if _cache and now - _cache[0] < _CACHE_SECONDS:
        return _cache[1]
    from app.services.accounts.config_db.d1_ops import get_external_api_key

    try:
        key = (await get_external_api_key(KEY_NAME) or "").strip()
    except Exception:  # noqa: BLE001 - DB lỗi: coi như chưa cấu hình, không chặn route khác
        key = ""
    _cache = (now, key)
    return key


def forget_key() -> None:
    global _cache
    _cache = None


async def configured() -> bool:
    return bool(await api_key())


async def movie_by_imdb(imdb_id: str) -> dict[str, Any] | None:
    if not _IMDB_ID.match(imdb_id):
        raise NotFoundError("IMDb id must look like tt1234567")
    key = await api_key()
    if not key:
        raise TrailerixNotConfigured("Trailerix API key is not configured (external_api_keys in cinemark-config-db)")
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
