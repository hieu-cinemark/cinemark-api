"""Trakt (api.trakt.tv) (25/11/2026): phim trending, phòng vé Mỹ cuối tuần, lịch phát hành, chi tiết phim (kèm link trailer).
Cần client id (header trakt-api-key) lưu ở cinemark-config-db (external_api_keys, name="trakt"). Lịch "my" cần OAuth của
một tài khoản nên không dùng. Phim Việt rất ít (lọc countries=vn rỗng, tìm tên tiếng Việt ra sai); tra theo IMDb ID thì có
một số phim (tên tiếng Anh)."""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from app.clients import external_keys
from app.clients.external_http import get_json
from app.core.errors import AppError, NotFoundError

BASE_URL = "https://api.trakt.tv"
KEY_NAME = "trakt"
ATTRIBUTION = "Data from Trakt (trakt.tv)"
_ID = re.compile(r"^(tt\d{5,10}|\d+|[a-z0-9-]{2,120})$")


class TraktNotConfigured(AppError):
    status_code = 503
    code = "trakt_not_configured"


async def configured() -> bool:
    return bool(await external_keys.get_key(KEY_NAME))


async def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    key = await external_keys.get_key(KEY_NAME)
    if not key:
        raise TraktNotConfigured("Trakt API key is not configured (external_api_keys in cinemark-config-db)")
    headers = {"Content-Type": "application/json", "trakt-api-version": "2", "trakt-api-key": key}
    return await get_json("Trakt", f"{BASE_URL}{path}", params=params, headers=headers)


async def trending(page: int = 1, limit: int = 24) -> list[dict[str, Any]]:
    """[{watchers, movie}] - phim đang được xem nhiều nhất trên Trakt."""
    return await _get("/movies/trending", {"page": page, "limit": limit, "extended": "full"}) or []


async def box_office() -> list[dict[str, Any]]:
    """[{revenue, movie}] - top 10 phòng vé Mỹ cuối tuần gần nhất."""
    return await _get("/movies/boxoffice", {"extended": "full"}) or []


async def calendar(start: date, days: int = 7) -> list[dict[str, Any]]:
    """[{released, movie}] - phim phát hành trong `days` ngày từ `start` (lịch chung của Trakt)."""
    return await _get(f"/calendars/all/movies/{start.isoformat()}/{min(max(days, 1), 31)}", {"extended": "full"}) or []


async def search(query: str, limit: int = 20) -> list[dict[str, Any]]:
    return await _get("/search/movie", {"query": query, "limit": limit, "extended": "full"}) or []


async def movie(movie_id: str) -> dict[str, Any] | None:
    """Chi tiết phim theo IMDb ID, Trakt ID hoặc slug."""
    if not _ID.match(movie_id):
        raise NotFoundError("Trakt id must be an IMDb id, a Trakt id or a slug")
    return await _get(f"/movies/{movie_id}", {"extended": "full"})
