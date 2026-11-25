"""TMDB (api.themoviedb.org) (25/11/2026): phim đang chiếu / sắp chiếu theo vùng (VN), phim theo quốc gia gốc, tìm kiếm, chi tiết
kèm trailer, diễn viên, ảnh. Có tên + tóm tắt tiếng Việt (language=vi-VN) và phủ phim Việt tốt (136 phim Việt 2026; Bò Sữa
Bay, Mẹ Mìn, Trại Giam Hạnh Phúc đều có). Xác thực bằng API Read Access Token (Bearer) lưu ở cinemark-config-db
(external_api_keys, name="tmdb"). Điều khoản TMDB: hiển thị ghi nguồn "This product uses the TMDB API but is not endorsed or
certified by TMDB"; dùng thương mại cần thoả thuận riêng với TMDB.
"""

from __future__ import annotations

import re
from typing import Any

from app.clients import external_keys
from app.clients.external_http import get_json
from app.core.errors import AppError, NotFoundError

BASE_URL = "https://api.themoviedb.org/3"
IMAGE_BASE = "https://image.tmdb.org/t/p"
KEY_NAME = "tmdb"
ATTRIBUTION = "This product uses the TMDB API but is not endorsed or certified by TMDB."
LISTS = ("now_playing", "upcoming", "popular", "top_rated")
_IMDB = re.compile(r"^tt\d{5,10}$")


class TmdbNotConfigured(AppError):
    status_code = 503
    code = "tmdb_not_configured"


async def configured() -> bool:
    return bool(await external_keys.get_key(KEY_NAME))


async def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    token = await external_keys.get_key(KEY_NAME)
    if not token:
        raise TmdbNotConfigured("TMDB token is not configured (external_api_keys in cinemark-config-db)")
    return await get_json("TMDB", f"{BASE_URL}{path}", params=params, headers={"Authorization": f"Bearer {token}"})


def _page(data: Any) -> dict[str, Any]:
    data = data or {}
    return {
        "page": data.get("page", 1),
        "total_pages": data.get("total_pages", 0),
        "total_results": data.get("total_results", 0),
        "results": data.get("results") or [],
    }


async def movie_list(kind: str, *, region: str = "VN", language: str = "vi-VN", page: int = 1) -> dict[str, Any]:
    """now_playing / upcoming / popular / top_rated, lọc theo vùng phát hành."""
    if kind not in LISTS:
        raise NotFoundError(f"TMDB list must be one of {', '.join(LISTS)}")
    return _page(await _get(f"/movie/{kind}", {"region": region, "language": language, "page": page}))


async def discover(
    *, country: str, year_from: int | None = None, year_to: int | None = None, language: str = "vi-VN", page: int = 1
) -> dict[str, Any]:
    """Phim theo quốc gia gốc (with_origin_country), mới phát hành trước."""
    params: dict[str, Any] = {
        "with_origin_country": country,
        "sort_by": "primary_release_date.desc",
        "language": language,
        "page": page,
        "include_adult": "false",
    }
    if year_from:
        params["primary_release_date.gte"] = f"{year_from}-01-01"
    if year_to:
        params["primary_release_date.lte"] = f"{year_to}-12-31"
    return _page(await _get("/discover/movie", params))


async def search(query: str, *, language: str = "vi-VN", page: int = 1) -> dict[str, Any]:
    return _page(
        await _get("/search/movie", {"query": query, "language": language, "page": page, "include_adult": "false"})
    )


async def movie(movie_id: str, *, language: str = "vi-VN") -> dict[str, Any] | None:
    """Chi tiết theo TMDB ID (số) hoặc IMDb ID (tt...), kèm trailer, diễn viên / đoàn làm phim, ngày phát hành theo nước."""
    if _IMDB.match(movie_id):
        found = await _get(f"/find/{movie_id}", {"external_source": "imdb_id", "language": language})
        hits = (found or {}).get("movie_results") or []
        if not hits:
            return None
        movie_id = str(hits[0]["id"])
    if not movie_id.isdigit():
        raise NotFoundError("TMDB id must be a number or an IMDb id")
    return await _get(
        f"/movie/{movie_id}",
        {
            "language": language,
            "append_to_response": "videos,credits,release_dates,external_ids",
            # Trailer tiếng Việt thường không có: lấy cả bản tiếng Anh / không ngôn ngữ.
            "include_video_language": "vi,en,null",
        },
    )
