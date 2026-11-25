"""TMDB (api.themoviedb.org) (25/11/2026): phim đang chiếu / sắp chiếu theo vùng (VN), phim theo quốc gia gốc, tìm kiếm, chi tiết
kèm trailer, diễn viên, ảnh. Có tên + tóm tắt tiếng Việt (language=vi-VN) và phủ phim Việt tốt (136 phim Việt 2026; Bò Sữa
Bay, Mẹ Mìn, Trại Giam Hạnh Phúc đều có). Xác thực bằng API Read Access Token (Bearer) lưu ở cinemark-config-db
(external_api_keys, name="tmdb"). Điều khoản TMDB: hiển thị ghi nguồn "This product uses the TMDB API but is not endorsed or
certified by TMDB"; dùng thương mại cần thoả thuận riêng với TMDB.
"""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime, timedelta
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


async def trending(window: str = "week", *, language: str = "vi-VN", page: int = 1) -> dict[str, Any]:
    """Phim đang được quan tâm nhất trên TMDB trong ngày (day) hoặc tuần (week)."""
    if window not in ("day", "week"):
        raise NotFoundError("TMDB trending window must be day or week")
    return _page(await _get(f"/trending/movie/{window}", {"language": language, "page": page}))


async def genres(*, language: str = "vi") -> list[dict[str, Any]]:
    return (await _get("/genre/movie/list", {"language": language}) or {}).get("genres") or []


# Xếp theo điểm phải có ngưỡng lượt chấm, không thì phim vài phiếu 10 điểm đứng đầu. Lọc theo một nước thì ngưỡng thấp hơn
# (phim Việt có rất ít lượt chấm).
_MIN_VOTES = 300
_MIN_VOTES_BY_COUNTRY = 10
_SORTS = {"popular": "popularity.desc", "newest": "primary_release_date.desc", "rating": "vote_average.desc"}


async def discover(
    *,
    country: str | None = None,
    genres_filter: str | None = None,
    person: int | None = None,
    company: int | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    sort: str = "popular",
    language: str = "vi-VN",
    page: int = 1,
) -> dict[str, Any]:
    """Lọc phim: quốc gia gốc, thể loại (ID cách nhau dấu phẩy = phải có đủ), người tham gia (diễn viên / đoàn làm phim),
    hãng sản xuất, khoảng năm. Xếp theo độ phổ biến (mặc định), ngày phát hành mới nhất hoặc điểm. Xếp "mới nhất" mà không
    chọn năm thì chỉ lấy tới một năm tới, để phim mới lên danh sách (chưa có gì ngoài tên) không đứng đầu."""
    params: dict[str, Any] = {
        "sort_by": _SORTS.get(sort, _SORTS["popular"]),
        "language": language,
        "page": page,
        "include_adult": "false",
    }
    optional = {
        "with_origin_country": country,
        "with_genres": genres_filter,
        "with_people": person,
        "with_companies": company,
    }
    params.update({key: value for key, value in optional.items() if value})
    if sort == "rating":
        params["vote_count.gte"] = _MIN_VOTES_BY_COUNTRY if country or person or company else _MIN_VOTES
    if year_from:
        params["primary_release_date.gte"] = f"{year_from}-01-01"
    if year_to:
        params["primary_release_date.lte"] = f"{year_to}-12-31"
    elif sort == "newest":
        params["primary_release_date.lte"] = (datetime.now(UTC) + timedelta(days=365)).date().isoformat()
    return _page(await _get("/discover/movie", params))


async def search(query: str, *, language: str = "vi-VN", page: int = 1) -> dict[str, Any]:
    return _page(
        await _get("/search/movie", {"query": query, "language": language, "page": page, "include_adult": "false"})
    )


async def search_people(query: str, *, language: str = "vi-VN", page: int = 1) -> dict[str, Any]:
    return _page(
        await _get("/search/person", {"query": query, "language": language, "page": page, "include_adult": "false"})
    )


async def search_companies(query: str) -> dict[str, Any]:
    return _page(await _get("/search/company", {"query": query}))


async def person(person_id: int, *, language: str = "vi-VN") -> dict[str, Any] | None:
    """Tiểu sử + phim đã tham gia. Tiểu sử tiếng Việt thường trống: lấy bản tiếng Anh thay và đánh dấu `biography_language`."""
    data = await _get(
        f"/person/{person_id}", {"language": language, "append_to_response": "movie_credits,external_ids"}
    )
    if data is None:
        return None
    data["biography_language"] = language[:2]
    if not data.get("biography") and not language.startswith("en"):
        english = await _get(f"/person/{person_id}", {"language": "en-US"})
        if english and english.get("biography"):
            data["biography"] = english["biography"]
            data["biography_language"] = "en"
    return data


async def collection(collection_id: int, *, language: str = "vi-VN") -> dict[str, Any] | None:
    """Các phim trong cùng một loạt."""
    return await _get(f"/collection/{collection_id}", {"language": language})


async def movie(movie_id: str, *, language: str = "vi-VN") -> dict[str, Any] | None:
    """Chi tiết theo TMDB ID (số) hoặc IMDb ID (tt...), kèm trailer, diễn viên / đoàn làm phim, ngày phát hành theo nước, phim
    đề xuất, bộ ảnh, từ khoá và bài đánh giá."""
    if _IMDB.match(movie_id):
        found = await _get(f"/find/{movie_id}", {"external_source": "imdb_id", "language": language})
        hits = (found or {}).get("movie_results") or []
        if not hits:
            return None
        movie_id = str(hits[0]["id"])
    if not movie_id.isdigit():
        raise NotFoundError("TMDB id must be a number or an IMDb id")
    detail, reviews = await asyncio.gather(
        _get(
            f"/movie/{movie_id}",
            {
                "language": language,
                "append_to_response": "videos,credits,release_dates,external_ids,recommendations,images,keywords",
                # Trailer / ảnh tiếng Việt thường không có: lấy cả bản tiếng Anh / không ngôn ngữ.
                "include_video_language": "vi,en,null",
                "include_image_language": "vi,en,null",
            },
        ),
        # Bài đánh giá lọc theo ngôn ngữ: hỏi bằng vi-VN thì gần như luôn rỗng nên hỏi riêng bản tiếng Anh.
        _reviews(movie_id),
    )
    if detail is not None:
        detail["reviews"] = reviews
    return detail


async def _reviews(movie_id: str) -> dict[str, Any]:
    """Bài đánh giá là phần phụ: lỗi thì trả rỗng, không làm hỏng trang chi tiết."""
    try:
        return _page(await _get(f"/movie/{movie_id}/reviews", {"language": "en-US"}))
    except AppError:
        return _page(None)
