"""OMDb (omdbapi.com) (26/11/2026): điểm đánh giá của một phim theo IMDb ID - IMDb (điểm + số phiếu), Rotten Tomatoes,
Metacritic, kèm doanh thu Mỹ và giải thưởng. Key lưu ở cinemark-config-db (external_api_keys, name="omdb"); gói miễn phí giới
hạn lượt gọi mỗi ngày nên kết quả được giữ trong Redis OMDB_CACHE_SECONDS.

Phim quốc tế có đủ điểm; phim Việt mới chiếu thường chưa có phiếu IMDb (OMDb trả "N/A") - đo 26/11/2026: Mai (2024) 6.9 với
1.909 phiếu, Trại Giam Hạnh Phúc (2026) chưa có điểm. Tra theo tên có dấu không ra, nên chỉ tra theo IMDb ID (lấy từ TMDB)."""

from __future__ import annotations

import json
import re
from typing import Any

from app.clients import external_keys
from app.clients.external_http import get_json
from app.clients.redis import get_redis_client
from app.core.errors import AppError, NotFoundError, UpstreamError
from app.core.logging import get_logger

logger = get_logger(__name__)

BASE_URL = "https://www.omdbapi.com/"
KEY_NAME = "omdb"
ATTRIBUTION = "Ratings from OMDb API (omdbapi.com)"
IMDB_ID = re.compile(r"^tt\d{5,10}$")
OMDB_CACHE_SECONDS = 24 * 3600
_CACHE_PREFIX = "omdb:ratings:"


class OmdbNotConfigured(AppError):
    status_code = 503
    code = "omdb_not_configured"


async def configured() -> bool:
    return bool(await external_keys.get_key(KEY_NAME))


def _value(raw: Any) -> str | None:
    text = str(raw or "").strip()
    return None if not text or text.upper() == "N/A" else text


def _number(raw: Any) -> float | None:
    text = _value(raw)
    if text is None:
        return None
    try:
        return float(text.replace(",", "").replace("$", "").rstrip("%"))
    except ValueError:
        return None


def normalize(data: dict[str, Any]) -> dict[str, Any]:
    """Câu trả lời OMDb -> dạng gọn cho giao diện; "N/A" thành null."""
    sources = {item.get("Source"): item.get("Value") for item in data.get("Ratings") or [] if isinstance(item, dict)}
    rotten = _value(sources.get("Rotten Tomatoes"))
    imdb_rating = _number(data.get("imdbRating"))
    return {
        "imdb_id": data.get("imdbID"),
        "title": _value(data.get("Title")),
        "year": _value(data.get("Year")),
        "released": _value(data.get("Released")),
        "rated": _value(data.get("Rated")),
        "runtime": _value(data.get("Runtime")),
        "country": _value(data.get("Country")),
        "awards": _value(data.get("Awards")),
        "box_office": _value(data.get("BoxOffice")),
        "ratings": {
            "imdb": {"value": imdb_rating, "votes": int(_number(data.get("imdbVotes")) or 0)}
            if imdb_rating is not None
            else None,
            "rotten_tomatoes": {"value": int(_number(rotten) or 0), "text": rotten} if rotten else None,
            "metacritic": {"value": int(_number(data.get("Metascore")) or 0)}
            if _value(data.get("Metascore"))
            else None,
        },
    }


async def ratings(imdb_id: str) -> dict[str, Any]:
    """Điểm của một phim theo IMDb ID; giữ trong Redis 24 giờ (kể cả khi OMDb chưa có điểm)."""
    imdb_id = imdb_id.strip().lower()
    if not IMDB_ID.match(imdb_id):
        raise NotFoundError("IMDb id must look like tt1234567")
    cache_key = _CACHE_PREFIX + imdb_id
    redis = get_redis_client()
    try:
        cached = await redis.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception as exc:  # noqa: BLE001 - Redis lỗi thì gọi thẳng OMDb
        logger.warning("omdb_cache_read_failed", error=exc)
    key = await external_keys.get_key(KEY_NAME)
    if not key:
        raise OmdbNotConfigured("OMDb API key is not configured (external_api_keys in cinemark-config-db)")
    data = await get_json("OMDb", BASE_URL, params={"apikey": key, "i": imdb_id})
    if not isinstance(data, dict):
        raise UpstreamError("OMDb returned an invalid response")
    if data.get("Response") != "True":
        error = str(data.get("Error") or "")
        if "limit" in error.lower():
            raise UpstreamError("OMDb daily request limit reached, try again tomorrow")
        if "key" in error.lower():
            raise UpstreamError("OMDb rejected the API key")
        raise NotFoundError("Movie not found on OMDb")
    result = normalize(data)
    try:
        await redis.set(cache_key, json.dumps(result, ensure_ascii=False), ex=OMDB_CACHE_SECONDS)
    except Exception as exc:  # noqa: BLE001
        logger.warning("omdb_cache_write_failed", error=exc)
    return result
