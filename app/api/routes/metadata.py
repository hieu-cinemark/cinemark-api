"""Metadata phim / show từ nguồn mở, gọi thẳng nguồn và trả về - không lưu DB (25/11/2026).

- Wikidata: phim theo quốc gia + năm (CC0). Phim Việt đang chiếu gần như chưa có trên Wikidata; phim Mỹ / Hàn / Nhật đầy đủ hơn.
- TMDB: phim đang chiếu / sắp chiếu tại VN, phim theo quốc gia, tìm kiếm, chi tiết + trailer (token ở cinemark-config-db).
- Trakt: phim trending, phòng vé Mỹ cuối tuần, lịch phát hành (key ở cinemark-config-db).
"""

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query

from app.clients import tmdb, trakt, wikidata
from app.core.errors import NotFoundError

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("/wikidata/movies")
async def wikidata_movies(
    country: str = Query(pattern=r"^[A-Za-z]{2}$", description="Mã quốc gia ISO 3166-1 alpha-2, vd VN, US, KR, JP, TH"),
    year_from: int | None = Query(default=None, ge=1880, le=2100),
    year_to: int | None = Query(default=None, ge=1880, le=2100),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=20000),
    lang: str = Query(default="vi,en", pattern=r"^[a-z]{2,3}(-[a-z]+)?(,[a-z]{2,3}(-[a-z]+)?)*$"),
) -> dict[str, Any]:
    """Phim của một quốc gia trên Wikidata, mới phát hành trước, kèm tên gốc, thể loại, đạo diễn, diễn viên, IMDb/TMDB ID.
    Không có năm thì quét mọi năm (nước nhiều phim như US nên lọc năm để không bị timeout)."""
    items = await wikidata.movies_by_country(
        country.upper(), year_from=year_from, year_to=year_to, limit=limit, offset=offset, lang=lang
    )
    return {
        "source": "wikidata",
        "license": "CC0",
        "country": country.upper(),
        "count": len(items),
        "offset": offset,
        "items": items,
    }


@router.get("/wikidata/search")
async def wikidata_search(
    q: str = Query(min_length=2, max_length=120),
    limit: int = Query(default=12, ge=1, le=20),
    lang: str = Query(default="vi,en", pattern=r"^[a-z,\-]+$"),
) -> dict[str, Any]:
    """Tìm phim trên Wikidata theo tên hoặc IMDb ID (tt...)."""
    items = await wikidata.search_movies(q, limit=limit, lang=lang)
    return {"source": "wikidata", "license": "CC0", "count": len(items), "items": items}


@router.get("/wikidata/movies/{qid}")
async def wikidata_movie(qid: str, lang: str = Query(default="vi,en", pattern=r"^[a-z,\-]+$")) -> dict[str, Any]:
    """Chi tiết một phim theo QID Wikidata (vd Q123456)."""
    if not qid.upper().startswith("Q") or not qid[1:].isdigit():
        raise NotFoundError("Wikidata id must look like Q123456")
    items = await wikidata.movie_details([qid.upper()], lang=lang)
    if not items or not items[0].get("name"):
        raise NotFoundError("Movie not found on Wikidata")
    return {"source": "wikidata", "license": "CC0", "item": items[0]}


# --- TMDB ------------------------------------------------------------------------------------------------------------


def _tmdb(data: Any) -> dict[str, Any]:
    return {"source": "tmdb", "attribution": tmdb.ATTRIBUTION, "image_base": tmdb.IMAGE_BASE, "data": data}


@router.get("/tmdb/list/{kind}")
async def tmdb_list(
    kind: str,
    region: str = Query(default="VN", pattern=r"^[A-Z]{2}$"),
    language: str = Query(default="vi-VN", pattern=r"^[a-z]{2}(-[A-Z]{2})?$"),
    page: int = Query(default=1, ge=1, le=500),
) -> dict[str, Any]:
    """Phim đang chiếu (now_playing) / sắp chiếu (upcoming) / phổ biến (popular) / điểm cao (top_rated) theo vùng."""
    return _tmdb(await tmdb.movie_list(kind, region=region, language=language, page=page))


@router.get("/tmdb/discover")
async def tmdb_discover(
    country: str | None = Query(default=None, pattern=r"^[A-Z]{2}$"),
    genres: str | None = Query(
        default=None, pattern=r"^\d{1,6}(,\d{1,6}){0,5}$", description="ID thể loại, cách nhau dấu phẩy"
    ),
    person: int | None = Query(default=None, ge=1),
    company: int | None = Query(default=None, ge=1),
    year_from: int | None = Query(default=None, ge=1880, le=2100),
    year_to: int | None = Query(default=None, ge=1880, le=2100),
    sort: str = Query(default="popular", pattern=r"^(popular|newest|rating)$"),
    language: str = Query(default="vi-VN", pattern=r"^[a-z]{2}(-[A-Z]{2})?$"),
    page: int = Query(default=1, ge=1, le=500),
) -> dict[str, Any]:
    """Lọc phim theo quốc gia gốc, thể loại, người tham gia, hãng sản xuất, năm."""
    data = await tmdb.discover(
        country=country,
        genres_filter=genres,
        person=person,
        company=company,
        year_from=year_from,
        year_to=year_to,
        sort=sort,
        language=language,
        page=page,
    )
    return _tmdb(data)


@router.get("/tmdb/trending")
async def tmdb_trending(
    window: str = Query(default="week", pattern=r"^(day|week)$"), page: int = Query(default=1, ge=1, le=500)
) -> dict[str, Any]:
    """Phim đang được quan tâm nhất trong ngày / tuần."""
    return _tmdb(await tmdb.trending(window, page=page))


@router.get("/tmdb/genres")
async def tmdb_genres() -> dict[str, Any]:
    return _tmdb(await tmdb.genres())


@router.get("/tmdb/people")
async def tmdb_people(
    q: str = Query(min_length=1, max_length=120), page: int = Query(default=1, ge=1, le=500)
) -> dict[str, Any]:
    """Tìm diễn viên / đoàn làm phim theo tên."""
    return _tmdb(await tmdb.search_people(q, page=page))


@router.get("/tmdb/companies")
async def tmdb_companies(q: str = Query(min_length=1, max_length=120)) -> dict[str, Any]:
    """Tìm hãng sản xuất theo tên."""
    return _tmdb(await tmdb.search_companies(q))


@router.get("/tmdb/person/{person_id}")
async def tmdb_person(person_id: int) -> dict[str, Any]:
    """Tiểu sử và phim đã tham gia của một người; không có thì data = null."""
    return _tmdb(await tmdb.person(person_id))


@router.get("/tmdb/collection/{collection_id}")
async def tmdb_collection(collection_id: int) -> dict[str, Any]:
    """Các phim cùng một loạt."""
    return _tmdb(await tmdb.collection(collection_id))


@router.get("/tmdb/search")
async def tmdb_search(
    q: str = Query(min_length=1, max_length=120),
    language: str = Query(default="vi-VN", pattern=r"^[a-z]{2}(-[A-Z]{2})?$"),
    page: int = Query(default=1, ge=1, le=500),
) -> dict[str, Any]:
    return _tmdb(await tmdb.search(q, language=language, page=page))


@router.get("/tmdb/movie/{movie_id}")
async def tmdb_movie(
    movie_id: str, language: str = Query(default="vi-VN", pattern=r"^[a-z]{2}(-[A-Z]{2})?$")
) -> dict[str, Any]:
    """Chi tiết phim theo TMDB ID hoặc IMDb ID (tt...); không có thì data = null."""
    return _tmdb(await tmdb.movie(movie_id, language=language))


# --- Trakt -----------------------------------------------------------------------------------------------------------


def _trakt(data: Any) -> dict[str, Any]:
    return {"source": "trakt", "attribution": trakt.ATTRIBUTION, "data": data}


@router.get("/trakt/trending")
async def trakt_trending(
    page: int = Query(default=1, ge=1, le=100), limit: int = Query(default=24, ge=1, le=50)
) -> dict[str, Any]:
    return _trakt(await trakt.trending(page, limit))


@router.get("/trakt/boxoffice")
async def trakt_box_office() -> dict[str, Any]:
    """Top 10 phòng vé Mỹ cuối tuần gần nhất (doanh thu USD)."""
    return _trakt(await trakt.box_office())


@router.get("/trakt/calendar")
async def trakt_calendar(start: date | None = None, days: int = Query(default=7, ge=1, le=31)) -> dict[str, Any]:
    """Phim phát hành trong `days` ngày từ `start` (mặc định hôm nay)."""
    return _trakt(await trakt.calendar(start or datetime.now(VN_TZ).date(), days))


@router.get("/trakt/search")
async def trakt_search(
    q: str = Query(min_length=2, max_length=120),
    page: int = Query(default=1, ge=1, le=100),
    limit: int = Query(default=24, ge=1, le=50),
) -> dict[str, Any]:
    return _trakt(await trakt.search(q, page, limit))


@router.get("/trakt/movie/{movie_id}")
async def trakt_movie(movie_id: str) -> dict[str, Any]:
    return _trakt(await trakt.movie(movie_id))


@router.get("/sources")
async def sources() -> dict[str, Any]:
    """Nguồn nào đang dùng được (giao diện báo nguồn chưa cấu hình)."""
    return {"wikidata": True, "tmdb": await tmdb.configured(), "trakt": await trakt.configured()}
