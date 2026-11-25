"""Metadata phim / show từ nguồn mở, gọi thẳng nguồn và trả về - không lưu DB (25/11/2026).

- Wikidata: phim theo quốc gia + năm (CC0). Phim Việt đang chiếu gần như chưa có trên Wikidata; phim Mỹ / Hàn / Nhật đầy đủ hơn.
- KinoCheck: trailer / teaser / clip chính thức theo IMDb / TMDB ID, trailer mới / thịnh hành (không key; chỉ en / de).
- Trailerix: metadata + trailer theo IMDb ID (cần TRAILERIX_API_KEY, phải ghi nguồn).
"""

from typing import Any

from fastapi import APIRouter, Query, Request

from app.clients import kinocheck, trailerix, wikidata
from app.core.errors import NotFoundError

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


@router.get("/wikidata/movies/{qid}")
async def wikidata_movie(qid: str, lang: str = Query(default="vi,en", pattern=r"^[a-z,\-]+$")) -> dict[str, Any]:
    """Chi tiết một phim theo QID Wikidata (vd Q123456)."""
    if not qid.upper().startswith("Q") or not qid[1:].isdigit():
        raise NotFoundError("Wikidata id must look like Q123456")
    items = await wikidata.movie_details([qid.upper()], lang=lang)
    if not items or not items[0].get("name"):
        raise NotFoundError("Movie not found on Wikidata")
    return {"source": "wikidata", "license": "CC0", "item": items[0]}


@router.get("/kinocheck/{path:path}")
async def kinocheck_proxy(path: str, request: Request) -> dict[str, Any]:
    """Trailer chính thức từ KinoCheck: movies?imdb_id= | movies?tmdb_id= | shows?tmdb_id= | trailers/latest | trailers/trending |
    trailers?genres=. Mặc định language=en (KinoCheck chỉ có en / de). Phim không có trên KinoCheck thì data = null."""
    data = await kinocheck.get(path, dict(request.query_params))
    return {"source": "kinocheck", "attribution": "Trailers from KinoCheck (kinocheck.com)", "data": data}


@router.get("/trailerix/movie/{imdb_id}")
async def trailerix_movie(imdb_id: str) -> dict[str, Any]:
    """Metadata + trailer chính của một phim theo IMDb ID từ Trailerix (cần TRAILERIX_API_KEY). Không có phim thì data = null."""
    data = await trailerix.movie_by_imdb(imdb_id)
    return {"source": "trailerix", "attribution": trailerix.ATTRIBUTION, "data": data}


@router.get("/sources")
async def sources() -> dict[str, Any]:
    """Nguồn nào đang dùng được (giao diện ẩn / báo nguồn chưa cấu hình)."""
    return {"wikidata": True, "kinocheck": True, "trailerix": trailerix.configured()}
