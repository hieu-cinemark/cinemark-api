"""Metadata phim / show từ nguồn mở, gọi thẳng nguồn và trả về - không lưu DB (25/11/2026).

- Wikidata: phim theo quốc gia + năm (CC0). Phim Việt đang chiếu gần như chưa có trên Wikidata; phim Mỹ / Hàn / Nhật đầy đủ hơn.
- Trailerix: metadata + trailer theo IMDb ID (key ở cinemark-config-db, bảng external_api_keys; phải ghi nguồn).
"""

from typing import Any

from fastapi import APIRouter, Query

from app.clients import trailerix, wikidata
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


@router.get("/trailerix/latest")
async def trailerix_latest(
    page: int = Query(default=1, ge=1, le=500), per_page: int = Query(default=24, ge=1, le=50)
) -> dict[str, Any]:
    """Trailer mới nhất của Trailerix, phân trang (hết trang thì data rỗng)."""
    data = await trailerix.latest(page, per_page)
    return {"source": "trailerix", "attribution": trailerix.ATTRIBUTION, "data": data}


@router.get("/trailerix/search")
async def trailerix_search(q: str = Query(min_length=2, max_length=120)) -> dict[str, Any]:
    """Tìm phim trên Trailerix theo tên (kết quả chỉ có tên, tóm tắt, slug)."""
    return {"source": "trailerix", "attribution": trailerix.ATTRIBUTION, "data": await trailerix.search(q)}


@router.get("/trailerix/movie/{kind}/{movie_id}")
async def trailerix_movie(kind: str, movie_id: str) -> dict[str, Any]:
    """Metadata + trailer của một phim từ Trailerix theo IMDb ID (kind=imdb) hoặc TMDB ID (kind=tmdb). Không có phim thì data = null.
    Độ phủ còn mỏng (25/11/2026: có Avengers: Endgame, chưa có Avatar / Inception / Oppenheimer / phim Việt)."""
    data = await trailerix.movie(kind, movie_id)
    return {"source": "trailerix", "attribution": trailerix.ATTRIBUTION, "data": data}


@router.get("/sources")
async def sources() -> dict[str, Any]:
    """Nguồn nào đang dùng được (giao diện ẩn / báo nguồn chưa cấu hình)."""
    return {"wikidata": True, "trailerix": await trailerix.configured()}
