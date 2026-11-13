"""Nguồn nghiên cứu bổ sung cho đánh giá phim."""

from typing import Annotated, Any

from fastapi import APIRouter, Query

from app.clients import google as google_free
from app.schemas.research import GoogleNewsRequest
from app.services import google_news, post_release_news, press_paging

router = APIRouter(prefix="/research", tags=["research"])


@router.post("/google-news")
async def google_news_search(payload: GoogleNewsRequest) -> dict[str, Any]:
    """Tìm Google News Light, trả JSON SerpApi cùng nguồn và thông tin phân trang."""
    return await google_news.search(payload)


@router.get("/google/news-rss")
async def google_news_rss(
    q: str = Query(min_length=1, max_length=500),
    when: str | None = Query(default=None, pattern=r"^\d{1,3}[hdmy]$"),
    hl: str = "vi",
    gl: str = "VN",
    resolve: bool = False,
) -> dict[str, Any]:
    """Google News RSS miễn phí (~100 bài). resolve=true giải link về bài gốc (chậm hơn, 2 request mỗi bài)."""
    items = await google_free.news_search(q, hl=hl, gl=gl, when=when)
    if resolve:
        await google_free.resolve_links(items)
    return {"q": q, "count": len(items), "items": items}


@router.get("/google/autocomplete")
async def google_autocomplete(q: str = Query(min_length=1, max_length=200), hl: str = "vi", gl: str = "vn") -> dict:
    """Gợi ý tìm kiếm Google - người xem đang hỏi gì về phim (giờ chiếu, diễn viên, kết thúc...)."""
    return {"q": q, "suggestions": await google_free.autocomplete(q, hl=hl, gl=gl)}


@router.get("/google/trending")
async def google_trending(geo: str = Query(default="VN", pattern=r"^[A-Za-z]{2}$")) -> dict[str, Any]:
    """Xu hướng tìm kiếm Google trong ngày của một nước."""
    return {"geo": geo.upper(), "items": await google_free.trending(geo)}


@router.get("/google/trends")
async def google_trends(
    keywords: Annotated[list[str], Query(min_length=1, max_length=5)],
    geo: Annotated[str, Query(pattern=r"^[A-Za-z]{2}$")] = "VN",
    timeframe: str = "today 3-m",
    related: bool = False,
) -> dict[str, Any]:
    """Mức quan tâm Google Trends (0-100, các từ khoá cùng thang); related=true thêm câu tìm liên quan của từ khoá đầu."""
    out = await google_free.interest_over_time(keywords, geo=geo, timeframe=timeframe)
    if related:
        out["related"] = await google_free.related_queries(keywords[0], geo=geo, timeframe=timeframe)
    return out


@router.post("/google-search")
async def google_search(payload: GoogleNewsRequest) -> dict[str, Any]:
    """Google Search: organic_results và top_stories khi Google có trả về."""
    return await google_news.search_web(payload)


@router.get("/movies/{movie_id}/news")
async def movie_news(movie_id: str) -> dict[str, Any] | None:
    return await post_release_news.read_news(movie_id)


@router.post("/movies/{movie_id}/news/refresh")
async def refresh_movie_news(movie_id: str, force: bool = False) -> dict[str, Any]:
    """Lấy tin đến hạn và lưu D1; không gọi Bee."""
    return await post_release_news.refresh_news(movie_id, force=force)


@router.get("/movies/{movie_id}/news/more")
async def more_movie_news(movie_id: str, page: int = 2, max_pages: int = 6, source_offset: int = 0) -> dict[str, Any]:
    """Lấy tiếp các trang kế tiếp của Google Search và Google News Light.

    Bản tin trong D1 mới có trang đầu mỗi nguồn; endpoint này đọc câu tìm đã lưu rồi
    gọi tiếp cho tới khi hết trang, trả về danh sách bài MỚI đã đánh số nguồn tiếp nối.
    `source_offset` là số bài đã giao ở lượt trước để số nguồn không bị lặp.
    """
    return await press_paging.more_pages(movie_id, page=page, max_pages=max_pages, source_offset=source_offset)
