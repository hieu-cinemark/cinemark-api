"""Nguồn nghiên cứu bổ sung cho đánh giá phim."""

from typing import Any

from fastapi import APIRouter

from app.schemas.research import GoogleNewsRequest
from app.services import google_news, post_release_news, press_paging

router = APIRouter(prefix="/research", tags=["research"])


@router.post("/google-news")
async def google_news_search(payload: GoogleNewsRequest) -> dict[str, Any]:
    """Tìm Google News Light, trả JSON SerpApi cùng nguồn và thông tin phân trang."""
    return await google_news.search(payload)


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
async def more_movie_news(
    movie_id: str, page: int = 2, max_pages: int = 6, source_offset: int = 0
) -> dict[str, Any]:
    """Lấy tiếp các trang kế tiếp của Google Search và Google News Light.

    Bản tin trong D1 mới có trang đầu mỗi nguồn; endpoint này đọc câu tìm đã lưu rồi
    gọi tiếp cho tới khi hết trang, trả về danh sách bài MỚI đã đánh số nguồn tiếp nối.
    `source_offset` là số bài đã giao ở lượt trước để số nguồn không bị lặp.
    """
    return await press_paging.more_pages(movie_id, page=page, max_pages=max_pages, source_offset=source_offset)
