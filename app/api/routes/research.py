"""Nguồn nghiên cứu bổ sung cho đánh giá phim."""

from typing import Any

from fastapi import APIRouter

from app.schemas.research import GoogleNewsRequest
from app.services import google_news, post_release_news

router = APIRouter(prefix="/research", tags=["research"])


@router.post("/google-news")
async def google_news_search(payload: GoogleNewsRequest) -> dict[str, Any]:
    """Tìm Google News Light, trả JSON SerpApi cùng nguồn và thông tin phân trang."""
    return await google_news.search(payload)


@router.get("/movies/{movie_id}/news")
async def movie_news(movie_id: str) -> dict[str, Any]:
    return await post_release_news.get_news(movie_id)


@router.post("/movies/{movie_id}/news/analyze")
async def analyze_movie_news(movie_id: str) -> dict[str, Any]:
    return await post_release_news.analyze(movie_id)
