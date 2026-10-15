"""Các route GET/POST /<platform>/keywords dùng chung - GET liệt kê mọi từ khoá đang
bật của một nền tảng (kèm tên phim), để form kích hoạt crawl trên dashboard có ô
chọn từ khoá thật thay vì chỉ có "mọi từ khoá đang bật"; POST cho phép chính form đó
tạo từ khoá mới ngay tại chỗ (gõ vào ô chọn, không có trong danh sách) mà không phải
rời trang. Cùng kiểu builder dùng chung như platform_scraper.build_run_route /
token_refresh.build_token_refresh_routes."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.errors import NotFoundError, UpstreamError, ValidationError
from app.schemas.scraper import KeywordEnabledUpdate, KeywordOut
from app.services.d1 import d1_query, get_or_create_keyword, list_keywords, set_keyword_enabled
from app.services.keyword_yield import PLATFORMS as YIELD_PLATFORMS
from app.services.keyword_yield import keyword_yields, paused_keywords


class KeywordCreateRequest(BaseModel):
    movie_id: str
    keyword: str


def build_keyword_routes(router: APIRouter, platform: str) -> None:
    @router.get("/keywords", response_model=list[KeywordOut])
    async def keywords() -> list[KeywordOut]:
        rows = await list_keywords(platform)
        return [KeywordOut(**row) for row in rows]

    @router.post("/keywords", response_model=KeywordOut)
    async def create_keyword(payload: KeywordCreateRequest) -> KeywordOut:
        keyword = payload.keyword.strip()
        if not keyword:
            raise ValidationError("keyword must not be empty")
        if platform == "threads" and keyword.startswith("#"):
            # Tìm hashtag trên Threads trả bài ngẫu nhiên toàn cầu (đo 01-15/10/2026: ~1% liên quan) - chỉ cào dạng chữ.
            raise ValidationError("Threads không cào theo hashtag - nhập từ khoá dạng chữ (vd. tên phim)")
        row = await get_or_create_keyword(payload.movie_id, platform, keyword)
        if row is None:
            raise UpstreamError("Could not create keyword (D1 write failed or movie not found)")
        return KeywordOut(**row)

    @router.patch("/keywords/{keyword_id}", response_model=KeywordOut)
    async def update_keyword_enabled(keyword_id: str, payload: KeywordEnabledUpdate) -> KeywordOut:
        row = await set_keyword_enabled(platform, keyword_id, payload.enabled)
        if row is None:
            raise NotFoundError(f"No {platform} keyword {keyword_id}")
        return KeywordOut(**row)

    if platform not in YIELD_PLATFORMS:
        return

    @router.get("/keywords/yield")
    async def keyword_yield() -> dict:
        """Hiệu quả từng từ khoá 14 ngày qua (app/services/keyword_yield.py) + từ khoá bị tự tắt vì toàn ra rác."""
        yields = await keyword_yields(platform)
        rows = await d1_query(
            "SELECT k.id, k.keyword, k.enabled, m.title AS movie_title FROM keywords k JOIN movies m ON m.id = k.movie_id "
            "WHERE k.platform = ? AND m.enabled = 1",
            [platform],
        )
        paused = await paused_keywords()
        items = [
            {
                "id": row["id"],
                "keyword": row["keyword"],
                "movie_title": row["movie_title"],
                "enabled": bool(row["enabled"]),
                "auto_paused": row["id"] in paused and not row["enabled"],
                **yields.get(row["id"], {"sample": 0, "ratio": None}),
            }
            for row in rows or []
        ]
        items.sort(key=lambda item: (item["ratio"] is None, item["ratio"] if item["ratio"] is not None else 0))
        return {"items": items}
