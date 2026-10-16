"""Các route scraper Facebook: POST /facebook/run (dùng chung hợp đồng kích hoạt - xem
platform_scraper.py), GET /facebook/keywords (xem keywords.py), bộ ba
refresh-token/token-status/WS (xem token_refresh.py), và
POST /facebook/posts/{post_id}/comments/run (xem
platform_scraper.build_comments_run_route). TikTok dùng cùng bộ ba khôi phục
session / import cookie (xem tiktok.py) nhưng lấy lại device_id/odin_id thay vì
cache token GraphQL.

Sau này thêm một nền tảng khác chạy qua spider-hub thì tạo file mới cùng dạng:
build_run_route(router, "<platform>") + build_keyword_routes (+
build_token_refresh_routes nếu session được khôi phục/import từ dashboard, +
build_comments_run_route nếu spider-hub có spider comment cho nền tảng đó) - rồi
đăng ký trong app/main.py cạnh file này."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_comments_run_route, build_run_route
from app.api.routes.token_refresh import build_token_refresh_routes
from app.services import source_pages

router = APIRouter(prefix="/facebook", tags=["facebook"])
build_run_route(router, "facebook")
build_keyword_routes(router, "facebook")
build_token_refresh_routes(router, "facebook")
build_comments_run_route(router, "facebook")


# --- Trang nguồn (cào theo nguồn, app/services/source_pages.py) ---------------------------------------------------


class SourcePageCreate(BaseModel):
    url: str
    # None/rỗng = trang chung (nhiều phim) - ingest tự tìm phim theo nội dung bài.
    movie_id: str | None = None
    name: str | None = None


class SourcePageUpdate(BaseModel):
    enabled: bool


@router.get("/sources")
async def list_source_pages() -> dict[str, Any]:
    return {"items": await source_pages.list_sources()}


@router.get("/sources/suggestions")
async def source_page_suggestions() -> dict[str, Any]:
    """Trang có nhiều bài liên quan nhất 30 ngày qua chưa có trong danh sách và Kira chưa quyết định."""
    return {"items": await source_pages.pending_suggestions()}


@router.post("/sources")
async def create_source_page(payload: SourcePageCreate) -> dict[str, Any]:
    return await source_pages.add_source(url=payload.url, movie_id=payload.movie_id, name=payload.name)


@router.patch("/sources/{source_id}")
async def update_source_page(source_id: str, payload: SourcePageUpdate) -> dict[str, bool]:
    await source_pages.set_enabled(source_id, payload.enabled)
    return {"ok": True}


@router.delete("/sources/{source_id}")
async def delete_source_page(source_id: str) -> dict[str, bool]:
    await source_pages.delete_source(source_id)
    return {"ok": True}


@router.post("/sources/{source_id}/run")
async def run_source_page(source_id: str) -> dict[str, bool]:
    return {"published": await source_pages.run_source(source_id)}
