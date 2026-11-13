"""Các route YouTube: POST /youtube/run, GET/POST /youtube/keywords, POST /youtube/posts/{post_id}/comments/run - cùng hợp
đồng với các nền tảng khác (platform_scraper.py, keywords.py). YouTube không dùng tài khoản nên không có route phiên/token."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_comments_run_route, build_run_route

router = APIRouter(prefix="/youtube", tags=["youtube"])
build_run_route(router, "youtube")
build_keyword_routes(router, "youtube")
build_comments_run_route(router, "youtube")
