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

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_comments_run_route, build_run_route
from app.api.routes.token_refresh import build_token_refresh_routes

router = APIRouter(prefix="/facebook", tags=["facebook"])
build_run_route(router, "facebook")
build_keyword_routes(router, "facebook")
build_token_refresh_routes(router, "facebook")
build_comments_run_route(router, "facebook")
