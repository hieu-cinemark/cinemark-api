"""Các route scraper Threads: POST /threads/run, cùng hợp đồng kích hoạt dùng chung với
facebook.py (xem platform_scraper.py) - crawl_request_consumer.py của spider-hub đã
có ánh xạ "threads" -> spider threads_search (SPIDER_BY_PLATFORM), ở đây chỉ mở ra
nút kích hoạt cho nó. GET /threads/keywords (xem keywords.py) liệt kê từ khoá cho ô
chọn trên dashboard. Phần tích hợp Threads của spider-hub có cache token bootstrap
trình duyệt riêng, giống Facebook từng trường một (xem
spiders/threads/auth/bootstrap.py của spider-hub) - nên nó có cùng bộ ba
refresh-token/token-status/WS qua token_refresh.py, và POST
/threads/posts/{post_id}/comments/run (xem platform_scraper.build_comments_run_route
- spider threads_comments của spider-hub chỉ lấy trang reply đầu tiên của một bài,
xem docstring module của nó để biết lý do, nhưng đó vẫn là một tính năng comment
thật đáng để mở ra ở đây)."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_comments_run_route, build_run_route
from app.api.routes.token_refresh import build_token_refresh_routes

router = APIRouter(prefix="/threads", tags=["threads"])
build_run_route(router, "threads")
build_keyword_routes(router, "threads")
build_token_refresh_routes(router, "threads")
build_comments_run_route(router, "threads")
