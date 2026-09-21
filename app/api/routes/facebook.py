"""Facebook scraper routes: POST /facebook/run (shared trigger contract -
see platform_scraper.py), GET /facebook/keywords (see keywords.py), the
refresh-token/token-status/WS trio (see token_refresh.py), and
POST /facebook/posts/{post_id}/comments/run (see
platform_scraper.build_comments_run_route). TikTok uses the same restore /
cookie-import trio (see tiktok.py) but recaptures device_id/odin_id rather
than a GraphQL token cache.

Adding another spider-hub-backed platform later means a new file this same
shape: build_run_route(router, "<platform>") + build_keyword_routes (+
build_token_refresh_routes if sessions are restored/imported from the
dashboard, + build_comments_run_route if spider-hub has a comments spider
for it) - registered in app/main.py next to this one."""

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
