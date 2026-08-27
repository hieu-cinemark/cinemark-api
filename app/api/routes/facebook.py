"""Facebook scraper routes: POST /facebook/run (shared trigger contract -
see platform_scraper.py), GET /facebook/keywords (see keywords.py), plus
the refresh-token/token-status/WS trio every spider-hub-backed platform
with its own browser-bootstrap token cache gets (see token_refresh.py).
TikTok is spider-hub-backed too (see tiktok.py) but skips this trio - its
identity is a captured cookie/device_id/odin_id triple, not a browser
session token that expires and needs periodic re-bootstrapping.

Adding another spider-hub-backed platform later means a new file this same
shape: build_run_route(router, "<platform>") + build_keyword_routes (+
build_token_refresh_routes if it has a bootstrap-captured token cache like
Facebook/Threads do) - registered in app/main.py next to this one."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_run_route
from app.api.routes.token_refresh import build_token_refresh_routes

router = APIRouter(prefix="/facebook", tags=["facebook"])
build_run_route(router, "facebook")
build_keyword_routes(router, "facebook")
build_token_refresh_routes(router, "facebook")
