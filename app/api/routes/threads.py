"""Threads scraper routes: POST /threads/run, same shared trigger contract
as facebook.py (see platform_scraper.py) - spider-hub's crawl_request_consumer.py
already has a "threads" -> threads_search spider mapping (SPIDER_BY_PLATFORM),
this just exposes the trigger for it. GET /threads/keywords (see
keywords.py) lists keywords for the dashboard's picker. Threads' spider-hub
integration does have its own browser-bootstrap token cache, mirroring
Facebook's field for field (see spider-hub's spiders/threads/auth/bootstrap.py)
- so it gets the same refresh-token/token-status/WS trio via
token_refresh.py."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_run_route
from app.api.routes.token_refresh import build_token_refresh_routes

router = APIRouter(prefix="/threads", tags=["threads"])
build_run_route(router, "threads")
build_keyword_routes(router, "threads")
build_token_refresh_routes(router, "threads")
