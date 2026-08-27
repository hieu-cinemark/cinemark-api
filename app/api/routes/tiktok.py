"""TikTok scraper routes: POST /tiktok/run (shared trigger contract - see
platform_scraper.py), GET/POST /tiktok/keywords (see keywords.py). Unlike
Facebook/Threads, TikTok has no browser-bootstrap token cache to refresh -
its spider-hub identity (cookie/device_id/odin_id) is captured once into
platform_accounts and doesn't expire the way a browser session token does
(see spider-hub's spiders/tiktok/client.py module docstring), so there's no
build_token_refresh_routes call here."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.keywords import build_keyword_routes
from app.api.routes.platform_scraper import build_run_route

router = APIRouter(prefix="/tiktok", tags=["tiktok"])
build_run_route(router, "tiktok")
build_keyword_routes(router, "tiktok")
