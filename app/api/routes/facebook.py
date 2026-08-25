"""Facebook-specific scraper routes: POST /facebook/run (shared trigger
contract - see platform_scraper.py) plus the Facebook-only session/token
endpoints spider-hub's cookie-based login (bootstrap.py) needs. Threads/
TikTok don't have an equivalent - they're API-key based, managed entirely
on cinemark-scraper's own side, not spider-hub.

Adding another spider-hub-backed platform later means a new file this same
shape: build_run_route(router, "<platform>") + whatever platform-specific
extras it needs, if any - registered in app/main.py next to this one."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.platform_scraper import build_run_route
from app.schemas.scraper import FacebookTokenStatus, TriggerTokenRefreshResponse
from app.services.facebook_token import get_facebook_token_status
from app.services.kafka import publish_token_refresh_request

router = APIRouter(prefix="/facebook", tags=["facebook"])
build_run_route(router, "facebook")


@router.post("/refresh-token", response_model=TriggerTokenRefreshResponse)
async def refresh_facebook_token() -> TriggerTokenRefreshResponse:
    """Manual trigger for the same Facebook session-token refresh that
    already runs on spider-hub's 4h cron (scripts/refresh_token.sh) - for
    when the cached token expires between ticks and someone doesn't want to
    wait or SSH in to run the script by hand."""
    ok = await publish_token_refresh_request()
    return TriggerTokenRefreshResponse(ok=ok)


@router.get("/token-status", response_model=FacebookTokenStatus)
async def facebook_token_status() -> FacebookTokenStatus:
    """Whether spider-hub's cached Facebook session (see graphql_client.py's
    FacebookGraphQLClient there) is still usable - the cache key's own
    Redis TTL already encodes this (bootstrap.py sets it equal to
    CACHE_MAX_AGE_SECONDS), so "does the key still exist" *is* "is it
    valid", no separate age calculation needed."""
    account, ttl = await get_facebook_token_status()
    return FacebookTokenStatus(valid=ttl is not None, account=account, expires_in_seconds=ttl)
