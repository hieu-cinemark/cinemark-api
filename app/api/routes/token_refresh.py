"""Shared refresh-token trio for every spider-hub-backed platform that has
a saved session to restore or a human-pasted cookie import
(facebook, threads, tiktok):

  GET  /<platform>/token-status         current session status
                                        (FB/Threads: Redis GraphQL cache TTL;
                                         TikTok: enabled account + sessionid)
  WS   /<platform>/refresh-token/ws     live progress of a triggered refresh
  POST /<platform>/import-cookies      hand over a human-exported session,
                                        then refresh (GraphQL tokens, or
                                        TikTok device_id/odin_id)
  POST /<platform>/restore-session     reuse Redis storage_state / cookie
                                        column for one account, then refresh

There's no standalone "refresh now" that types a password. spider-hub's
bootstrap.py refuses to auto-login unattended. The GraphQL token cache
expiring is *not* the same as Facebook logging the account out: cookies in
Redis (or the account's cookie column) may still be live. restore-session
is the button for that case. import-cookies is only when those saved
cookies are actually dead and a person logged in from a real browser.

A platform file just calls build_token_refresh_routes(router, "<platform>") -
same shape as platform_scraper.build_run_route."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.errors import NoSavedSessionError, NotFoundError
from app.core.logging import get_logger
from app.schemas.scraper import ImportCookiesRequest, RestoreSessionRequest, TokenStatus, TriggerTokenRefreshResponse
from app.services import refresh_tracker
from app.services.kafka import publish_cookie_import_request, publish_restore_session_request
from app.services.platform_config_db import get_account
from app.services.platform_token import account_key as _account_key
from app.services.platform_token import get_token_status
from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

logger = get_logger(__name__)


async def _has_saved_session(platform: str, account: dict) -> bool:
    cookie = (account.get("cookie") or "").strip()
    if cookie:
        return True
    client = get_redis_client()
    candidates = []
    for raw in (account.get("email"), account.get("account_id")):
        if raw and str(raw).strip():
            candidates.append(str(raw).strip().lower())
    for suffix in dict.fromkeys(candidates):
        if await client.exists(f"{REDIS_KEY_PREFIX}{platform}:storage_state:{suffix}"):
            return True
    return False


def build_token_refresh_routes(router: APIRouter, platform: str) -> None:
    @router.post("/import-cookies", response_model=TriggerTokenRefreshResponse)
    async def import_cookies(payload: ImportCookiesRequest) -> TriggerTokenRefreshResponse:
        account = await get_account(payload.account_id)
        if account is None or account["platform"] != platform:
            raise NotFoundError(f"No {platform} account {payload.account_id}")
        account_key = _account_key(account)
        run_id = str(uuid.uuid4())
        if not refresh_tracker.start_refresh(platform, run_id):
            return TriggerTokenRefreshResponse(ok=False)
        published = await publish_cookie_import_request(platform, account_key, payload.cookies, run_id=run_id)
        return TriggerTokenRefreshResponse(ok=published is not None)

    @router.post("/restore-session", response_model=TriggerTokenRefreshResponse)
    async def restore_session(payload: RestoreSessionRequest) -> TriggerTokenRefreshResponse:
        account = await get_account(payload.account_id)
        if account is None or account["platform"] != platform:
            raise NotFoundError(f"No {platform} account {payload.account_id}")
        if not await _has_saved_session(platform, account):
            raise NoSavedSessionError(
                "This account has no saved login to reuse. Paste cookies from a real browser."
            )
        run_id = str(uuid.uuid4())
        if not refresh_tracker.start_refresh(platform, run_id):
            return TriggerTokenRefreshResponse(ok=False)
        published = await publish_restore_session_request(platform, _account_key(account), run_id=run_id)
        return TriggerTokenRefreshResponse(ok=published is not None)

    @router.get("/token-status", response_model=TokenStatus)
    async def token_status() -> TokenStatus:
        account, ttl = await get_token_status(platform)
        return TokenStatus(valid=ttl is not None, account=account, expires_in_seconds=ttl)

    @router.websocket("/refresh-token/ws")
    async def refresh_token_ws(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json(refresh_tracker.snapshot(platform))
        queue = refresh_tracker.subscribe(platform)
        try:
            while True:
                message = await queue.get()
                if message is None:
                    break
                await websocket.send_json(message)
        except WebSocketDisconnect:
            pass
        finally:
            refresh_tracker.unsubscribe(platform, queue)
            try:
                await websocket.close()
            except Exception:
                pass
