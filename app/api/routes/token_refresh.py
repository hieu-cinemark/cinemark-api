"""Shared refresh-token trio for every spider-hub-backed platform that has
its own browser-bootstrap token cache (facebook, threads - see spider-hub's
constants/facebook.py + constants/threads.py):

  POST /<platform>/refresh-token       fire-and-forget trigger (Kafka)
  GET  /<platform>/token-status         current Redis-cached session status
  WS   /<platform>/refresh-token/ws     live progress of a triggered refresh

A platform file just calls build_token_refresh_routes(router, "<platform>") -
same shape as platform_scraper.build_run_route."""

from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.logging import get_logger
from app.schemas.scraper import TokenStatus, TriggerTokenRefreshResponse
from app.services import refresh_tracker
from app.services.kafka import publish_token_refresh_request
from app.services.platform_token import get_token_status

logger = get_logger(__name__)


def build_token_refresh_routes(router: APIRouter, platform: str) -> None:
    @router.post("/refresh-token", response_model=TriggerTokenRefreshResponse)
    async def refresh_token() -> TriggerTokenRefreshResponse:
        ok = await publish_token_refresh_request(platform)
        if ok:
            refresh_tracker.start_refresh(platform)
        return TriggerTokenRefreshResponse(ok=ok)

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
                await websocket.send_json(message)
        except WebSocketDisconnect:
            pass
        finally:
            refresh_tracker.unsubscribe(platform, queue)
