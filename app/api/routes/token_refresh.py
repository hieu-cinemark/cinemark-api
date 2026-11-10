"""Route quản lý phiên đăng nhập dùng chung cho facebook, threads, tiktok:

- `GET  /<platform>/token-status`: trạng thái phiên hiện tại.
- `WS   /<platform>/refresh-token/ws`: tiến độ lần refresh vừa kích hoạt.
- `POST /<platform>/import-cookies`: nhận cookie người dùng xuất từ trình duyệt rồi refresh.
- `POST /<platform>/restore-session`: dùng lại phiên đã lưu (Redis / cột cookie) rồi refresh.

Không có nút gõ mật khẩu từ dashboard. Cache token hết hạn không có nghĩa là tài khoản bị đăng xuất: thử
`restore-session` trước, chỉ `import-cookies` khi cookie đã chết. Nền tảng đăng ký bằng
`build_token_refresh_routes(router, "<platform>")`.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.clients.kafka import publish_cookie_import_request, publish_restore_session_request
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.errors import NoSavedSessionError, NotFoundError
from app.core.logging import get_logger
from app.schemas.scraper import ImportCookiesRequest, RestoreSessionRequest, TokenStatus, TriggerTokenRefreshResponse
from app.services.accounts.platform_config_db import get_account
from app.services.accounts.platform_token import account_key as _account_key
from app.services.accounts.platform_token import get_token_status
from app.services.crawl import refresh_tracker

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
            raise NoSavedSessionError("This account has no saved login to reuse. Paste cookies from a real browser.")
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
