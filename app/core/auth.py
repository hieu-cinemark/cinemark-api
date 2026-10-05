"""Xác thực bằng một khoá dùng chung cho toàn bộ API (header X-API-Key, hoặc query
?api_key= cho WebSocket - trình duyệt không gửi được header tuỳ chỉnh khi mở
WebSocket).

Chỉ bật khi đặt API_AUTH_KEY trong .env; để trống thì mọi request đi qua như trước
(tương thích ngược). Trước khi có lớp này API hoàn toàn công khai, kể cả
/settings/accounts - nơi từng trả nguyên mật khẩu/2FA/cookie cho bất kỳ ai gọi tới.

ASGI thuần chứ không phải BaseHTTPMiddleware, để chặn được cả WebSocket.
"""

from __future__ import annotations

import hmac
from urllib.parse import parse_qs

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

API_KEY_HEADER = b"x-api-key"
API_KEY_QUERY = "api_key"

# /auth/check tự kiểm tra khoá (xem routes/auth.py) để dashboard biết API có bật auth
# hay không trước khi đăng nhập.
PUBLIC_PATHS = frozenset({"/health", "/auth/check", "/docs", "/redoc", "/openapi.json"})


def provided_key(scope: Scope) -> str:
    for name, value in scope.get("headers") or []:
        if name == API_KEY_HEADER:
            return value.decode("latin-1")
    query = parse_qs((scope.get("query_string") or b"").decode("latin-1"))
    return (query.get(API_KEY_QUERY) or [""])[0]


def key_matches(expected: str, given: str) -> bool:
    return bool(given) and hmac.compare_digest(expected.encode(), given.encode())


class ApiKeyMiddleware:
    def __init__(self, app: ASGIApp, api_key: str | None) -> None:
        self.app = app
        self.api_key = api_key or ""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            not self.api_key
            or scope["type"] not in ("http", "websocket")
            or scope["path"] in PUBLIC_PATHS
            # Preflight CORS không bao giờ mang header tuỳ chỉnh.
            or (scope["type"] == "http" and scope["method"] == "OPTIONS")
            or key_matches(self.api_key, provided_key(scope))
        ):
            await self.app(scope, receive, send)
            return

        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 4401})
            return
        response = JSONResponse(
            status_code=401,
            content={"error": {"code": "unauthorized", "message": "Missing or invalid API key."}},
            headers={"WWW-Authenticate": "Bearer"},
        )
        await response(scope, receive, send)
