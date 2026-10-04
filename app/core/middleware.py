"""Bộ chặn request: gán/truyền tiếp request id và log mỗi request khi xong (method,
path, status, thời gian) - request id được bind vào contextvars của structlog trong
suốt request, nên mọi dòng log phát ra ở bất cứ đâu trong lúc xử lý nó (route,
service, lời gọi db) đều mang cùng request_id mà không phải tự truyền qua chữ ký của
từng hàm."""

from __future__ import annotations

import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.core.logging import bind_request_context, clear_request_context, get_logger

logger = get_logger(__name__)

REQUEST_ID_HEADER = "x-request-id"


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        bind_request_context(request_id=request_id)

        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            # Route/handler không chuyển lỗi này thành AppError - để nó đi tiếp tới handler
            # exception bắt-tất-cả trong errors.py, nhưng vẫn log thời gian ở đây trước khi
            # context bị xoá.
            duration_ms = (time.perf_counter() - start) * 1000
            logger.error(
                "request_failed", method=request.method, path=request.url.path, duration_ms=round(duration_ms, 1)
            )
            clear_request_context()
            raise

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "request_handled",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 1),
        )
        response.headers[REQUEST_ID_HEADER] = request_id
        clear_request_context()
        return response
