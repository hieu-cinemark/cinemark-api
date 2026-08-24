"""Request interceptor: assigns/propagates a request id and logs every
request once it's done (method, path, status, duration) - the request id is
bound into structlog's contextvars for the duration of the request, so every
log line emitted anywhere while handling it (routes, services, db calls)
carries the same request_id without threading it through every function
signature by hand."""

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
            # The route/handler didn't turn this into an AppError - let it
            # propagate to the catch-all exception handler in errors.py,
            # but log the timing here too before the context gets cleared.
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
