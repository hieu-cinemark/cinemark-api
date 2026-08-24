"""App-wide custom error hierarchy + the FastAPI handlers that turn them into
a consistent JSON shape: {"error": {"code": "...", "message": "..."}}.

Raise these from anywhere in app/services, app/api/routes, etc. instead of
FastAPI's own HTTPException - a service function raising NotFoundError
doesn't need to know it's being called from an HTTP route (a Kafka consumer
calling the same service later doesn't have a status_code to give, but can
still catch AppError and log err.code)."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.logging import get_logger

logger = get_logger(__name__)


class AppError(Exception):
    """Base class for every expected, named failure in this app. Unexpected
    exceptions (bugs, third-party library errors) are NOT AppErrors - those
    are caught separately by the catch-all handler below and always
    reported as a generic 500, never leaking internals to the client."""

    status_code = 500
    code = "internal_error"

    def __init__(self, message: str | None = None):
        self.message = message or self.__class__.__doc__ or self.code
        super().__init__(self.message)


class NotFoundError(AppError):
    """The requested resource does not exist."""

    status_code = 404
    code = "not_found"


class AuthenticationError(AppError):
    """Missing, invalid, or expired credentials."""

    status_code = 401
    code = "unauthorized"


class AuthorizationError(AppError):
    """Authenticated, but not allowed to do this (wrong scope/role)."""

    status_code = 403
    code = "forbidden"


class ValidationError(AppError):
    """The request is well-formed but fails a business rule (distinct from
    FastAPI/Pydantic's own 422 for malformed request bodies)."""

    status_code = 400
    code = "validation_error"


class ConflictError(AppError):
    """The request conflicts with existing state (e.g. a duplicate key)."""

    status_code = 409
    code = "conflict"


class UpstreamError(AppError):
    """A dependency this app relies on (Kafka, the database, an external
    API) failed or is unreachable."""

    status_code = 502
    code = "upstream_error"


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    # Required by the OAuth2 spec (and expected by Swagger UI / any proper
    # OAuth2 client) on a 401 - without it, clients can't tell what auth
    # scheme to retry with.
    headers = {"WWW-Authenticate": "Bearer"} if status_code == 401 else None
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}}, headers=headers)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        logger.warning("app_error", code=exc.code, message=exc.message, path=request.url.path)
        return _error_response(exc.status_code, exc.code, exc.message)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled_error", error=str(exc), path=request.url.path, exc_info=exc)
        return _error_response(500, "internal_error", "Something went wrong.")
