"""Cây lỗi tuỳ chỉnh dùng cho cả app + các handler FastAPI biến chúng thành một dạng
JSON thống nhất: {"error": {"code": "...", "message": "..."}}.

Raise các lỗi này từ bất cứ đâu trong app/services, app/api/routes, v.v. thay cho
HTTPException của FastAPI - một hàm service raise NotFoundError không cần biết nó
đang được gọi từ một route HTTP (một Kafka consumer gọi cùng service đó về sau không
có status_code nào để trả, nhưng vẫn bắt được AppError và log err.code)."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.logging import get_logger

logger = get_logger(__name__)


class AppError(Exception):
    """Lớp cơ sở cho mọi lỗi có tên, đã lường trước trong app này. Exception bất ngờ (bug,
    lỗi của thư viện bên thứ ba) KHÔNG phải AppError - chúng được handler bắt-tất-cả bên
    dưới xử lý riêng và luôn trả về một lỗi 500 chung chung, không bao giờ để lộ chi
    tiết bên trong cho client."""

    status_code = 500
    code = "internal_error"

    def __init__(self, message: str | None = None):
        self.message = message or self.__class__.__doc__ or self.code
        super().__init__(self.message)


class NotFoundError(AppError):
    """Tài nguyên được yêu cầu không tồn tại."""

    status_code = 404
    code = "not_found"


class AuthenticationError(AppError):
    """Thông tin xác thực bị thiếu, sai hoặc hết hạn."""

    status_code = 401
    code = "unauthorized"


class AuthorizationError(AppError):
    """Đã xác thực, nhưng không được phép làm việc này (sai scope/role)."""

    status_code = 403
    code = "forbidden"


class ValidationError(AppError):
    """Request đúng định dạng nhưng vi phạm một quy tắc nghiệp vụ (khác với lỗi 422 của
    chính FastAPI/Pydantic cho body request sai định dạng)."""

    status_code = 400
    code = "validation_error"


class NoSavedSessionError(ValidationError):
    """Tài khoản này không có storage_state trong Redis và cũng không có trường cookie nào
    để dùng lại."""

    code = "no_saved_session"


class ConflictError(AppError):
    """Request xung đột với trạng thái hiện có (ví dụ key bị trùng)."""

    status_code = 409
    code = "conflict"


class UpstreamError(AppError):
    """Một phụ thuộc mà app này dựa vào (Kafka, database, một API bên ngoài) bị lỗi hoặc
    không truy cập được."""

    status_code = 502
    code = "upstream_error"


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    # Bắt buộc theo đặc tả OAuth2 (và Swagger UI / mọi OAuth2 client đúng chuẩn đều chờ
    # nó) khi trả 401 - thiếu nó thì client không biết phải thử lại với kiểu xác thực nào.
    headers = {"WWW-Authenticate": "Bearer"} if status_code == 401 else None
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}}, headers=headers)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        logger.warning(
            "app_error", error=exc.message, error_type=type(exc).__name__, error_code=exc.code, path=request.url.path
        )
        return _error_response(exc.status_code, exc.code, exc.message)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled_error", error=exc, error_code="internal_error", path=request.url.path, exc_info=exc)
        return _error_response(500, "internal_error", "Something went wrong.")
