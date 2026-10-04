"""Cấu hình structlog cho cinemark-api.

Hợp đồng log dùng chung với spider-hub (social_crawler/logger.py bên đó cài đặt
cùng hợp đồng này - giữ hai bên đồng bộ):

  - Mọi dòng đều có: timestamp (ISO 8601, UTC), level, event, service, logger (tên
    module), cộng với mọi context đã bind (request_id ở đây qua
    bind_request_context, run_id ở spider-hub).
  - event là một tên tiếng Anh dạng snake_case cố định ("proxy_settings_updated"),
    không bao giờ là một câu được ghép chuỗi - phần thay đổi đặt vào các trường
    key=value.
  - Lỗi dùng cùng các key ở mọi nơi: error (nội dung thông báo), error_type (tên
    class exception), error_code (mã cấp app, ví dụ AppError.code). Truyền chính
    object exception vào error= sẽ tự điền cả error lẫn error_type (xem
    _normalize_error_fields); các alias cũ exc=/err= cũng được gộp vào error= theo
    cách đó.
  - LOG_FORMAT=console (mặc định) in mỗi event một dòng dễ đọc; LOG_FORMAT=json in
    mỗi dòng một object JSON để chuyển log đi nơi khác. Chỉ có màu khi ghi ra
    terminal thật - file log không bao giờ có mã escape ANSI.
  - LOG_LEVEL (mặc định INFO) lọc bỏ các mức thấp hơn.

`bind_request_context()` / `clear_request_context()` được middleware log request
dùng để gắn request_id vào mọi dòng log phát ra trong lúc xử lý một request, mà
không phải truyền nó qua từng lời gọi.
"""

from __future__ import annotations

import logging
import sys

import structlog

from app.core.config import settings

SERVICE_NAME = "cinemark-api"

_configured = False


def _add_service_field(_logger, _method_name, event_dict):
    event_dict.setdefault("service", SERVICE_NAME)
    return event_dict


def _normalize_error_fields(_logger, _method_name, event_dict):
    """Gộp các alias cũ exc=/err= vào error=, và biến một object exception truyền vào
    error= thành error (text) + error_type (tên class) - để mọi dòng lỗi có cùng một
    dạng bất kể chỗ gọi viết thế nào."""
    for alias in ("exc", "err"):
        if alias in event_dict and "error" not in event_dict:
            event_dict["error"] = event_dict.pop(alias)
    error = event_dict.get("error")
    if isinstance(error, BaseException):
        event_dict.setdefault("error_type", type(error).__name__)
        event_dict["error"] = str(error) or type(error).__name__
    return event_dict


def _configure_once() -> None:
    global _configured
    if _configured:
        return
    _configured = True

    logging.basicConfig(level=settings.log_level, format="%(message)s")

    as_json = settings.log_format.lower() == "json"
    renderer = (
        structlog.processors.JSONRenderer(ensure_ascii=False)
        if as_json
        else structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_service_field,
            _normalize_error_fields,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.dict_tracebacks if as_json else structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(settings.log_level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    _configure_once()
    return structlog.get_logger(name).bind(logger=name)


def bind_request_context(**kwargs: object) -> None:
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_request_context() -> None:
    structlog.contextvars.clear_contextvars()
