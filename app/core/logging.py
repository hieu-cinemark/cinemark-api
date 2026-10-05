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

`enable_file_logging(path)` cho một tiến trình ghi thêm mọi dòng log ra file (vẫn in ra
terminal như cũ) - ingest_consumer dùng nó để trang Nhật ký của dashboard đọc được
(xem settings.ingest_consumer_log_path và routes/logs.py).

`bind_request_context()` / `clear_request_context()` được middleware log request
dùng để gắn request_id vào mọi dòng log phát ra trong lúc xử lý một request, mà
không phải truyền nó qua từng lời gọi.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading

import structlog

from app.core.config import settings

SERVICE_NAME = "cinemark-api"

_configured = False

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
# Trang Nhật ký chỉ đọc vài trăm dòng cuối - 20MB là dư, và một bản .1 giữ lượt trước.
_LOG_FILE_MAX_BYTES = 20 * 1024 * 1024


class _TeeStream:
    """Đích ghi của PrintLogger: luôn ghi ra stdout, và thêm vào file (đã bỏ mã màu
    ANSI) sau khi enable_file_logging() được gọi. Xoay file khi vượt
    _LOG_FILE_MAX_BYTES (đổi tên thành <file>.1, ghi đè bản cũ)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._path: str | None = None
        self._file = None

    def attach(self, path: str) -> None:
        with self._lock:
            if self._file is not None:
                self._file.close()
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self._path = path
            self._file = open(path, "a", encoding="utf-8")  # noqa: SIM115 - sống cùng tiến trình

    def write(self, text: str) -> int:
        sys.stdout.write(text)
        if self._file is not None:
            with self._lock:
                self._file.write(_ANSI_RE.sub("", text))
        return len(text)

    def flush(self) -> None:
        sys.stdout.flush()
        if self._file is None:
            return
        with self._lock:
            self._file.flush()
            try:
                if self._path and os.path.getsize(self._path) > _LOG_FILE_MAX_BYTES:
                    self._file.close()
                    os.replace(self._path, f"{self._path}.1")
                    self._file = open(self._path, "a", encoding="utf-8")  # noqa: SIM115
            except OSError:
                pass


_tee = _TeeStream()


def enable_file_logging(path: str) -> None:
    """Ghi thêm mọi dòng log của tiến trình này vào `path` (vẫn in ra terminal)."""
    _configure_once()
    _tee.attach(path)


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
        logger_factory=structlog.PrintLoggerFactory(file=_tee),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    _configure_once()
    return structlog.get_logger(name).bind(logger=name)


def bind_request_context(**kwargs: object) -> None:
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_request_context() -> None:
    structlog.contextvars.clear_contextvars()
