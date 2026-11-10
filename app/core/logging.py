"""Cấu hình structlog, theo hợp đồng log dùng chung với spider-hub.

- Mỗi dòng có `timestamp` (UTC), `level`, `event`, `service`, `logger` và context đã bind (`request_id`, `run_id`).
- `event` là tên snake_case cố định; dữ liệu thay đổi đặt vào trường key=value.
- Lỗi dùng `error`, `error_type`, `error_code`; truyền exception vào `error=` tự điền hai trường đầu.
- `LOG_FORMAT=console|json`, `LOG_LEVEL` lọc mức log; chỉ có màu khi ghi ra terminal.

`enable_file_logging(path)` ghi thêm ra file (trang Nhật ký đọc). `bind_request_context` / `clear_request_context`
gắn `request_id` cho mọi dòng trong một request.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
from typing import TextIO, cast

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
        logger_factory=structlog.PrintLoggerFactory(file=cast(TextIO, _tee)),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    _configure_once()
    return structlog.get_logger(name).bind(logger=name)


def bind_request_context(**kwargs: object) -> None:
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_request_context() -> None:
    structlog.contextvars.clear_contextvars()
