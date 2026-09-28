"""structlog setup for cinemark-api.

Shared log contract with spider-hub (social_crawler/logger.py implements
the same one - keep the two in sync):

  - Every line carries: timestamp (ISO 8601, UTC), level, event, service,
    logger (module name), plus whatever context was bound (request_id
    here via bind_request_context, run_id in spider-hub).
  - event is a static snake_case English name ("proxy_settings_updated"),
    never an interpolated sentence - variable parts go in key=value fields.
  - Errors use the same keys everywhere: error (message text), error_type
    (exception class name), error_code (an app-level code, e.g.
    AppError.code). Passing the exception object itself as error= fills in
    both error and error_type automatically (see _normalize_error_fields);
    the legacy aliases exc=/err= are folded into error= the same way.
  - LOG_FORMAT=console (default) renders one human-readable line per event;
    LOG_FORMAT=json renders one JSON object per line for log shipping.
    Colors only when writing to a real terminal - log files never get ANSI
    escape codes.
  - LOG_LEVEL (default INFO) filters below that level.

`bind_request_context()` / `clear_request_context()` are used by the
request-logging middleware to attach a request_id to every log line
emitted while handling one request, without passing it through every call.
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
    """Folds the legacy exc=/err= aliases into error=, and turns an
    exception object passed as error= into error (text) + error_type (class
    name) - so every error line has the same shape however the call site
    wrote it."""
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
