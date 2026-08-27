"""structlog setup - same idea as spider-hub's social_crawler/logger.py, so
logs from both services read the same way. `bind_request_context()` /
`clear_request_context()` are used by the request-logging middleware to
attach a request_id to every log line emitted while handling one request,
without having to pass it explicitly through every function call."""

from __future__ import annotations

import logging

import structlog

from app.core.config import settings

_configured = False


def _configure_once() -> None:
    global _configured
    if _configured:
        return
    _configured = True

    logging.basicConfig(level=settings.log_level, format="%(message)s")

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # JSON in production (machine-parseable, ships to log aggregation
    # cleanly); a readable console renderer everywhere else - matching
    # spider-hub's dev-vs-prod split, just driven by settings here instead
    # of always-console since this runs as a long-lived server, not a CLI.
    renderer = structlog.processors.JSONRenderer() if settings.log_format == "json" else structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(settings.log_level)),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    _configure_once()
    return structlog.get_logger(name)


def bind_request_context(**kwargs: object) -> None:
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_request_context() -> None:
    structlog.contextvars.clear_contextvars()
