"""Tails the two structlog console log files that make up spider-hub's
crawl pipeline, so the dashboard has somewhere to show "log này kia"
without needing a new log-shipping/DB-backed logging system:

- spider-hub's crawl_request_consumer.py (consumer.log there) - the Kafka
  consumer that launches `scrapy crawl` per crawl_requests message.
- this service's own ingest_consumer (ingest_consumer.log) - the Kafka
  consumer that writes scraped posts into D1.

Both are plain text ConsoleRenderer output (colored via ANSI escapes,
readable in a terminal but not in a browser) - _strip_ansi() cleans that up
before returning lines. Best-effort: a missing/unreadable file returns an
empty list with ok=False rather than a 500, since these are ops
conveniences, not something any user-facing feature depends on."""

from __future__ import annotations

import re
from collections import deque
from pathlib import Path

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.core.config import settings

router = APIRouter(prefix="/logs", tags=["logs"])

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


class LogTailResponse(BaseModel):
    ok: bool
    source: str
    lines: list[str]


def _tail(path_str: str, lines: int) -> LogTailResponse | None:
    path = Path(path_str)
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8", errors="replace") as f:
        last_lines = deque(f, maxlen=lines)
    cleaned = [_ANSI_RE.sub("", line).rstrip("\n") for line in last_lines]
    return LogTailResponse(ok=True, source=str(path), lines=cleaned)


@router.get("/spider-hub", response_model=LogTailResponse)
async def spider_hub_log(lines: int = Query(default=200, ge=1, le=2000)) -> LogTailResponse:
    result = _tail(settings.spider_hub_consumer_log_path, lines)
    if result is None:
        return LogTailResponse(ok=False, source=settings.spider_hub_consumer_log_path, lines=[])
    return result


@router.get("/ingest", response_model=LogTailResponse)
async def ingest_log(lines: int = Query(default=200, ge=1, le=2000)) -> LogTailResponse:
    result = _tail(settings.ingest_consumer_log_path, lines)
    if result is None:
        return LogTailResponse(ok=False, source=settings.ingest_consumer_log_path, lines=[])
    return result
