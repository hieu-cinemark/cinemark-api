"""Đọc phần cuối của hai file log console (structlog) tạo nên pipeline crawl của
spider-hub, để dashboard có chỗ hiển thị "log này kia" mà không cần dựng hệ thống
chuyển log / lưu log vào DB mới:

- crawl_request_consumer.py của spider-hub (consumer.log bên đó) - Kafka consumer
  khởi chạy `scrapy crawl` cho mỗi message crawl_requests.
- ingest_consumer của chính service này (ingest_consumer.log) - Kafka consumer ghi
  bài đã crawl vào D1.

Cả hai đều là output text thường của ConsoleRenderer (có màu bằng mã ANSI, đọc được
trên terminal nhưng không đọc được trên trình duyệt) - _strip_ansi() làm sạch trước
khi trả về. Cố gắng hết mức có thể: file thiếu/không đọc được thì trả về danh sách
rỗng với ok=False thay vì lỗi 500, vì đây chỉ là tiện ích vận hành, không tính năng
nào dành cho người dùng phụ thuộc vào nó."""

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
