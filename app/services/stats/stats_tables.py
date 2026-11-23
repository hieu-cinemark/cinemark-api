"""Bảng thống kê theo ngày (platform_daily, keyword_daily) và các hàm dùng chung: tạo bảng, truy vấn, cảnh báo khi rollup lỗi liên tiếp."""

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import (
    Column,
    Integer,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import ClauseElement

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.clients.telegram import send_telegram_message
from app.core.logging import get_logger
from app.repositories.d1.sql import compile_sql, metadata

logger = get_logger(__name__)

_ready = False

_ready_lock = asyncio.Lock()

# Lượt đối soát lỗi (D1 quá tải) để số liệu trên trang Overview đứng yên - báo Telegram một lần khi lỗi lặp lại trong cửa
# sổ trượt, để D1 sập kéo dài không spam kênh sau khi đã báo một lần.
_ROLLUP_FAILURE_ALERT_THRESHOLD = 5

_ROLLUP_FAILURE_WINDOW_SECONDS = 3600


async def note_rollup_failure(table: str, **context: Any) -> None:
    logger.error("stats_rollup_write_failed", table=table, **context)
    try:
        client = get_redis_client()
        key = f"{REDIS_KEY_PREFIX}stats_rollup_failed:{table}"
        count = await client.incr(key)
        if count == 1:
            await client.expire(key, _ROLLUP_FAILURE_WINDOW_SECONDS)
    except Exception as exc:  # noqa: BLE001 - chỉ là bộ đếm cảnh báo, không được làm hỏng lượt đối soát
        logger.warning("stats_rollup_failure_count_failed", error=exc)
        return
    if count == _ROLLUP_FAILURE_ALERT_THRESHOLD:
        details = " | ".join(f"{k}={v}" for k, v in context.items())
        await send_telegram_message(
            f"🚨 Stats rollup write failing: {table}\n"
            f"{count} failures in the last {_ROLLUP_FAILURE_WINDOW_SECONDS // 60}m - dashboard counts are "
            f"drifting.\nNext reconcile retries in 10 minutes.\n{details}"
        )


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except TypeError, ValueError:
        return 0


async def run_stats_query(
    sql: str | ClauseElement, params: list[Any] | None = None, *, quiet: bool = False, timeout: float = 10.0
) -> list[dict[str, Any]] | None:
    """Mọi câu của module đi qua đây (test vá hàm này). Nhận chuỗi SQL hoặc câu dựng bằng SQLAlchemy."""
    # Import lười: repositories gọi vào module này từ persist_*/get_*.
    from app.clients.d1 import d1_query

    if not isinstance(sql, str):
        sql, params = compile_sql(sql)
    return await d1_query(sql, params, quiet=quiet, timeout=timeout)


def _stats_table(name: str, key: str) -> Table:
    key_columns = [Column("day", Text, primary_key=True), Column(key, Text, primary_key=True)]
    if key == "keyword_id":
        key_columns.append(Column("platform", Text, nullable=False))
    return Table(
        name,
        metadata,
        *key_columns,
        Column("posts", Integer, nullable=False, server_default=text("0")),
        Column("comments", Integer, nullable=False, server_default=text("0")),
        Column("last_scraped_at", Text),
    )


# Bảng tổng hợp theo ngày (UTC) cho dashboard: theo nền tảng và theo từ khoá.
platform_daily = _stats_table("stats_platform_daily", "platform")

keyword_daily = _stats_table("stats_keyword_daily", "keyword_id")

SQL_TODAY = func.date("now")

SQL_YESTERDAY = func.date("now", "-1 day")


async def ensure_stats_tables() -> None:
    """Tạo các bảng tổng hợp một lần mỗi tiến trình nếu còn thiếu."""
    global _ready
    if _ready:
        return
    async with _ready_lock:
        if _ready:
            return
        for table in (platform_daily, keyword_daily):
            await run_stats_query(CreateTable(table, if_not_exists=True), quiet=True)
        _ready = True
