"""Pool kết nối Postgres (Supabase) dùng chung cho mọi bảng cấu hình."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, LiteralString, cast

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from app.core.config import settings
from app.core.errors import UpstreamError
from app.core.logging import get_logger

logger = get_logger(__name__)


# Một pool dùng chung cho cả tiến trình thay vì mở kết nối mới cho mỗi query: mỗi lần
# connect tới Supabase (TLS + auth qua pooler) mất ~1.6s từ VN, nên mọi trang Settings
# từng tốn 2.5-3s mỗi request. Pool giữ sẵn vài kết nối ấm.
#
# DATABASE_URL trỏ tới transaction pooler của Supabase (cổng 6543, pgbouncer) - các kết
# nối phía sau được chia sẻ giữa các transaction, nên prepared statement phía server
# không dùng được: prepare_threshold=None tắt hẳn (psycopg mặc định tự prepare một query
# sau 5 lần chạy, trước đây không bao giờ chạm tới vì mỗi kết nối chỉ chạy 1-2 query).
_POOL_MIN_SIZE = 1
_POOL_MAX_SIZE = 6
_POOL_ACQUIRE_TIMEOUT_SECONDS = 10.0
_pool: AsyncConnectionPool[Any] | None = None
_pool_lock = asyncio.Lock()


async def _get_pool() -> AsyncConnectionPool[Any]:
    global _pool
    if _pool is not None:
        return _pool
    if not settings.database_url:
        raise UpstreamError("DATABASE_URL is not configured on cinemark-api")
    async with _pool_lock:
        if _pool is None:
            pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
                settings.database_url,
                min_size=_POOL_MIN_SIZE,
                max_size=_POOL_MAX_SIZE,
                timeout=_POOL_ACQUIRE_TIMEOUT_SECONDS,
                # Đóng kết nối rảnh lâu để không giữ socket chết sau khi laptop ngủ/đổi
                # mạng; keepalive bên dưới phát hiện socket chết trong ~1 phút.
                max_idle=120,
                max_lifetime=1800,
                open=False,
                kwargs={
                    "row_factory": dict_row,
                    "prepare_threshold": None,
                    # Mỗi round trip tới pooler ~0.25s; ở chế độ transaction mặc định một
                    # SELECT tốn 3 lượt (BEGIN, query, COMMIT). Không hàm nào ở đây cần
                    # nhiều câu lệnh trong cùng một transaction (không có FOR UPDATE; các
                    # hàm nhiều câu chỉ là DDL IF NOT EXISTS hoặc đọc-rồi-ghi một dòng
                    # settings), nên autocommit cắt mỗi request còn một lượt.
                    "autocommit": True,
                    "connect_timeout": 5,
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 3,
                },
            )
            # wait=False: mở pool không được chặn request đầu tiên (hay startup) quá lâu khi
            # Supabase chậm - kết nối được tạo nền, connection() sẽ chờ tối đa timeout.
            await pool.open(wait=False)
            _pool = pool
    return _pool


def _trusted_sql(query: str) -> LiteralString:
    """Query ghép động chỉ từ tên cột trong whitelist (_*_CREATE_COLUMNS, AI_PROVIDER_EXTRA_FIELDS...) - giá trị
    luôn đi qua placeholder %s - nên an toàn dù type checker không chứng minh được là LiteralString."""
    return cast(LiteralString, query)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        pool, _pool = _pool, None
        await pool.close()


@asynccontextmanager
async def _connect() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """Mượn một kết nối (autocommit, xem _get_pool) từ pool, trả lại khi khối thoát - các
    lời gọi conn.commit() sẵn có thành no-op. Chỉ lỗi lúc *lấy* kết nối
    mới bị đổi thành UpstreamError - lỗi của chính query (UniqueViolation...) đi nguyên
    ra cho nơi gọi tự xử lý như cũ."""
    pool = await _get_pool()
    borrowed = pool.connection()
    try:
        conn = await borrowed.__aenter__()
    except (PoolTimeout, psycopg.OperationalError) as exc:
        logger.error("db_connect_failed", error=exc)
        raise UpstreamError("Could not connect to the settings database") from exc
    try:
        yield conn
    except BaseException as exc:
        if not await borrowed.__aexit__(type(exc), exc, exc.__traceback__):
            raise
    else:
        await borrowed.__aexit__(None, None, None)
