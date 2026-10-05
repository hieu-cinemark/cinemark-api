"""Client Postgres async (Supabase) cho các bảng platform_accounts / platform_proxies -
cùng các bảng mà package social_crawler/db/ của spider-hub đọc (xem __init__.py bên
đó để biết đầy đủ lý do: các giá trị này đổi quá thường xuyên, dùng env + restart
tiến trình thì không đáng). Phía này có quyền đọc VÀ ghi, phục vụ trang Settings của
dashboard; spider-hub chỉ đọc.

Mọi lần ghi ở đây dựng SQL từ một danh sách cột cố định, đã duyệt trước, cộng với giá
trị lấy từ một model Pydantic (không bao giờ từ dict request thô) - model mặc định bỏ
các trường lạ, nên chỗ gọi không bao giờ lén đưa được một tên cột tuỳ ý vào chuỗi
query."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from app.core.config import settings
from app.core.errors import ConflictError, NotFoundError, UpstreamError
from app.core.logging import get_logger

logger = get_logger(__name__)

ACCOUNT_COLUMNS = (
    "id, platform, account_id, password, totp_secret, cookie, token, email, email_password, "
    "enabled, created_at, updated_at, last_checked_at, last_check_status, last_check_note, "
    # Các cột pool / circuit-breaker + ghim proxy cố định - do services/pool.py & package
    # db/ của spider-hub ghi, phía này chỉ đọc (hệ thống ghi, không bao giờ nằm trong form
    # tài khoản) - xem ACCOUNT_CREATE_COLUMNS bên dưới, cố ý loại chúng ra. Cột gốc chỉ tên
    # là "status" - đặt alias thành pool_status để không lẫn với last_check_status ở trên
    # (một tín hiệu khác, kích hoạt bằng tay - xem app/services/account_health.py).
    "status AS pool_status, cooldown_until, consecutive_failures, last_used_at, assigned_proxy_id"
)
PROXY_COLUMNS = (
    "id, platform, proxy_url, username, password, login_use_proxy, enabled, created_at, updated_at, "
    "status AS pool_status, cooldown_until, consecutive_failures, last_used_at"
)

_pool_columns_ready = False


async def _ensure_pool_columns() -> None:
    """Thêm các cột pool/circuit-breaker + ghim proxy cố định (status, cooldown_until,
    consecutive_failures, last_used_at, assigned_proxy_id) nếu database Supabase này có
    từ trước services/pool.py của spider-hub - giống hệt các câu ALTER TABLE ... IF NOT
    EXISTS trong scripts/dev_db_schema.sql của spider-hub, để
    ACCOUNT_COLUMNS/PROXY_COLUMNS/_PROXY_LIST_COLUMNS (vốn SELECT các cột này không điều
    kiện) không làm cả trang Settings lỗi 500 với UndefinedColumn trên database chưa
    migrate. IF NOT EXISTS nên database đã migrate thì không làm gì."""
    global _pool_columns_ready
    if _pool_columns_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'"
        )
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS cooldown_until timestamptz")
        await cur.execute(
            "ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0"
        )
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS assigned_proxy_id int")
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'")
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS cooldown_until timestamptz")
        await cur.execute(
            "ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0"
        )
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
        await conn.commit()
    _pool_columns_ready = True


_ACCOUNT_CREATE_COLUMNS = (
    "platform",
    "account_id",
    "password",
    "totp_secret",
    "cookie",
    "token",
    "email",
    "email_password",
    "enabled",
)
_PROXY_CREATE_COLUMNS = ("platform", "proxy_url", "username", "password", "login_use_proxy", "enabled")


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
_pool: AsyncConnectionPool | None = None
_pool_lock = asyncio.Lock()


async def _get_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    if not settings.database_url:
        raise UpstreamError("DATABASE_URL is not configured on cinemark-api")
    async with _pool_lock:
        if _pool is None:
            pool = AsyncConnectionPool(
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
        logger.error("db_connect_failed", error=str(exc))
        raise UpstreamError("Could not connect to the settings database") from exc
    try:
        yield conn
    except BaseException as exc:
        if not await borrowed.__aexit__(type(exc), exc, exc.__traceback__):
            raise
    else:
        await borrowed.__aexit__(None, None, None)


# --- tài khoản ---------------------------------------------------------


_ACCOUNT_SECRET_COLUMNS = ("password", "totp_secret", "cookie", "token", "email_password")

# Như ACCOUNT_COLUMNS nhưng không kéo giá trị bí mật về: mỗi cột bí mật thành chuỗi rỗng
# kèm cờ has_<cột>. Cookie/token của vài chục tài khoản là phần lớn payload từ Supabase
# (~0.3s mỗi lần tải danh sách) mà danh sách trên dashboard vốn che hết (AccountOut.masked).
_ACCOUNT_LIST_COLUMNS = ACCOUNT_COLUMNS
for _col in _ACCOUNT_SECRET_COLUMNS:
    _ACCOUNT_LIST_COLUMNS = _ACCOUNT_LIST_COLUMNS.replace(
        f" {_col},", f" '' AS {_col}, (COALESCE({_col}, '') <> '') AS has_{_col},", 1
    )


async def list_accounts(platform: str | None = None, *, with_secrets: bool = True) -> list[dict[str, Any]]:
    await _ensure_pool_columns()
    columns = ACCOUNT_COLUMNS if with_secrets else _ACCOUNT_LIST_COLUMNS
    async with _connect() as conn, conn.cursor() as cur:
        if platform:
            await cur.execute(f"SELECT {columns} FROM platform_accounts WHERE platform = %s ORDER BY id", (platform,))
        else:
            await cur.execute(f"SELECT {columns} FROM platform_accounts ORDER BY platform, id")
        return await cur.fetchall()


async def get_account(account_id: int) -> dict[str, Any] | None:
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {ACCOUNT_COLUMNS} FROM platform_accounts WHERE id = %s", (account_id,))
        return await cur.fetchone()


async def create_account(fields: dict[str, Any]) -> dict[str, Any]:
    await _ensure_pool_columns()
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO platform_accounts ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                f"RETURNING {ACCOUNT_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(
            f"An account for {fields.get('platform')}/{fields.get('account_id')} already exists"
        ) from exc


async def update_account(account_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [account_id]
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET {set_clause}, updated_at = now() WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def update_account_check_result(account_id: int, *, status: str) -> dict[str, Any]:
    """Ghi kết quả của một lần kiểm tra sức khoẻ (xem app/services/account_health.py) - cố
    ý tách khỏi update_account ở trên: các cột này do hệ thống ghi từ một lần kiểm tra tự
    động, người dùng không bao giờ sửa qua form tài khoản, nên chúng hoàn toàn không nằm
    trong _ACCOUNT_CREATE_COLUMNS."""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET last_checked_at = now(), last_check_status = %s "
            f"WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (status, account_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def reset_account_proxy(account_id: int) -> dict[str, Any]:
    """Xoá ghim proxy cố định của một tài khoản (assigned_proxy_id -> NULL) - xem
    services/pool.acquire_proxy_for_account của spider-hub. Ở lượt crawl/bootstrap kế
    tiếp, tài khoản được ghim lại vào proxy đang có ít tài khoản nhất; dùng từ dashboard
    khi bỏ một proxy đang được ghim, hoặc để tự cân bằng lại sau khi thêm proxy mới vào
    pool."""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET assigned_proxy_id = NULL, updated_at = now() "
            f"WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (account_id,),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def set_account_proxy(account_id: int, proxy_id: int) -> dict[str, Any]:
    """Ghim tay một tài khoản vào một proxy cụ thể - phần tương ứng trên dashboard của logic
    tự ghim vào proxy ít tải nhất của spider-hub (xem
    services/pool.acquire_proxy_for_account), cho người vận hành muốn trực tiếp quyết
    định tài khoản nào nằm trên IP nào thay vì để pool tự cân bằng. Ràng buộc khoá ngoại
    trên assigned_proxy_id (xem scripts/dev_db_schema.sql của spider-hub) từ chối
    proxy_id không tồn tại - lỗi hiện ra ở đây là lỗi psycopg thường, không xử lý riêng,
    vì ô chọn proxy trên dashboard chỉ đưa ra id thật."""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET assigned_proxy_id = %s, updated_at = now() "
            f"WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (proxy_id, account_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def delete_account(account_id: int) -> None:
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_accounts WHERE id = %s", (account_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Account {account_id} not found")


_PROXY_LIST_COLUMNS = (
    "pp.id, pp.platform, pp.proxy_url, pp.username, pp.password, pp.login_use_proxy, pp.enabled, "
    "pp.created_at, pp.updated_at, pp.status AS pool_status, pp.cooldown_until, pp.consecutive_failures, "
    "pp.last_used_at, "
    # Số dòng platform_accounts còn sống đang được ghim cố định vào proxy này (đang bật,
    # không bị checkpoint) - khớp với get_least_loaded_proxy của spider-hub để con số "tài
    # khoản đã ghim" trên dashboard đúng bằng mức tải mà pool thực sự cân bằng theo. Các
    # ghim chết vẫn nằm trên assigned_proxy_id nhưng không được làm một IP trông như đã
    # đầy.
    "(SELECT count(*) FROM platform_accounts pa WHERE pa.assigned_proxy_id = pp.id "
    "AND pa.enabled = true AND pa.status != 'checkpoint') AS assigned_account_count"
)


async def list_proxies(platform: str | None = None) -> list[dict[str, Any]]:
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        if platform:
            await cur.execute(
                f"SELECT {_PROXY_LIST_COLUMNS} FROM platform_proxies pp WHERE pp.platform = %s ORDER BY pp.id",
                (platform,),
            )
        else:
            await cur.execute(f"SELECT {_PROXY_LIST_COLUMNS} FROM platform_proxies pp ORDER BY pp.platform, pp.id")
        return await cur.fetchall()


async def create_proxy(fields: dict[str, Any]) -> dict[str, Any]:
    await _ensure_pool_columns()
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO platform_proxies ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                f"RETURNING {PROXY_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(f"A proxy for {fields.get('platform')}/{fields.get('proxy_url')} already exists") from exc


async def update_proxy(proxy_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [proxy_id]
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_proxies SET {set_clause}, updated_at = now() WHERE id = %s RETURNING {PROXY_COLUMNS}",
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Proxy {proxy_id} not found")
    return row


async def delete_proxy(proxy_id: int) -> None:
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_proxies WHERE id = %s", (proxy_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Proxy {proxy_id} not found")


# --- từ khoá lọc -------------------------------------------------------
# filter_keywords: bộ lọc nội dung chung (không theo nền tảng) - các từ khoá liên quan
# tới phim so với spam/lạc đề, CRUD ở đây và spider-hub đọc để quyết định một bài/
# comment đã crawl có đáng giữ không. Cùng dạng "không ORM, danh sách cột cố định đã
# duyệt" như accounts/proxies ở trên.

FILTER_KEYWORD_COLUMNS = "id, keyword, category, enabled, created_at, updated_at"
_FILTER_KEYWORD_CREATE_COLUMNS = ("keyword", "category", "enabled")


async def list_filter_keywords(category: str | None = None) -> list[dict[str, Any]]:
    async with _connect() as conn, conn.cursor() as cur:
        if category:
            await cur.execute(
                f"SELECT {FILTER_KEYWORD_COLUMNS} FROM filter_keywords WHERE category = %s ORDER BY id",
                (category,),
            )
        else:
            await cur.execute(f"SELECT {FILTER_KEYWORD_COLUMNS} FROM filter_keywords ORDER BY category, id")
        return await cur.fetchall()


async def create_filter_keyword(fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _FILTER_KEYWORD_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO filter_keywords ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                f"RETURNING {FILTER_KEYWORD_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(f"A {fields.get('category')} keyword {fields.get('keyword')!r} already exists") from exc


async def update_filter_keyword(keyword_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _FILTER_KEYWORD_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [keyword_id]
    try:
        async with _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                f"UPDATE filter_keywords SET {set_clause}, updated_at = now() WHERE id = %s "
                f"RETURNING {FILTER_KEYWORD_COLUMNS}",
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError("A keyword with that category/text already exists") from exc
    if row is None:
        raise NotFoundError(f"Filter keyword {keyword_id} not found")
    return row


async def delete_filter_keyword(keyword_id: int) -> None:
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM filter_keywords WHERE id = %s", (keyword_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Filter keyword {keyword_id} not found")


# --- lịch crawl --------------------------------------------------------
# crawl_schedules: giờ crawl hằng ngày theo nền tảng, CRUD từ thẻ "Crawl schedule" trên
# dashboard, được vòng lặp trong tiến trình của app/services/scheduler.py đọc - xem
# comment của bảng này trong scripts/dev_db_schema.sql của spider-hub để biết đầy đủ lý
# do (thay thế cả một dòng crontab của hệ điều hành lẫn Cloudflare Cron Triggers của
# cinemark-scraper).

CRAWL_SCHEDULE_COLUMNS = "platform, run_time, enabled, last_triggered_date, nurture_before, nurture_after, updated_at"

_nurture_columns_ready = False


async def _ensure_crawl_schedule_nurture_columns() -> None:
    """Thêm nurture_before/after nếu database này được tạo trước khi có các cột đó
    (dev_db_schema.sql giờ đã có). IF NOT EXISTS nên database mới thì không làm gì."""
    global _nurture_columns_ready
    if _nurture_columns_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "ALTER TABLE crawl_schedules ADD COLUMN IF NOT EXISTS nurture_before boolean NOT NULL DEFAULT false"
        )
        await cur.execute(
            "ALTER TABLE crawl_schedules ADD COLUMN IF NOT EXISTS nurture_after boolean NOT NULL DEFAULT false"
        )
        await conn.commit()
    _nurture_columns_ready = True


async def list_crawl_schedules() -> list[dict[str, Any]]:
    await _ensure_crawl_schedule_nurture_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {CRAWL_SCHEDULE_COLUMNS} FROM crawl_schedules ORDER BY platform")
        return await cur.fetchall()


async def ensure_default_crawl_schedules(platforms: set[str]) -> None:
    """Gọi một lần lúc khởi động (xem app/main.py) cho mọi nền tảng đã đăng ký. Trước khi có
    bảng này, việc crawl Facebook/TikTok/Threads được bảo đảm bằng một crontab cố định /
    Cloudflare Cron Trigger bất kể setting trên dashboard; giờ một nền tảng không có dòng
    ở đây đơn giản là không bao giờ xuất hiện trong vòng lặp list_crawl_schedules() của
    scheduler.py và không bao giờ chạy nữa, mà không có gì báo ra lỗ hổng đó (xem
    docstring của upsert_crawl_schedule: "mọi nền tảng ban đầu đều không có dòng nào cho
    tới khi lịch của nó được lưu lần đầu"). Tạo sẵn cùng run_time='07:00'/enabled=true
    mà mặc định cột của bảng vốn đã có (scripts/dev_db_schema.sql), ON CONFLICT DO
    NOTHING để một nền tảng người vận hành đã cấu hình (hoặc cố ý tắt) từ dashboard không
    bao giờ bị đụng tới."""
    await _ensure_crawl_schedule_nurture_columns()
    async with _connect() as conn, conn.cursor() as cur:
        for platform in platforms:
            await cur.execute(
                "INSERT INTO crawl_schedules (platform) VALUES (%s) ON CONFLICT (platform) DO NOTHING",
                (platform,),
            )
        await conn.commit()


async def upsert_crawl_schedule(
    platform: str, *, run_time: str, enabled: bool, nurture_before: bool = False, nurture_after: bool = False
) -> dict[str, Any]:
    """Lần ghi phía dashboard - run_time/enabled/nurture_* do người dùng đặt. ON CONFLICT để
    dashboard không cần biết dòng của nền tảng đã có hay chưa (mọi nền tảng ban đầu đều
    không có dòng nào cho tới khi lịch của nó được lưu lần đầu)."""
    await _ensure_crawl_schedule_nurture_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"INSERT INTO crawl_schedules (platform, run_time, enabled, nurture_before, nurture_after) "
            f"VALUES (%s, %s, %s, %s, %s) "
            f"ON CONFLICT (platform) DO UPDATE SET run_time = EXCLUDED.run_time, enabled = EXCLUDED.enabled, "
            f"nurture_before = EXCLUDED.nurture_before, nurture_after = EXCLUDED.nurture_after, "
            f"updated_at = now() RETURNING {CRAWL_SCHEDULE_COLUMNS}",
            (platform, run_time, enabled, nurture_before, nurture_after),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def mark_crawl_schedule_triggered(platform: str, triggered_date: str) -> None:
    """Lần ghi chống chạy lặp của chính scheduler.py - xem comment của cột
    crawl_schedules.last_triggered_date. Không dành cho người dùng, không route nào gọi
    thẳng hàm này."""
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE crawl_schedules SET last_triggered_date = %s WHERE platform = %s",
            (triggered_date, platform),
        )
        await conn.commit()


# --- lịch crawl comment -------------------------------------------------
# comment_crawl_schedules: giờ "quét top comment" hằng ngày theo nền tảng - tách khỏi
# crawl_schedules (bài) ở trên vì lượt quét comment của một nền tảng chạy theo nhịp
# riêng, độc lập với lúc crawl bài của nền tảng đó. Tới run_time, với mỗi từ khoá đang
# bật của nền tảng, xếp hàng một lượt crawl comment (publish_comments_crawl_request
# trong app/clients/kafka.py) cho các bài top `top_n` theo tương tác của từ khoá mà vẫn
# chưa có comment nào được lưu (list_posts_needing_comments trong app/services/d1.py) -
# xem _comments_tick trong app/services/scheduler.py, đúng cùng dạng kiểm tra định kỳ/
# kích hoạt/chặn bằng last_triggered_date như _tick dùng cho crawl_schedules.

COMMENT_SCHEDULE_COLUMNS = "platform, run_time, enabled, top_n, last_triggered_date, updated_at"

_comment_schedule_ready = False


async def _ensure_comment_crawl_schedules_table() -> None:
    global _comment_schedule_ready
    if _comment_schedule_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS comment_crawl_schedules (
                platform text PRIMARY KEY,
                run_time text NOT NULL DEFAULT '08:00',
                enabled boolean NOT NULL DEFAULT false,
                top_n integer NOT NULL DEFAULT 100,
                last_triggered_date date,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.commit()
    _comment_schedule_ready = True


async def list_comment_crawl_schedules() -> list[dict[str, Any]]:
    await _ensure_comment_crawl_schedules_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {COMMENT_SCHEDULE_COLUMNS} FROM comment_crawl_schedules ORDER BY platform")
        return await cur.fetchall()


async def ensure_default_comment_crawl_schedules(platforms: set[str]) -> None:
    """Cùng lý do như ensure_default_crawl_schedules ở trên - một nền tảng không có dòng ở
    đây đơn giản là không bao giờ xuất hiện trong vòng lặp của _comments_tick, mà không
    có gì báo ra lỗ hổng đó. ON CONFLICT DO NOTHING để một nền tảng người vận hành đã cấu
    hình (hoặc cố ý tắt) không bao giờ bị đụng tới."""
    await _ensure_comment_crawl_schedules_table()
    async with _connect() as conn, conn.cursor() as cur:
        for platform in platforms:
            await cur.execute(
                "INSERT INTO comment_crawl_schedules (platform) VALUES (%s) ON CONFLICT (platform) DO NOTHING",
                (platform,),
            )
        await conn.commit()


async def upsert_comment_crawl_schedule(platform: str, *, run_time: str, enabled: bool, top_n: int) -> dict[str, Any]:
    """Lần ghi phía dashboard - ON CONFLICT để dashboard không cần biết dòng của nền tảng
    này đã có hay chưa."""
    await _ensure_comment_crawl_schedules_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO comment_crawl_schedules (platform, run_time, enabled, top_n)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (platform) DO UPDATE SET
                run_time = EXCLUDED.run_time, enabled = EXCLUDED.enabled, top_n = EXCLUDED.top_n, updated_at = now()
            RETURNING {COMMENT_SCHEDULE_COLUMNS}
            """,
            (platform, run_time, enabled, top_n),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def mark_comment_crawl_schedule_triggered(platform: str, triggered_date: str) -> None:
    """Lần ghi chống chạy lặp của chính scheduler.py - xem comment của cột
    comment_crawl_schedules.last_triggered_date ở trên."""
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE comment_crawl_schedules SET last_triggered_date = %s WHERE platform = %s",
            (triggered_date, platform),
        )
        await conn.commit()


# --- AI settings -------------------------------------------------------
# Dòng singleton (id=1): prompt hệ thống theo từng task mà tab AI trong Settings của
# dashboard sửa. Được đọc ở mỗi lời gọi Kira (cache ngắn trong app.ai.kira) nên lưu
# xong là áp dụng ngay, không cần restart ingest/spider-hub.

AI_SETTINGS_COLUMNS = "id, enabled, model, prompts, active_report_provider, updated_at"

_ai_settings_ready = False


async def _ensure_ai_settings_table() -> None:
    global _ai_settings_ready
    if _ai_settings_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                enabled boolean NOT NULL DEFAULT false,
                model text NOT NULL DEFAULT '',
                prompts jsonb NOT NULL DEFAULT '{}'::jsonb,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # Thêm ngày 2026-09-25 vào một bảng đã có trên production, nên câu CREATE TABLE IF NOT
        # EXISTS trần ở trên sẽ không tự thêm cột này về sau - cột chọn provider nào
        # (call_bee hay call_kira trong app/ai/tasks/report.py) tạo social_topic_reports, đổi
        # được từ dashboard mà không đụng tới thông tin đăng nhập trong ai_providers (xem
        # docstring của app/ai/tasks/report.py để biết vì sao có cột này: khi đó Kira gần như
        # ngồi không trên production - task LLM duy nhất còn lại có lượng gọi đáng kể là tạo
        # report, vốn bị gán cứng cho Bee).
        await cur.execute(
            "ALTER TABLE ai_settings ADD COLUMN IF NOT EXISTS active_report_provider text NOT NULL DEFAULT 'kira'"
        )
        await cur.execute(
            """
            INSERT INTO ai_settings (id, enabled, model, prompts)
            VALUES (1, %s, %s, '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
            """,
            (bool(settings.kira_enabled), ""),
        )
        await conn.commit()
    _ai_settings_ready = True


async def get_ai_settings() -> dict[str, Any]:
    await _ensure_ai_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {AI_SETTINGS_COLUMNS} FROM ai_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {
        "id": 1,
        "enabled": False,
        "model": "",
        "prompts": {},
        "active_report_provider": "kira",
        "updated_at": None,
    }


async def upsert_ai_settings(*, enabled: bool, prompts: dict[str, str], active_report_provider: str) -> dict[str, Any]:
    """Không còn ghi model ở đây - xem ai_providers bên dưới, nơi giữ
    base_url/api_key/model theo từng provider. Cột ai_settings.model được để nguyên
    (không đụng tới khi conflict) thay vì xoá, để đây không phải thay đổi schema mang
    tính phá huỷ; không còn gì đọc nó nữa."""
    from psycopg.types.json import Json

    await _ensure_ai_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO ai_settings (id, enabled, prompts, active_report_provider)
            VALUES (1, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                prompts = EXCLUDED.prompts,
                active_report_provider = EXCLUDED.active_report_provider,
                updated_at = now()
            RETURNING {AI_SETTINGS_COLUMNS}
            """,
            (enabled, Json(prompts), active_report_provider),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


# --- Thông tin đăng nhập AI provider ------------------------------------
# Mỗi LLM provider một dòng (key = "kira", "bee", ... - thêm provider mới bằng cách
# chèn một dòng mới, không cần đổi schema): base_url/api_key/model dùng để dựng client
# tương thích OpenAI của provider đó (xem app/ai/client.py). Trước đây là các biến env
# KIRA_API_KEY/KIRA_BASE_URL/BEEKNOEE_API_KEY/BEEKNOEE_BASE_URL - chuyển về đây (xem
# scripts/migrate_ai_provider_credentials.py) để xoay key hoặc thêm provider mới được
# từ dashboard mà không cần deploy lại.

AI_PROVIDER_COLUMNS = "key, base_url, api_key, model, updated_at"

_ai_providers_ready = False


async def _ensure_ai_providers_table() -> None:
    global _ai_providers_ready
    if _ai_providers_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_providers (
                key text PRIMARY KEY,
                base_url text NOT NULL DEFAULT '',
                api_key text NOT NULL DEFAULT '',
                model text NOT NULL DEFAULT '',
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.commit()
    _ai_providers_ready = True


async def list_ai_providers() -> list[dict[str, Any]]:
    await _ensure_ai_providers_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {AI_PROVIDER_COLUMNS} FROM ai_providers ORDER BY key")
        return await cur.fetchall()


async def get_ai_provider(key: str) -> dict[str, Any] | None:
    await _ensure_ai_providers_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {AI_PROVIDER_COLUMNS} FROM ai_providers WHERE key = %s", (key,))
        return await cur.fetchone()


async def upsert_ai_provider(key: str, *, base_url: str, api_key: str | None, model: str) -> dict[str, Any]:
    """api_key=None thì giữ nguyên secret đã lưu - cho dashboard đổi base_url/model mà
    không phải gửi lại secret mỗi lần."""
    await _ensure_ai_providers_table()
    async with _connect() as conn, conn.cursor() as cur:
        if api_key is None:
            await cur.execute(
                f"""
                INSERT INTO ai_providers (key, base_url, model)
                VALUES (%s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    base_url = EXCLUDED.base_url,
                    model = EXCLUDED.model,
                    updated_at = now()
                RETURNING {AI_PROVIDER_COLUMNS}
                """,
                (key, base_url, model),
            )
        else:
            await cur.execute(
                f"""
                INSERT INTO ai_providers (key, base_url, api_key, model)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    base_url = EXCLUDED.base_url,
                    api_key = EXCLUDED.api_key,
                    model = EXCLUDED.model,
                    updated_at = now()
                RETURNING {AI_PROVIDER_COLUMNS}
                """,
                (key, base_url, api_key, model),
            )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


# --- Setting hành vi proxy -----------------------------------------------
# proxy_settings: jsonb singleton chứa các tham số tinh chỉnh (xem ProxySettings trong
# app/schemas/settings.py cho key/mặc định/giới hạn). proxy_providers: mỗi gói proxy
# xoay vòng của nhà cung cấp một dòng (URL API + token + chế độ ip_allowlist). Cả hai
# đều được social_crawler/db/proxy_settings.py của spider-hub đọc (cache 60s bên đó,
# nên lưu ở đây thì các crawler đang chạy áp dụng trong vòng một phút).

_proxy_settings_ready = False


async def _ensure_proxy_settings_tables() -> None:
    global _proxy_settings_ready
    if _proxy_settings_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS proxy_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                settings jsonb NOT NULL DEFAULT '{}'::jsonb,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS proxy_providers (
                key text PRIMARY KEY,
                api_url text NOT NULL DEFAULT '',
                token text NOT NULL DEFAULT '',
                ip_allowlist boolean NOT NULL DEFAULT false,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.commit()
    _proxy_settings_ready = True


async def get_proxy_settings() -> dict[str, Any]:
    """{"settings": {...chỉ các key đã lưu...}, "updated_at": ...} - việc trộn lên trên giá
    trị mặc định là việc của schema (ProxySettings)."""
    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT settings, updated_at FROM proxy_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {"settings": {}, "updated_at": None}


async def upsert_proxy_settings(values: dict[str, Any]) -> dict[str, Any]:
    from psycopg.types.json import Json

    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO proxy_settings (id, settings) VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = now()
            RETURNING settings, updated_at
            """,
            (Json(values),),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


PROXY_PROVIDER_COLUMNS = "key, api_url, token, ip_allowlist, updated_at"


async def list_proxy_providers() -> list[dict[str, Any]]:
    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {PROXY_PROVIDER_COLUMNS} FROM proxy_providers ORDER BY key")
        return await cur.fetchall()


async def upsert_proxy_provider(key: str, *, api_url: str, token: str | None, ip_allowlist: bool) -> dict[str, Any]:
    """token=None thì giữ nguyên token đã lưu (cùng hợp đồng với api_key của
    upsert_ai_provider)."""
    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        if token is None:
            await cur.execute(
                f"""
                INSERT INTO proxy_providers (key, api_url, ip_allowlist) VALUES (%s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    api_url = EXCLUDED.api_url, ip_allowlist = EXCLUDED.ip_allowlist, updated_at = now()
                RETURNING {PROXY_PROVIDER_COLUMNS}
                """,
                (key, api_url, ip_allowlist),
            )
        else:
            await cur.execute(
                f"""
                INSERT INTO proxy_providers (key, api_url, token, ip_allowlist) VALUES (%s, %s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    api_url = EXCLUDED.api_url, token = EXCLUDED.token,
                    ip_allowlist = EXCLUDED.ip_allowlist, updated_at = now()
                RETURNING {PROXY_PROVIDER_COLUMNS}
                """,
                (key, api_url, token, ip_allowlist),
            )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


# cleanup_settings: jsonb singleton chứa các tham số sửa được trên dashboard cho lượt
# dọn bài không liên quan (xem app/services/cleanup.py + scheduler.py).
# cleanup_run_history: log chỉ-thêm của mọi lượt chạy (theo lịch hoặc bấm tay từ
# /settings/cleanup/run) kèm số dòng đã xoá theo từng bảng + hàng tồn còn lại - cùng
# dạng mà cleanup.py vốn trả về trong bộ nhớ, chỉ là được lưu lại để dashboard hiện "lần
# trước đã xảy ra gì" mà không phải truy lại log của API.

_cleanup_settings_ready = False


async def _ensure_cleanup_settings_tables() -> None:
    global _cleanup_settings_ready
    if _cleanup_settings_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cleanup_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                settings jsonb NOT NULL DEFAULT '{}'::jsonb,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cleanup_run_history (
                id bigserial PRIMARY KEY,
                started_at timestamptz NOT NULL DEFAULT now(),
                finished_at timestamptz,
                dry_run boolean NOT NULL DEFAULT false,
                triggered_by text NOT NULL DEFAULT 'schedule',
                posts_deleted integer NOT NULL DEFAULT 0,
                comments_deleted integer NOT NULL DEFAULT 0,
                snapshots_deleted integer NOT NULL DEFAULT 0,
                batches integer NOT NULL DEFAULT 0,
                remaining_posts integer,
                error text
            )
            """
        )
        await cur.execute(
            "CREATE INDEX IF NOT EXISTS cleanup_run_history_started_at_idx ON cleanup_run_history (started_at DESC)"
        )
    _cleanup_settings_ready = True


async def get_cleanup_settings() -> dict[str, Any]:
    """{"settings": {...chỉ các key đã lưu...}, "updated_at": ...} - việc trộn lên trên giá
    trị mặc định của CleanupSettings là việc của schema."""
    await _ensure_cleanup_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT settings, updated_at FROM cleanup_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {"settings": {}, "updated_at": None}


async def upsert_cleanup_settings(values: dict[str, Any]) -> dict[str, Any]:
    """Chỉ ghi các key có trong `values` - key thiếu giữ giá trị đã lưu trước đó. Cho
    PUT /settings/cleanup hoạt động như cập nhật một phần thay vì phải gửi mọi key."""
    from psycopg.types.json import Json

    await _ensure_cleanup_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT settings FROM cleanup_settings WHERE id = 1")
        existing = await cur.fetchone()
        merged: dict[str, Any] = dict((existing or {}).get("settings") or {})
        for key, value in values.items():
            if value is None:
                continue
            merged[key] = value
        await cur.execute(
            """
            INSERT INTO cleanup_settings (id, settings) VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = now()
            RETURNING settings, updated_at
            """,
            (Json(merged),),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def record_cleanup_run_start(*, dry_run: bool, triggered_by: str) -> int:
    await _ensure_cleanup_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO cleanup_run_history (dry_run, triggered_by) VALUES (%s, %s) RETURNING id",
            (dry_run, triggered_by),
        )
        row = await cur.fetchone()
        await conn.commit()
    return int(row["id"])  # type: ignore[arg-type]


async def record_cleanup_run_finish(
    run_id: int,
    *,
    summary: dict[str, Any],
    error: str | None = None,
) -> None:
    await _ensure_cleanup_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE cleanup_run_history SET
                finished_at = now(),
                posts_deleted = %s,
                comments_deleted = %s,
                snapshots_deleted = %s,
                batches = %s,
                remaining_posts = %s,
                error = %s
            WHERE id = %s
            """,
            (
                int(summary.get("posts") or 0),
                int(summary.get("comments") or 0),
                int(summary.get("snapshots") or 0),
                int(summary.get("batches") or 0),
                summary.get("remaining_posts"),
                error,
                run_id,
            ),
        )
        await conn.commit()


async def list_cleanup_run_history(limit: int = 20) -> list[dict[str, Any]]:
    await _ensure_cleanup_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT id, started_at, finished_at, dry_run, triggered_by,
                   posts_deleted, comments_deleted, snapshots_deleted,
                   batches, remaining_posts, error
            FROM cleanup_run_history
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return await cur.fetchall()


# auto_login_settings: jsonb singleton chứa các tham số sửa được trên dashboard cho bộ
# lập lịch auto-login mỗi giờ (xem app/services/auto_login.py + auto_login/consumer.py
# của spider-hub). Cùng dạng với cleanup_settings (singleton id=1 + settings JSON +
# updated_at) để ngữ nghĩa PUT ("chỉ các key trong payload thay đổi") giống hệt nhau
# trên cả trang.
#
# auto_login_run_history: log chỉ-thêm của mọi lượt mà bộ lập lịch hoặc endpoint /run
# bấm tay kích hoạt - cùng kiểu với cleanup_run_history, nhưng theo dõi nền tảng nào đã
# được xử lý, bao nhiêu tài khoản được thử trên mỗi nền tảng, bao nhiêu trả về
# relogged_in / needs_human / failed / error, và bộ lập lịch bên dưới có thực sự
# publish request cho từng tài khoản không (lỗi publish Kafka được theo dõi riêng - xem
# cờ `telegram_alert` trong setting lịch, đối chiếu với dòng log
# "auto_login_request_publish_failed" của auto_login.py).

_auto_login_settings_ready = False


async def _ensure_auto_login_tables() -> None:
    global _auto_login_settings_ready
    if _auto_login_settings_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_login_settings (
                id integer PRIMARY KEY CHECK (id = 1),
                settings jsonb NOT NULL DEFAULT '{}'::jsonb,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_login_run_history (
                id bigserial PRIMARY KEY,
                started_at timestamptz NOT NULL DEFAULT now(),
                finished_at timestamptz,
                triggered_by text NOT NULL DEFAULT 'schedule',
                dry_run boolean NOT NULL DEFAULT false,
                -- Snapshot of the settings used for THIS run - captures
                -- the operator's intent at tick-time, not whatever the
                -- settings table currently says. Lets the dashboard
                -- render "the last scheduled tick was a dry-run"
                -- without having to dereference the settings row.
                interval_seconds integer NOT NULL,
                platforms text NOT NULL,
                -- Per-platform attempted / by_status. JSONB so we don't
                -- need a schema change to track a new outcome (Facebook
                -- today, Threads adds `needs_2fa` tomorrow, ...).
                per_platform jsonb NOT NULL DEFAULT '{}'::jsonb,
                total_attempted integer NOT NULL DEFAULT 0,
                total_relogged_in integer NOT NULL DEFAULT 0,
                total_needs_human integer NOT NULL DEFAULT 0,
                total_failed integer NOT NULL DEFAULT 0,
                total_error integer NOT NULL DEFAULT 0,
                kafka_published integer NOT NULL DEFAULT 0,
                kafka_publish_failed integer NOT NULL DEFAULT 0,
                error text
            )
            """
        )
        # Bản đầu tiên tạo total_relogged_in kiểu boolean, nên không ghi được tổng số nguyên
        # của record_auto_login_run_finish vào đó.
        await cur.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'auto_login_run_history'
                      AND column_name = 'total_relogged_in' AND data_type = 'boolean'
                ) THEN
                    ALTER TABLE auto_login_run_history ALTER COLUMN total_relogged_in DROP DEFAULT;
                    ALTER TABLE auto_login_run_history
                        ALTER COLUMN total_relogged_in TYPE integer USING total_relogged_in::integer;
                    ALTER TABLE auto_login_run_history ALTER COLUMN total_relogged_in SET DEFAULT 0;
                END IF;
            END $$
            """
        )
        await cur.execute(
            "CREATE INDEX IF NOT EXISTS auto_login_run_history_started_at_idx ON auto_login_run_history (started_at DESC)"
        )
    _auto_login_settings_ready = True


async def get_auto_login_settings() -> dict[str, Any]:
    await _ensure_auto_login_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT settings, updated_at FROM auto_login_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {"settings": {}, "updated_at": None}


async def upsert_auto_login_settings(values: dict[str, Any]) -> dict[str, Any]:
    """Cập nhật một phần: chỉ ghi các key có trong `values`; key thiếu giữ giá trị đã lưu
    trước đó. Cho PUT của dashboard hoạt động như một nút bật/tắt (chỉ gửi
    `{enabled: true}`) hoặc sửa toàn bộ (gửi mọi key) mà không cái nào đè cái nào."""
    from psycopg.types.json import Json

    await _ensure_auto_login_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT settings FROM auto_login_settings WHERE id = 1")
        existing = await cur.fetchone()
        merged: dict[str, Any] = dict((existing or {}).get("settings") or {})
        for key, value in values.items():
            if value is None:
                continue
            merged[key] = value
        await cur.execute(
            """
            INSERT INTO auto_login_settings (id, settings) VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = now()
            RETURNING settings, updated_at
            """,
            (Json(merged),),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def record_auto_login_run_start(
    *,
    triggered_by: str,
    dry_run: bool,
    interval_seconds: int,
    platforms: list[str],
) -> int:
    await _ensure_auto_login_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO auto_login_run_history
                (triggered_by, dry_run, interval_seconds, platforms)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (triggered_by, dry_run, interval_seconds, ",".join(platforms)),
        )
        row = await cur.fetchone()
        await conn.commit()
    return int(row["id"])  # type: ignore[arg-type]


async def record_auto_login_run_finish(
    run_id: int,
    *,
    per_platform: dict[str, dict[str, int]],
    kafka_published: int,
    kafka_publish_failed: int,
    error: str | None = None,
) -> None:
    """Ghi chi tiết theo nền tảng + bộ đếm publish Kafka + thông báo lỗi tuỳ chọn. Cùng dạng
    mà auto_login.py vốn trả về trong bộ nhớ, chỉ là được lưu lại để bảng lịch sử trên
    dashboard hiện "lần trước đã xảy ra gì" mà không phải truy lại log của API."""
    from psycopg.types.json import Json

    total_attempted = sum(p.get("attempted", 0) for p in per_platform.values())
    total_relogged_in = sum(p.get("relogged_in", 0) for p in per_platform.values())
    total_needs_human = sum(p.get("needs_human", 0) for p in per_platform.values())
    total_failed = sum(p.get("failed", 0) for p in per_platform.values())
    total_error = sum(p.get("error", 0) for p in per_platform.values())
    await _ensure_auto_login_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE auto_login_run_history SET
                finished_at = now(),
                per_platform = %s,
                total_attempted = %s,
                total_relogged_in = %s,
                total_needs_human = %s,
                total_failed = %s,
                total_error = %s,
                kafka_published = %s,
                kafka_publish_failed = %s,
                error = %s
            WHERE id = %s
            """,
            (
                Json(per_platform),
                total_attempted,
                total_relogged_in,
                total_needs_human,
                total_failed,
                total_error,
                kafka_published,
                kafka_publish_failed,
                error,
                run_id,
            ),
        )
        await conn.commit()


async def list_auto_login_run_history(limit: int = 20) -> list[dict[str, Any]]:
    await _ensure_auto_login_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT id, started_at, finished_at, triggered_by, dry_run,
                   interval_seconds, platforms, per_platform,
                   total_attempted, total_relogged_in, total_needs_human,
                   total_failed, total_error,
                   kafka_published, kafka_publish_failed, error
            FROM auto_login_run_history
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return await cur.fetchall()
