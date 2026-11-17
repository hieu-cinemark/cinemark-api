"""Từ khoá lọc, lịch cào bài và lịch cào bình luận (giờ Việt Nam, sửa trên dashboard)."""

from __future__ import annotations

from typing import Any

import psycopg

from app.core.errors import ConflictError, NotFoundError
from app.core.logging import get_logger

logger = get_logger(__name__)
from app.services.accounts.config_db.conn import _connect, _get_pool, _trusted_sql  # noqa: F401

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
                _trusted_sql(
                    f"INSERT INTO filter_keywords ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                    f"RETURNING {FILTER_KEYWORD_COLUMNS}"
                ),
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
                _trusted_sql(
                    f"UPDATE filter_keywords SET {set_clause}, updated_at = now() WHERE id = %s "
                    f"RETURNING {FILTER_KEYWORD_COLUMNS}"
                ),
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
# dashboard, được vòng lặp trong tiến trình của app/services/crawl/scheduler/ đọc - xem
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
# comment_crawl_schedules: bật/tắt crawl comment theo nền tảng. Từ 2026-10-08 chạy MỖI GIỜ (xem _comments_tick trong
# app/services/crawl/scheduler/ và app/services/crawl/comment_planner.py): top_n = số bài nóng mỗi phim được theo dõi, run_time
# = giờ chạy lượt mẫu phân tầng (và 12 tiếng sau). last_triggered_date không còn dùng cho lượt mỗi giờ.

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
                top_n integer NOT NULL DEFAULT 15,
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
