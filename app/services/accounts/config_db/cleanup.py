"""Cài đặt và lịch sử lượt dọn bài không liên quan (cleanup_settings, cleanup_run_history)."""

from __future__ import annotations

from typing import Any

from app.core.logging import get_logger

logger = get_logger(__name__)
from app.services.accounts.config_db import d1_ops, d1_store
from app.services.accounts.config_db.conn import _connect, _get_pool, _trusted_sql  # noqa: F401

# cleanup_settings: jsonb singleton chứa các tham số sửa được trên dashboard cho lượt
# dọn bài không liên quan (xem app/services/relevance/cleanup.py + scheduler.py).
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
    if d1_store.enabled():
        return await d1_ops.get_cleanup_settings()
    await _ensure_cleanup_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT settings, updated_at FROM cleanup_settings WHERE id = 1")
        row = await cur.fetchone()
    return row or {"settings": {}, "updated_at": None}


async def upsert_cleanup_settings(values: dict[str, Any]) -> dict[str, Any]:
    """Chỉ ghi các key có trong `values` - key thiếu giữ giá trị đã lưu trước đó. Cho
    PUT /settings/cleanup hoạt động như cập nhật một phần thay vì phải gửi mọi key."""
    if d1_store.enabled():
        return await d1_ops.upsert_cleanup_settings(values)
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
    if d1_store.enabled():
        return await d1_ops.record_cleanup_run_start(dry_run=dry_run, triggered_by=triggered_by)
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
    if d1_store.enabled():
        await d1_ops.record_cleanup_run_finish(run_id, summary=summary, error=error)
        return
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
    if d1_store.enabled():
        return await d1_ops.list_cleanup_run_history(limit)
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
