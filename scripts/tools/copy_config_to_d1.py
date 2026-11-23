"""Chép các bảng cấu hình từ Supabase (Postgres) sang D1 cinemark-config-db.

    python -m scripts.tools.copy_config_to_d1            # chỉ xem sẽ chép gì (không ghi)
    python -m scripts.tools.copy_config_to_d1 --apply    # chép thật

Chép mọi bảng mà cinemark-api/spider-hub đã có bản D1 (d1_store, d1_ops, d1_accounts), giữ nguyên `id` (lease proxy, cài đặt như
fb_public_discovery_proxy_id đang tham chiếu id). Chạy lại được nhiều lần: dòng trùng khoá chính bị ghi đè bằng dữ liệu mới nhất từ
Postgres; dòng chỉ có ở D1 không bị xoá. Chỉ chép các cột có ở cả hai bên (cột cũ không còn code nào dùng bị bỏ).
Sau khi chép xong mới đặt CONFIG_DB_BACKEND=d1 ở cinemark-api VÀ spider-hub (xem docs/config-db-d1.md)."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Table

from app.core.config import settings
from app.repositories.d1.sql import d1_insert_many
from app.services.accounts.config_db import d1_accounts, d1_ops, d1_store
from app.services.accounts.config_db.conn import _connect, close_pool

# Thứ tự không quan trọng (không có khoá ngoại), liệt kê theo nhóm.
TABLES: tuple[Table, ...] = (
    d1_store.ai_settings,
    d1_store.ai_providers,
    d1_store.ai_accuracy_runs,
    d1_store.proxy_settings,
    d1_ops.filter_keywords,
    d1_ops.crawl_schedules,
    d1_ops.comment_crawl_schedules,
    d1_ops.cleanup_settings,
    d1_ops.cleanup_run_history,
    d1_ops.discovery_settings,
    d1_ops.omar_api_settings,
    d1_accounts.platform_accounts,
    d1_accounts.platform_proxies,
    d1_accounts.proxy_providers,
    d1_accounts.account_events,
)


def to_d1(value: Any) -> Any:
    """Giá trị Postgres -> giá trị D1: bool -> 0/1, thời gian -> ISO UTC độ rộng cố định, jsonb -> chuỗi JSON."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, datetime):
        value = value if value.tzinfo else value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False)
    return value


async def _pg_rows(table: Table) -> tuple[list[str], list[dict[str, Any]]] | None:
    """Các cột dùng chung + dòng của bảng bên Postgres; None khi bảng chưa tồn tại bên đó."""
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT to_regclass(%s) AS t", (f"public.{table.name}",))
        if (await cur.fetchone())["t"] is None:
            return None
        await cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
            (table.name,),
        )
        have = {r["column_name"] for r in await cur.fetchall()}
        columns = [c.name for c in table.columns if c.name in have]
        await cur.execute(f"SELECT {', '.join(columns)} FROM {table.name}")
        return columns, await cur.fetchall()


async def main(apply: bool) -> None:
    settings.config_db_backend = "postgres"  # đọc nguồn từ Postgres dù biến môi trường đang là d1
    failed = False
    for table in TABLES:
        found = await _pg_rows(table)
        if found is None:
            print(f"{table.name:26} (không có bên Postgres, bỏ qua)")
            continue
        columns, rows = found
        print(f"{table.name:26} {len(rows):>5} dòng, cột: {len(columns)}")
        if not apply or not rows:
            continue
        converted = [{name: to_d1(row[name]) for name in columns} for row in rows]
        keys = [c.name for c in table.primary_key.columns]
        ok = await d1_insert_many(table, converted, conflict=keys, database="config", timeout=30.0)
        failed = failed or not ok
        if not ok:
            print(f"  LỖI khi ghi {table.name} sang D1")
    if not apply:
        print("\nChạy lại với --apply để chép sang D1.")
    elif failed:
        raise SystemExit("Có bảng chép lỗi - kiểm tra log rồi chạy lại.")
    else:
        print("\nĐã chép xong. Chạy lại không kèm --apply để đối chiếu số dòng bên Postgres.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    async def _run() -> None:
        try:
            if args.apply:
                for ensure in (d1_store.ensure_tables, d1_ops.ensure_tables, d1_accounts.ensure_tables):
                    await ensure()
            await main(args.apply)
        finally:
            await close_pool()

    asyncio.run(_run())
