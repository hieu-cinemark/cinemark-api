"""Chép các bảng cấu hình từ Supabase (Postgres) sang D1 cinemark-config-db: ai_settings, ai_providers, ai_accuracy_runs, proxy_settings.

    python -m scripts.tools.copy_config_to_d1            # chỉ xem sẽ chép gì (không ghi)
    python -m scripts.tools.copy_config_to_d1 --apply    # chép thật

Chạy lại được nhiều lần: ai_settings/ai_providers/proxy_settings ghi đè theo khoá; ai_accuracy_runs chỉ chép khi D1 còn trống (tránh
nhân đôi). Sau khi chép xong mới đặt CONFIG_DB_BACKEND=d1 ở cinemark-api VÀ spider-hub (xem docs/config-db-d1.md)."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from app.core.config import settings
from app.services.accounts.config_db import ai_config, d1_store
from app.services.accounts.config_db import proxy_settings as proxy_config
from app.services.accounts.config_db.conn import _connect, close_pool


async def _pg_accuracy_runs() -> list[dict[str, Any]]:
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("SELECT labeled_at, sample_size, metrics, note FROM ai_accuracy_runs ORDER BY id")
        return await cur.fetchall()


async def main(apply: bool) -> None:
    settings.config_db_backend = "postgres"  # đọc nguồn từ Postgres dù biến môi trường đang là d1
    ai = await ai_config.get_ai_settings()
    providers = await ai_config.list_ai_providers()
    proxy = (await proxy_config.get_proxy_settings()).get("settings") or {}
    runs = await _pg_accuracy_runs()
    print(
        f"Postgres: ai_settings enabled={ai.get('enabled')} active={ai.get('active_provider')!r}, "
        f"{len(providers)} ai_providers, {len(proxy)} khoá proxy_settings, {len(runs)} ai_accuracy_runs"
    )
    if not apply:
        print("Chạy lại với --apply để chép sang D1.")
        return

    await d1_store.ensure_tables()
    await d1_store.upsert_ai_settings(enabled_flag=bool(ai.get("enabled")), prompts=ai.get("prompts") or {})
    if ai.get("active_provider"):
        await d1_store.set_active_ai_provider(ai["active_provider"])
    for row in providers:
        fields = {name: row[name] or "" for name in ("base_url", "api_key", "model", "reasoning_effort")}
        await d1_store.upsert_ai_provider(row["key"], fields)
    if proxy:
        await d1_store.upsert_proxy_settings(proxy)
    if runs and await d1_store.get_latest_accuracy_run() is None:
        for run in runs:
            metrics = run["metrics"] if isinstance(run["metrics"], dict) else json.loads(run["metrics"])
            await d1_store.insert_accuracy_run(
                labeled_at=str(run["labeled_at"]), sample_size=run["sample_size"], metrics=metrics, note=run["note"]
            )
    print("Đã chép xong. Kiểm tra lại bằng cách chạy script không kèm --apply và so với D1.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    async def _run() -> None:
        try:
            await main(args.apply)
        finally:
            await close_pool()

    asyncio.run(_run())
