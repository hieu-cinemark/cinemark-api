"""Cấu hình AI: ai_settings (bật/tắt, prompt, provider đang dùng), ai_providers (base_url, key, model, reasoning_effort) và lịch sử đo độ chính xác."""

from __future__ import annotations

import time
from typing import Any

from app.core.logging import get_logger

logger = get_logger(__name__)
from app.services.accounts.config_db.conn import _connect, _get_pool, _trusted_sql  # noqa: F401

AI_SETTINGS_COLUMNS = "id, enabled, prompts, active_provider, updated_at"

_ai_settings_ready = False

# Cache ngắn trong tiến trình: mọi lời gọi AI đều đọc dòng này (kèm cả prompts) nên là nguồn egress Supabase lớn nhất. Ghi trong
# tiến trình này thì xoá cache ngay; tiến trình khác (web <-> scheduler) thấy thay đổi trong tối đa _AI_SETTINGS_TTL giây.
_AI_SETTINGS_TTL = 30.0
_ai_settings_cache: tuple[float, dict[str, Any]] | None = None


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
        # Nhà cung cấp đang dùng cho MỌI lời gọi AI (khoá một dòng ai_providers), rỗng = chưa chọn. cinemark-api, spider-hub và
        # Worker cinemark (qua bảng ai_runtime của D1) đều theo.
        await cur.execute("ALTER TABLE ai_settings ADD COLUMN IF NOT EXISTS active_provider text NOT NULL DEFAULT ''")
        await cur.execute(
            """
            INSERT INTO ai_settings (id, enabled, model, prompts)
            VALUES (1, %s, %s, '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
            """,
            (False, ""),
        )
        await conn.commit()
    _ai_settings_ready = True


def _forget_ai_settings() -> None:
    global _ai_settings_cache
    _ai_settings_cache = None


async def get_ai_settings() -> dict[str, Any]:
    global _ai_settings_cache
    if _ai_settings_cache is not None and time.monotonic() - _ai_settings_cache[0] < _AI_SETTINGS_TTL:
        return _ai_settings_cache[1]
    await _ensure_ai_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {AI_SETTINGS_COLUMNS} FROM ai_settings WHERE id = 1")
        row = await cur.fetchone()
    if row:
        _ai_settings_cache = (time.monotonic(), row)
    return row or {
        "id": 1,
        "enabled": False,
        "prompts": {},
        "active_provider": "",
        "updated_at": None,
    }


async def set_active_ai_provider(key: str) -> None:
    await _ensure_ai_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE ai_settings SET active_provider = %s, updated_at = now() WHERE id = 1", (key,))
        await conn.commit()
    _forget_ai_settings()


async def upsert_ai_settings(*, enabled: bool, prompts: dict[str, str]) -> dict[str, Any]:
    """Công tắc AI và prompt theo task. Nhà cung cấp/model ở ai_providers + active_provider. Các cột cũ (model,
    active_report_provider) được để nguyên trong bảng cho khỏi phải đổi schema phá huỷ; không còn gì đọc chúng."""
    from psycopg.types.json import Json

    await _ensure_ai_settings_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO ai_settings (id, enabled, prompts)
            VALUES (1, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                prompts = EXCLUDED.prompts,
                updated_at = now()
            RETURNING {AI_SETTINGS_COLUMNS}
            """,
            (enabled, Json(prompts)),
        )
        row = await cur.fetchone()
        await conn.commit()
    _forget_ai_settings()
    return row  # type: ignore[return-value]


# --- Thông tin đăng nhập AI provider ------------------------------------
# Mỗi LLM provider một dòng (key = "bee", ... - thêm provider mới bằng cách
# chèn một dòng mới, không cần đổi schema): base_url/api_key/model dùng để dựng client
# tương thích OpenAI của provider đó (xem app/ai/providers/client.py). Nằm ở DB (không phải env) để xoay key hoặc
# thêm provider mới được từ dashboard mà không cần deploy lại.

# reasoning_effort (16/11/2026): mức suy luận gửi kèm cho model suy luận (Meta Muse: minimal/low/...), rỗng = không gửi.
AI_PROVIDER_COLUMNS = "key, base_url, api_key, model, reasoning_effort, updated_at"
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
        await cur.execute("ALTER TABLE ai_providers ADD COLUMN IF NOT EXISTS reasoning_effort text NOT NULL DEFAULT ''")
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


async def upsert_ai_provider(
    key: str, *, base_url: str, api_key: str | None, model: str, reasoning_effort: str | None = None
) -> dict[str, Any]:
    """api_key=None thì giữ nguyên secret đã lưu - cho dashboard đổi base_url/model mà không phải gửi lại secret mỗi lần.
    reasoning_effort=None thì giữ giá trị đã lưu."""
    await _ensure_ai_providers_table()
    fields = {"base_url": base_url, "model": model}
    if reasoning_effort is not None:
        fields["reasoning_effort"] = reasoning_effort
    if api_key is not None:
        fields["api_key"] = api_key
    columns = ", ".join(fields)
    placeholders = ", ".join(["%s"] * len(fields))
    updates = ", ".join(f"{name} = EXCLUDED.{name}" for name in fields)
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            _trusted_sql(f"""
            INSERT INTO ai_providers (key, {columns}) VALUES (%s, {placeholders})
            ON CONFLICT (key) DO UPDATE SET {updates}, updated_at = now()
            RETURNING {AI_PROVIDER_COLUMNS}
            """),
            (key, *fields.values()),
        )
        row = await cur.fetchone()
        await conn.commit()
    return row  # type: ignore[return-value]


async def delete_ai_provider(key: str) -> bool:
    await _ensure_ai_providers_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM ai_providers WHERE key = %s", (key,))
        deleted = cur.rowcount > 0
        await conn.commit()
    return deleted


# --- Bảng ai_accuracy_runs --------------------------------------------------------------------------
# Mỗi lần chấm độ chính xác của máy (scripts/ml/accuracy_sample.py score --save) so với nhãn người làm một dòng: độ
# chính xác cảm xúc, F1 từng nhãn, F1 khía cạnh, độ chính xác giai đoạn khán giả. Report Social Topic gắn lần mới
# nhất vào dashboard_data["accuracy"] để người đọc biết nên tin các con số tới đâu (học từ Kompa).

_accuracy_runs_ready = False


async def _ensure_accuracy_runs_table() -> None:
    global _accuracy_runs_ready
    if _accuracy_runs_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_accuracy_runs (
                id bigserial PRIMARY KEY,
                labeled_at date NOT NULL,
                sample_size integer NOT NULL,
                metrics jsonb NOT NULL,
                note text,
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.commit()
    _accuracy_runs_ready = True


async def insert_accuracy_run(*, labeled_at: str, sample_size: int, metrics: dict[str, Any], note: str | None) -> None:
    from psycopg.types.json import Json

    await _ensure_accuracy_runs_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO ai_accuracy_runs (labeled_at, sample_size, metrics, note) VALUES (%s, %s, %s, %s)",
            (labeled_at, sample_size, Json(metrics), note),
        )
        await conn.commit()


async def get_latest_accuracy_run() -> dict[str, Any] | None:
    await _ensure_accuracy_runs_table()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT labeled_at, sample_size, metrics, note, created_at FROM ai_accuracy_runs ORDER BY id DESC LIMIT 1"
        )
        return await cur.fetchone()
