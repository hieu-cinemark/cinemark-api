"""Cấu hình AI: ai_settings (bật/tắt, prompt, provider đang dùng), ai_providers (base_url, key, model, reasoning_effort) và lịch sử đo độ chính xác.

Dữ liệu ở D1 (xem d1_store.py)."""

from __future__ import annotations

import time
from typing import Any

from app.services.accounts.config_db import d1_store

# Cache ngắn trong tiến trình: mọi lời gọi AI đều đọc dòng này (kèm cả prompts). Ghi trong tiến trình này thì xoá cache
# ngay; tiến trình khác (web <-> scheduler) thấy thay đổi trong tối đa _AI_SETTINGS_TTL giây.
_AI_SETTINGS_TTL = 30.0
_ai_settings_cache: tuple[float, dict[str, Any]] | None = None


def _forget_ai_settings() -> None:
    global _ai_settings_cache
    _ai_settings_cache = None


async def get_ai_settings() -> dict[str, Any]:
    global _ai_settings_cache
    if _ai_settings_cache is not None and time.monotonic() - _ai_settings_cache[0] < _AI_SETTINGS_TTL:
        return _ai_settings_cache[1]
    row = await d1_store.get_ai_settings()
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
    await d1_store.set_active_ai_provider(key)
    _forget_ai_settings()


async def upsert_ai_settings(*, enabled: bool, prompts: dict[str, str]) -> dict[str, Any]:
    """Công tắc AI và prompt theo task. Nhà cung cấp/model ở ai_providers + active_provider."""
    row = await d1_store.upsert_ai_settings(enabled_flag=enabled, prompts=prompts)
    _forget_ai_settings()
    return row


# --- Thông tin đăng nhập AI provider ------------------------------------
# Mỗi LLM provider một dòng (key = "bee", ...): base_url/api_key/model dùng để dựng client tương thích OpenAI của provider
# đó (xem app/ai/providers/client.py). Nằm ở DB (không phải env) để xoay key hoặc thêm provider từ dashboard mà không deploy.


async def list_ai_providers() -> list[dict[str, Any]]:
    return await d1_store.list_ai_providers()


async def get_ai_provider(key: str) -> dict[str, Any] | None:
    return await d1_store.get_ai_provider(key)


async def upsert_ai_provider(
    key: str, *, base_url: str, api_key: str | None, model: str, reasoning_effort: str | None = None
) -> dict[str, Any]:
    """api_key=None thì giữ nguyên secret đã lưu - cho dashboard đổi base_url/model mà không phải gửi lại secret mỗi lần.
    reasoning_effort=None thì giữ giá trị đã lưu."""
    fields = {"base_url": base_url, "model": model}
    if reasoning_effort is not None:
        fields["reasoning_effort"] = reasoning_effort
    if api_key is not None:
        fields["api_key"] = api_key
    return await d1_store.upsert_ai_provider(key, fields)


async def delete_ai_provider(key: str) -> bool:
    return await d1_store.delete_ai_provider(key)


# --- Lịch sử đo độ chính xác (ai_accuracy_runs) -------------------------------------------------------
# Mỗi lần chấm độ chính xác của máy so với nhãn người là một dòng; report Social Topic gắn lần mới nhất vào
# dashboard_data["accuracy"] để người đọc biết nên tin các con số tới đâu.


async def insert_accuracy_run(*, labeled_at: str, sample_size: int, metrics: dict[str, Any], note: str | None) -> None:
    await d1_store.insert_accuracy_run(labeled_at=labeled_at, sample_size=sample_size, metrics=metrics, note=note)


async def get_latest_accuracy_run() -> dict[str, Any] | None:
    return await d1_store.get_latest_accuracy_run()
