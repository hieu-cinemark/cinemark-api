"""Route cấu hình AI trong Settings: công tắc và prompt, nhà cung cấp, nhà cung cấp đang chọn, danh sách model."""

from __future__ import annotations

import re

from fastapi import APIRouter

from app.core.errors import NotFoundError, UpstreamError, ValidationError
from app.core.logging import get_logger
from app.schemas.settings import (
    AiActiveProviderUpdate,
    AiModelsOut,
    AiModelsRequest,
    AiProviderOut,
    AiProviderUpdate,
    AiSettingsOut,
    AiSettingsUpdate,
)
from app.services.accounts.config_db import ai_config as ai_config_db

logger = get_logger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])


async def _ai_settings_out(row: dict) -> AiSettingsOut:
    from app.ai.prompts.defaults import AI_PROMPT_TASKS, default_system_prompts
    from app.ai.providers.client import active_provider_key

    active = await active_provider_key()
    provider = await ai_config_db.get_ai_provider(active) if active else None
    defaults = default_system_prompts()
    stored = value if isinstance(value := row.get("prompts"), dict) else {}
    prompts = []
    for task in AI_PROMPT_TASKS:
        default = defaults.get(task, "")
        current = str(stored.get(task) or "").strip() or default
        prompts.append(
            {
                "task": task,
                "system_prompt": current,
                "default_system_prompt": default,
            }
        )
    return AiSettingsOut(
        enabled=bool(row.get("enabled")),
        # Lấy thẳng từ ai_providers - rỗng nghĩa là "chưa đặt", không bao giờ đoán.
        model=str((provider or {}).get("model") or ""),
        configured=bool(provider and provider.get("base_url") and provider.get("api_key")),
        prompts=prompts,
        active_provider=active,
        updated_at=row.get("updated_at"),
    )


@router.get("/ai", response_model=AiSettingsOut)
async def get_ai_settings() -> AiSettingsOut:
    row = await ai_config_db.get_ai_settings()
    return await _ai_settings_out(row)


@router.put("/ai", response_model=AiSettingsOut)
async def set_ai_settings(payload: AiSettingsUpdate) -> AiSettingsOut:
    """Công tắc bật/tắt AI cho các bộ phân loại lúc ingest và prompt theo từng task. Nhà cung cấp và model chọn ở /ai/providers."""
    from app.ai.prompts.defaults import AI_PROMPT_TASKS
    from app.ai.runtime import invalidate_ai_runtime_cache

    allowed = set(AI_PROMPT_TASKS)
    prompts = {key: value for key, value in payload.prompts.items() if key in allowed and isinstance(value, str)}
    row = await ai_config_db.upsert_ai_settings(enabled=payload.enabled, prompts=prompts)
    invalidate_ai_runtime_cache()
    logger.info("ai_settings_updated", enabled=payload.enabled, prompt_tasks=sorted(prompts))
    return await _ai_settings_out(row)


def _ai_provider_out(row: dict, active: str = "") -> AiProviderOut:
    return AiProviderOut(
        key=row["key"],
        base_url=row.get("base_url") or "",
        api_key_set=bool(row.get("api_key")),
        model=row.get("model") or "",
        reasoning_effort=row.get("reasoning_effort") or "",
        active=bool(active) and row["key"] == active,
        updated_at=row.get("updated_at"),
    )


@router.put("/ai/active", response_model=AiSettingsOut)
async def set_active_ai_provider(payload: AiActiveProviderUpdate) -> AiSettingsOut:
    """Chọn nhà cung cấp cho MỌI lời gọi AI - cinemark-api, spider-hub (đọc D1) và Worker cinemark
    (qua ai_runtime của D1) đều theo trong vòng một phút."""
    from app.ai.providers.client import invalidate_provider_cache
    from app.ai.runtime import invalidate_ai_runtime_cache
    from app.services.ops.ai_runtime_sync import sync_ai_runtime

    row = await ai_config_db.get_ai_provider(payload.provider)
    if not row or not row.get("base_url") or not row.get("api_key") or not row.get("model"):
        raise ValidationError(f"Provider {payload.provider} is missing base URL / API key / model")
    await ai_config_db.set_active_ai_provider(payload.provider)
    invalidate_provider_cache()
    invalidate_ai_runtime_cache()
    await sync_ai_runtime()
    logger.info("ai_active_provider_set", provider=payload.provider)
    return await _ai_settings_out(await ai_config_db.get_ai_settings())


@router.get("/ai/providers", response_model=list[AiProviderOut])
async def list_ai_providers() -> list[AiProviderOut]:
    """Mọi nhà cung cấp AI đã thêm ({key, base_url, model, active} - không bao giờ trả về api_key, chỉ cho biết đã đặt hay chưa)."""
    from app.ai.providers.client import active_provider_key

    active = await active_provider_key()
    return [_ai_provider_out(row, active) for row in await ai_config_db.list_ai_providers()]


@router.put("/ai/providers/{key}", response_model=AiProviderOut)
async def set_ai_provider(key: str, payload: AiProviderUpdate) -> AiProviderOut:
    """Thêm hoặc sửa một nhà cung cấp (`key` mới thì tạo). Bỏ trống api_key thì giữ key đã lưu. Nhà cung cấp đầu tiên đủ thông
    tin khi chưa có nhà cung cấp nào được chọn thì tự được chọn."""
    from app.ai.providers.client import active_provider_key, invalidate_provider_cache
    from app.ai.runtime import invalidate_ai_runtime_cache
    from app.services.ops.ai_runtime_sync import sync_ai_runtime

    if not re.fullmatch(r"[a-z0-9_-]{1,40}", key):
        raise ValidationError(
            "Provider name may only contain lowercase letters, digits, hyphens and underscores (max 40 characters)"
        )
    api_key = payload.api_key.strip() if payload.api_key and payload.api_key.strip() else None
    row = await ai_config_db.upsert_ai_provider(
        key,
        base_url=payload.base_url.strip(),
        api_key=api_key,
        model=payload.model.strip(),
        reasoning_effort=payload.reasoning_effort,
    )
    active = await active_provider_key()
    if not active and row.get("base_url") and row.get("api_key") and row.get("model"):
        await ai_config_db.set_active_ai_provider(key)
        active = key
    invalidate_provider_cache()
    invalidate_ai_runtime_cache()
    await sync_ai_runtime()
    logger.info("ai_provider_updated", key=key, base_url=payload.base_url.strip(), model=payload.model.strip())
    return _ai_provider_out(row, active)


@router.delete("/ai/providers/{key}", status_code=204)
async def delete_ai_provider(key: str) -> None:
    """Xoá một nhà cung cấp. Không xoá được nhà cung cấp đang chọn - chọn nhà cung cấp khác trước."""
    from app.ai.providers.client import active_provider_key, invalidate_provider_cache
    from app.services.ops.ai_runtime_sync import sync_ai_runtime

    if key == await active_provider_key():
        raise ValidationError("This provider is currently in use - select another provider before deleting it")
    if not await ai_config_db.delete_ai_provider(key):
        raise NotFoundError(f"Provider {key} not found")
    invalidate_provider_cache()
    await sync_ai_runtime()
    logger.info("ai_provider_deleted", key=key)


@router.post("/ai/models", response_model=AiModelsOut)
async def list_ai_models(payload: AiModelsRequest) -> AiModelsOut:
    """Danh sách model của một nhà cung cấp (GET {base_url}/models) để chọn ở form. api_key trống thì dùng key đã lưu của `key`."""
    import httpx

    api_key = (payload.api_key or "").strip()
    if not api_key and payload.key:
        api_key = str((await ai_config_db.get_ai_provider(payload.key) or {}).get("api_key") or "")
    if not api_key:
        raise ValidationError("An API key is required (or choose a provider with a saved key) to list models")
    url = payload.base_url.strip().rstrip("/")
    url = url.removesuffix("/chat/completions")
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
            resp = await client.get(f"{url}/models", headers={"Authorization": f"Bearer {api_key}"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        raise UpstreamError(f"Could not fetch the model list from {url}/models ({type(exc).__name__})") from exc
    items = data.get("data") if isinstance(data, dict) else data
    ids = sorted({str(item.get("id")) for item in items or [] if isinstance(item, dict) and item.get("id")})
    return AiModelsOut(models=ids)
