"""Tab Omar trên spider-hub-dashboard: hạn mức của key, lượt đang chạy, tiến độ từng hashtag, nút Chạy ngay / Dừng
(app/services/omar_tiktok.py). Key lưu/đổi qua GET/PUT /settings/omar-api (app/api/routes/settings.py)."""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter

from app.core.config import settings
from app.core.logging import get_logger
from app.services import omar_tiktok
from app.services.platform_config_db import get_omar_api_settings

logger = get_logger(__name__)

router = APIRouter(prefix="/omar", tags=["omar"])


async def _usage(api_key: str) -> dict[str, Any] | None:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(f"{settings.omar_api_base_url.rstrip('/')}/usage", headers={"X-API-Key": api_key})
        data = (resp.json() or {}).get("data") or {}
        return {
            "plan": data.get("plan"),
            "status": data.get("status"),
            "expires_at": data.get("expires_at"),
            **(data.get("usage") or {}),
        }
    except Exception as exc:  # noqa: BLE001 - dashboard vẫn hiện phần còn lại
        logger.warning("omar_status_usage_failed", error=str(exc)[:200])
        return None


@router.get("/status")
async def omar_status() -> dict[str, Any]:
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    progress = await omar_tiktok.hashtag_progress()
    return {
        "key_set": bool(api_key),
        "usage": await _usage(api_key) if api_key else None,
        "round": omar_tiktok.round_status(),
        "mode": "spread_daily" if settings.omar_spread_daily else "continuous",
        "hashtags": progress,
        "totals": {
            "hashtags": len(progress),
            "finished": sum(1 for row in progress if row["done_at"]),
            "started": sum(1 for row in progress if row["pages"] > 0),
            "pages": sum(row["pages"] for row in progress),
        },
    }


@router.post("/run")
async def omar_run() -> dict[str, bool]:
    from app.services.scheduler import trigger_omar_now

    started = trigger_omar_now()
    logger.info("omar_manual_run", started=started)
    return {"started": started}


@router.post("/stop")
async def omar_stop() -> dict[str, bool]:
    stopping = omar_tiktok.request_stop()
    logger.info("omar_manual_stop", stopping=stopping)
    return {"stopping": stopping}
