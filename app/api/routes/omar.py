"""Tab Omar trên spider-hub-dashboard: hạn mức của key, lượt đang chạy, tiến độ từng hashtag, nút Chạy ngay / Dừng
(app/services/omar_tiktok.py). Key lưu/đổi qua GET/PUT /settings/omar-api (app/api/routes/settings.py)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.config import settings
from app.core.errors import UpstreamError, ValidationError
from app.core.logging import get_logger
from app.services import omar_channels, omar_comments, omar_search, omar_tiktok
from app.services.d1 import get_or_create_keyword, set_keyword_enabled
from app.services.platform_config_db import get_omar_api_settings

logger = get_logger(__name__)

router = APIRouter(prefix="/omar", tags=["omar"])


@router.get("/status")
async def omar_status() -> dict[str, Any]:
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    progress = await omar_tiktok.hashtag_progress()
    return {
        "key_set": bool(api_key),
        "usage": await omar_tiktok.fetch_usage(api_key) if api_key else None,
        "round": omar_tiktok.round_status(),
        "mode": "spread_daily" if settings.omar_spread_daily else "continuous",
        "hashtags": progress,
        "channels": await omar_channels.channel_status(),
        "comments": {
            **await omar_comments.job_status(),
            "weekday": settings.omar_comments_weekday,
            "time": settings.omar_comments_time,
            "top_per_hashtag": settings.omar_comments_top_per_hashtag,
        },
        "totals": {
            "hashtags": len(progress),
            "finished": sum(1 for row in progress if row["done_at"]),
            "started": sum(1 for row in progress if row["pages"] > 0),
            "pages": sum(row["pages"] for row in progress),
            "tiktok_videos": sum(row["video_count"] or 0 for row in progress),
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


@router.post("/comments/run")
async def omar_comments_run() -> dict[str, Any]:
    """Xếp hàng bình luận tuần ngay (không đợi mốc tuần) rồi mở lượt Omar nếu đang rảnh - lượt đang chạy thì hàng đợi được
    xử lý ở lượt kế tiếp."""
    from app.services.scheduler import trigger_omar_now

    job = await omar_comments.enqueue_weekly_job(trigger="manual")
    started = trigger_omar_now()
    logger.info("omar_comments_manual_run", videos=job["videos"], started=started)
    return {
        "queued": job["videos"],
        "pending": (await omar_comments.job_status())["pending_videos"],
        "started": started,
    }


class OmarHashtagRequest(BaseModel):
    movie_id: str
    hashtag: str


@router.post("/hashtags")
async def omar_add_hashtag(payload: OmarHashtagRequest) -> dict[str, Any]:
    """Ô "Thêm hashtag" ở tab Omar: người dùng gõ hashtag cho một phim -> kiểm tra có video trên TikTok, tạo (hoặc bật lại)
    từ khoá TikTok, rồi cào ngay tới hết (omar_tiktok.crawl_hashtag_now). Spider TikTok và các lượt Omar sau dùng từ khoá
    này như mọi từ khoá khác."""
    tag = payload.hashtag.strip().lstrip("#").replace(" ", "")
    if len(tag) < 2:
        raise ValidationError("Hashtag không hợp lệ")
    exists = await omar_tiktok.hashtag_exists(tag)
    if exists is False:
        raise ValidationError(f"#{tag} không có video nào trên TikTok")
    row = await get_or_create_keyword(payload.movie_id, "tiktok", f"#{tag}")
    if row is None:
        raise UpstreamError("Không tạo được từ khoá (phim không tồn tại / đang tắt, hoặc D1 lỗi)")
    await set_keyword_enabled("tiktok", row["id"], True)
    started = omar_tiktok.crawl_hashtag_now(row)
    logger.info("omar_hashtag_added", hashtag=row["keyword"], movie_id=payload.movie_id, started=started)
    return {"keyword": row, "started": started, "verified": exists is True}


@router.get("/search")
async def omar_search_queries() -> dict[str, Any]:
    """Câu tìm theo từ khoá (tên phim, không dấu, câu tìm Facebook/Threads, câu tự thêm) và kết quả lần tìm gần nhất."""
    return {
        "queries": await omar_search.query_status(),
        "modes": omar_search.search_modes(),
        "pages": settings.omar_search_pages,
    }


class OmarSearchRequest(BaseModel):
    movie_id: str
    query: str


@router.post("/search")
async def omar_add_search(payload: OmarSearchRequest) -> dict[str, Any]:
    """Thêm câu tìm cho một phim rồi tìm ngay; các ngày sau tìm cùng các câu khác trong lượt Omar."""
    query = payload.query.strip()
    if len(query) < 3:
        raise ValidationError("Câu tìm quá ngắn")
    row = await omar_search.add_custom_query(payload.movie_id, query)
    found = next((q for q in await omar_search.build_queries() if q.get("id") == row["id"]), None)
    if found is None:
        await omar_search.delete_custom_query(row["id"])
        raise ValidationError("Phim chưa có hashtag TikTok đang bật, hoặc câu tìm trùng câu đã có")
    omar_search.search_now(found)
    logger.info("omar_search_added", query=query, movie_id=payload.movie_id)
    return {"query": found}


@router.delete("/search/{query_id}")
async def omar_delete_search(query_id: str) -> dict[str, bool]:
    await omar_search.delete_custom_query(query_id)
    return {"ok": True}
