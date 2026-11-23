"""Tab Omar trên spider-hub-dashboard: hạn mức của key, lượt đang chạy, tiến độ từng hashtag, nút Chạy ngay / Dừng
(app/services/omar/omar_tiktok.py). Key, tổng request mỗi ngày, số request đồng thời lưu/đổi qua GET/PUT /settings/omar-api
(app/api/routes/settings.py)."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.clients.omar import OmarStop
from app.core.config import settings
from app.core.errors import NotFoundError, UpstreamError, ValidationError
from app.core.logging import get_logger
from app.repositories.d1.keywords import get_enabled_keywords, get_or_create_keyword, set_keyword_enabled
from app.services.accounts.config_db.omar import get_omar_api_settings, omar_limits
from app.services.omar import omar_comments, omar_fetch, omar_hashtag, omar_hashtag_stats, omar_pause, omar_tiktok

logger = get_logger(__name__)

router = APIRouter(prefix="/omar", tags=["omar"])

EXISTS_CHECK_SECONDS = 8.0


@router.get("/status")
async def omar_status() -> dict[str, Any]:
    """Trạng thái tab Omar cho dashboard: key đã đặt chưa, hạn mức của gói (GET /usage), tổng request mỗi ngày / số request đồng
    thời đang đặt, lượt đang chạy, tiến độ từng hashtag và bình luận, trạng thái tạm dừng. Các nguồn độc lập (D1, Redis, API
    Omar) được gọi song song."""
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()

    async def _usage() -> dict[str, Any] | None:
        return await omar_tiktok.fetch_usage(api_key) if api_key else None

    progress, usage, round_status, paused, comments, limits = await asyncio.gather(
        omar_hashtag.hashtag_progress(),
        _usage(),
        omar_tiktok.round_status_full(),
        omar_pause.get_pause(),
        omar_comments.job_status(),
        omar_limits(),
    )
    return {
        "key_set": bool(api_key),
        # Key dùng được = /usage đọc được. Chưa có key thì None (chưa kiểm tra).
        "key_valid": (usage is not None) if api_key else None,
        "usage": usage,
        # Giới hạn người dùng đặt (0 = theo gói / mặc định): trần request/phút, tổng request mỗi ngày, số request đồng thời.
        "requests_per_minute": int((row or {}).get("requests_per_minute") or 0),
        "daily_requests": limits["daily_requests"],
        "concurrency": limits["concurrency"],
        "default_concurrency": settings.omar_concurrency,
        "round": round_status,
        # Đang dừng chờ hạn mức / key mới: {"reason": "quota"|"key", "until": iso, ...} hoặc None.
        "paused": paused,
        "mode": "continuous",
        "hashtags": progress,
        "comments": {
            **comments,
            "cadence": settings.omar_comments_cadence,
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
    from app.services.crawl.scheduler import trigger_omar_now

    # Bấm "Chạy ngay" là bỏ trạng thái dừng (vd. vừa nâng gói / đổi key) - lượt tự kiểm tra lại hạn mức.
    await omar_pause.clear_pause()
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
    from app.services.crawl.scheduler import trigger_omar_now

    job = await omar_comments.enqueue_weekly_job(trigger="manual")
    started = trigger_omar_now()
    logger.info("omar_comments_manual_run", videos=job["videos"], started=started)
    return {
        "queued": job["videos"],
        "pending": (await omar_comments.job_status())["pending_videos"],
        "started": started,
    }


class OmarCommentKeywordsRequest(BaseModel):
    keyword_ids: list[str]
    # None = giữ nguyên tuỳ chọn đã lưu.
    all_videos: bool | None = None


@router.get("/comments/keywords")
async def omar_comment_keywords() -> dict[str, Any]:
    """Ô chọn từ khoá cào bình luận ở tab Omar: danh sách đã chọn (rỗng = mọi hashtag TikTok đang bật) + tuỳ chọn cào
    toàn bộ video của các từ khoá đó."""
    return {
        "keyword_ids": await omar_comments.selected_keyword_ids(),
        "all_videos": await omar_comments.all_videos_enabled(),
    }


@router.put("/comments/keywords")
async def omar_set_comment_keywords(payload: OmarCommentKeywordsRequest) -> dict[str, Any]:
    saved = await omar_comments.set_selected_keyword_ids(payload.keyword_ids)
    if payload.all_videos is not None:
        await omar_comments.set_all_videos(payload.all_videos)
    return {"keyword_ids": saved, "all_videos": await omar_comments.all_videos_enabled()}


class OmarHashtagRequest(BaseModel):
    movie_id: str
    hashtag: str


@router.post("/hashtags")
async def omar_add_hashtag(payload: OmarHashtagRequest) -> dict[str, Any]:
    """Ô "Thêm hashtag" ở tab Omar: người dùng gõ hashtag cho một phim -> kiểm tra có video trên TikTok, tạo (hoặc bật lại)
    từ khoá TikTok, rồi cào ngay tới hết (omar_hashtag.crawl_hashtag_now). Spider TikTok và các lượt Omar sau dùng từ khoá
    này như mọi từ khoá khác."""
    tag = payload.hashtag.strip().lstrip("#").replace(" ", "")
    if len(tag) < 2:
        raise ValidationError("Invalid hashtag")
    # Kiểm tra trên TikTok qua Omar có lúc >12 giây (đi chung bộ giãn nhịp với lượt Omar đang chạy) - dashboard chỉ chờ
    # 15 giây. Quá EXISTS_CHECK_SECONDS thì coi như chưa biết: vẫn tạo từ khoá và cào; hashtag rỗng thì lượt cào dừng ở trang 1.
    try:
        exists = await asyncio.wait_for(omar_fetch.hashtag_exists(tag), timeout=EXISTS_CHECK_SECONDS)
    except TimeoutError:
        exists = None
    if exists is False:
        raise ValidationError(f"#{tag} has no videos on TikTok")
    row = await get_or_create_keyword(payload.movie_id, "tiktok", f"#{tag}")
    if row is None:
        raise UpstreamError("Could not create the keyword (movie does not exist or is disabled, or D1 failed)")
    await set_keyword_enabled("tiktok", row["id"], True)
    started = omar_hashtag.crawl_hashtag_now(row)
    logger.info("omar_hashtag_added", hashtag=row["keyword"], movie_id=payload.movie_id, started=started)
    return {"keyword": row, "started": started, "verified": exists is True}


@router.get("/hashtags/search")
async def omar_search_hashtags(q: str = Query(min_length=2, max_length=60)) -> dict[str, Any]:
    """Nút "Tìm hashtag liên quan" ở tab Omar: các hashtag TikTok có tên chứa `q` kèm tổng video / lượt xem, tách mục khớp
    chính xác và các biến thể; `tracked` cho biết hashtag đã là từ khoá của một phim (omar_hashtag_stats.search)."""
    name = omar_hashtag_stats.clean_name(q)
    if len(name) < 2:
        raise ValidationError("Invalid hashtag")
    try:
        result = await asyncio.wait_for(
            omar_hashtag_stats.search(name), timeout=omar_hashtag_stats.SEARCH_TIMEOUT_SECONDS
        )
    except TimeoutError as exc:
        raise UpstreamError("Omar did not answer the hashtag search in time") from exc
    except OmarStop as exc:
        raise UpstreamError(f"Omar refused the search: {exc}") from exc
    except Exception as exc:
        raise UpstreamError(f"Omar hashtag search failed: {type(exc).__name__}") from exc
    if result is None:
        raise ValidationError("Omar API key is not set")
    return result


@router.post("/hashtags/{keyword_id}/recrawl")
async def omar_recrawl_hashtag(keyword_id: str) -> dict[str, Any]:
    """Nút "Cào lại" trên từng dòng bảng tiến độ hashtag: bỏ con trỏ của hashtag và cào lại từ trang đầu ngay, độc lập với lượt
    Omar đang chạy (nhịp theo omar_manual_requests_per_minute). started=False khi hashtag đang được cào riêng."""
    keywords = await get_enabled_keywords(platform="tiktok")
    keyword = next((k for k in keywords if k["id"] == keyword_id and str(k["keyword"]).startswith("#")), None)
    if keyword is None:
        raise NotFoundError("Enabled TikTok hashtag not found")
    started = await omar_hashtag.recrawl_hashtag_now(keyword)
    logger.info("omar_hashtag_recrawl", hashtag=keyword["keyword"], started=started)
    return {"keyword_id": keyword_id, "hashtag": keyword["keyword"], "started": started}
