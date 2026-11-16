"""Tab ShortVid trên spider-hub-dashboard: hạn mức của key, lượt đang chạy, tiến độ từng hashtag, nút Chạy ngay / Dừng
(app/services/shortvid/shortvid_tiktok.py). Key lưu/đổi qua GET/PUT /settings/shortvid-api (app/api/routes/settings.py)."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.config import settings
from app.core.errors import UpstreamError, ValidationError
from app.core.logging import get_logger
from app.repositories.d1.store import get_or_create_keyword, set_keyword_enabled
from app.services.accounts.platform_config_db import get_shortvid_api_settings
from app.services.shortvid import shortvid_channels, shortvid_comments, shortvid_search, shortvid_tiktok, shortvid_trending, shortvid_yield

logger = get_logger(__name__)

router = APIRouter(prefix="/shortvid", tags=["shortvid"])

EXISTS_CHECK_SECONDS = 8.0


@router.get("/status")
async def shortvid_status() -> dict[str, Any]:
    row = await get_shortvid_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()

    async def _usage() -> dict[str, Any] | None:
        return await shortvid_tiktok.fetch_usage(api_key) if api_key else None

    # Các nguồn độc lập (D1, Redis, API ShortVid) - gọi song song thay vì lần lượt.
    progress, usage, round_status, paused, channels, comments = await asyncio.gather(
        shortvid_tiktok.hashtag_progress(),
        _usage(),
        shortvid_tiktok.round_status_full(),
        shortvid_tiktok.get_pause(),
        shortvid_channels.channel_status(),
        shortvid_comments.job_status(),
    )
    return {
        "key_set": bool(api_key),
        "usage": usage,
        # Trần request/phút đặt ở tab ShortVid (0 = theo gói); lượt chạy dùng số nhỏ hơn giữa trần này và gói.
        "requests_per_minute": int((row or {}).get("requests_per_minute") or 0),
        "round": round_status,
        # Đang dừng chờ hạn mức / key mới: {"reason": "quota"|"key", "until": iso, ...} hoặc None.
        "paused": paused,
        "mode": "spread_daily" if settings.shortvid_spread_daily else "continuous",
        "hashtags": progress,
        "channels": channels,
        "comments": {
            **comments,
            "cadence": settings.shortvid_comments_cadence,
            "weekday": settings.shortvid_comments_weekday,
            "time": settings.shortvid_comments_time,
            "top_per_hashtag": settings.shortvid_comments_top_per_hashtag,
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
async def shortvid_run() -> dict[str, bool]:
    from app.services.crawl.scheduler import trigger_shortvid_now

    # Bấm "Chạy ngay" là bỏ trạng thái dừng (vd. vừa nâng gói / đổi key) - lượt tự kiểm tra lại hạn mức.
    await shortvid_tiktok.clear_pause()
    started = trigger_shortvid_now()
    logger.info("shortvid_manual_run", started=started)
    return {"started": started}


@router.post("/stop")
async def shortvid_stop() -> dict[str, bool]:
    stopping = shortvid_tiktok.request_stop()
    logger.info("shortvid_manual_stop", stopping=stopping)
    return {"stopping": stopping}


@router.post("/comments/run")
async def shortvid_comments_run() -> dict[str, Any]:
    """Xếp hàng bình luận tuần ngay (không đợi mốc tuần) rồi mở lượt ShortVid nếu đang rảnh - lượt đang chạy thì hàng đợi được
    xử lý ở lượt kế tiếp."""
    from app.services.crawl.scheduler import trigger_shortvid_now

    job = await shortvid_comments.enqueue_weekly_job(trigger="manual")
    started = trigger_shortvid_now()
    logger.info("shortvid_comments_manual_run", videos=job["videos"], started=started)
    return {
        "queued": job["videos"],
        "pending": (await shortvid_comments.job_status())["pending_videos"],
        "started": started,
    }


class ShortVidCommentKeywordsRequest(BaseModel):
    keyword_ids: list[str]
    # None = giữ nguyên tuỳ chọn đã lưu.
    all_videos: bool | None = None


@router.get("/comments/keywords")
async def shortvid_comment_keywords() -> dict[str, Any]:
    """Ô chọn từ khoá cào bình luận ở tab ShortVid: danh sách đã chọn (rỗng = mọi hashtag TikTok đang bật) + tuỳ chọn cào
    toàn bộ video của các từ khoá đó."""
    return {
        "keyword_ids": await shortvid_comments.selected_keyword_ids(),
        "all_videos": await shortvid_comments.all_videos_enabled(),
    }


@router.put("/comments/keywords")
async def shortvid_set_comment_keywords(payload: ShortVidCommentKeywordsRequest) -> dict[str, Any]:
    saved = await shortvid_comments.set_selected_keyword_ids(payload.keyword_ids)
    if payload.all_videos is not None:
        await shortvid_comments.set_all_videos(payload.all_videos)
    return {"keyword_ids": saved, "all_videos": await shortvid_comments.all_videos_enabled()}


class ShortVidHashtagRequest(BaseModel):
    movie_id: str
    hashtag: str


@router.post("/hashtags")
async def shortvid_add_hashtag(payload: ShortVidHashtagRequest) -> dict[str, Any]:
    """Ô "Thêm hashtag" ở tab ShortVid: người dùng gõ hashtag cho một phim -> kiểm tra có video trên TikTok, tạo (hoặc bật lại)
    từ khoá TikTok, rồi cào ngay tới hết (shortvid_tiktok.crawl_hashtag_now). Spider TikTok và các lượt ShortVid sau dùng từ khoá
    này như mọi từ khoá khác."""
    tag = payload.hashtag.strip().lstrip("#").replace(" ", "")
    if len(tag) < 2:
        raise ValidationError("Hashtag không hợp lệ")
    # Kiểm tra trên TikTok qua ShortVid có lúc >12 giây (đi chung bộ giãn nhịp với lượt ShortVid đang chạy) - dashboard chỉ chờ
    # 15 giây. Quá EXISTS_CHECK_SECONDS thì coi như chưa biết: vẫn tạo từ khoá và cào; hashtag rỗng thì lượt cào dừng ở trang 1.
    try:
        exists = await asyncio.wait_for(shortvid_tiktok.hashtag_exists(tag), timeout=EXISTS_CHECK_SECONDS)
    except TimeoutError:
        exists = None
    if exists is False:
        raise ValidationError(f"#{tag} không có video nào trên TikTok")
    row = await get_or_create_keyword(payload.movie_id, "tiktok", f"#{tag}")
    if row is None:
        raise UpstreamError("Không tạo được từ khoá (phim không tồn tại / đang tắt, hoặc D1 lỗi)")
    await set_keyword_enabled("tiktok", row["id"], True)
    started = shortvid_tiktok.crawl_hashtag_now(row)
    logger.info("shortvid_hashtag_added", hashtag=row["keyword"], movie_id=payload.movie_id, started=started)
    return {"keyword": row, "started": started, "verified": exists is True}


@router.get("/search")
async def shortvid_search_queries() -> dict[str, Any]:
    """Câu tìm theo từ khoá (tên phim, không dấu, câu tìm Facebook/Threads, câu tự thêm) và kết quả lần tìm gần nhất."""
    return {
        "queries": await shortvid_search.query_status(),
        "modes": shortvid_search.search_modes(),
        "pages": settings.shortvid_search_pages,
    }


class ShortVidSearchRequest(BaseModel):
    movie_id: str
    query: str


@router.post("/search")
async def shortvid_add_search(payload: ShortVidSearchRequest) -> dict[str, Any]:
    """Thêm câu tìm cho một phim rồi tìm ngay; các ngày sau tìm cùng các câu khác trong lượt ShortVid."""
    query = payload.query.strip()
    if len(query) < 3:
        raise ValidationError("Câu tìm quá ngắn")
    row = await shortvid_search.add_custom_query(payload.movie_id, query)
    found = next((q for q in await shortvid_search.build_queries() if q.get("id") == row["id"]), None)
    if found is None:
        await shortvid_search.delete_custom_query(row["id"])
        raise ValidationError("Phim chưa có hashtag TikTok đang bật, hoặc câu tìm trùng câu đã có")
    shortvid_search.search_now(found)
    logger.info("shortvid_search_added", query=query, movie_id=payload.movie_id)
    return {"query": found}


@router.delete("/search/{query_id}")
async def shortvid_delete_search(query_id: str) -> dict[str, bool]:
    await shortvid_search.delete_custom_query(query_id)
    return {"ok": True}


@router.get("/yield")
async def shortvid_yield_summary(days: int = 7) -> dict[str, Any]:
    """Hiệu quả từng nguồn theo ngày (request, gửi đi, mới thật) và phần ngân sách đang dùng cho tìm kiếm / kênh."""
    per_day = await shortvid_yield.daily(max(1, min(days, 30)))
    return {
        "days": per_day,
        "totals": shortvid_yield.totals(per_day),
        "shares": await shortvid_yield.adaptive_shares(),
        "base_shares": {"search": settings.shortvid_search_budget_share, "channel": settings.shortvid_channels_budget_share},
    }


@router.get("/trending")
async def shortvid_trending_videos(movie_id: str | None = None, hours: float = 24, limit: int = 30) -> dict[str, Any]:
    """Video đang nổi: tốc độ lượt xem/bình luận của top video TikTok mỗi phim, từ snapshot ShortVid
    (app/services/shortvid/shortvid_trending.py). hours = khoảng snapshot được xét (cần >= 1 giờ để có hai snapshot so sánh)."""
    if not 1 <= hours <= 24 * 7:
        raise ValidationError("hours phải trong khoảng 1..168")
    return await shortvid_trending.trending(movie_id=movie_id or None, hours=hours, limit=max(1, min(limit, 100)))
