"""Gọi Omar API cho hashtag TikTok: một trang video, kiểm tra hashtag tồn tại, thông tin hashtag, và đẩy video vào Kafka."""

from __future__ import annotations

from typing import Any

import httpx

from app.clients.kafka import publish_raw_post
from app.clients.omar import hashtag_info, hashtag_search_results, next_cursor, omar_get, videos_of
from app.core.config import settings
from app.services.accounts.config_db.omar import get_omar_api_settings
from app.services.omar.omar_budget import Pacer
from app.services.omar.omar_seen import claim_unseen, forget_seen


async def publish_videos(mapped: list[dict[str, Any]], keyword_id: str, stats: dict[str, Any]) -> None:
    """Gửi video đã map lên raw_posts như spider (chống trùng qua tập seen dùng chung với spider, claim_unseen)."""
    brand_new: set[str] = set()
    fresh_ids = await claim_unseen([video["video_id"] for video in mapped], brand_new)
    for video in mapped:
        if video["video_id"] not in fresh_ids:
            stats["skipped_seen"] = int(stats.get("skipped_seen") or 0) + 1
            continue
        ok = await publish_raw_post(
            key=f"tiktok:{video['video_id']}", value={"platform": "tiktok", "keyword_id": keyword_id, **video}
        )
        stats["published"] = int(stats.get("published") or 0) + int(ok)
        if not ok:
            brand_new.discard(video["video_id"])
            await forget_seen(video["video_id"])  # chưa tới Kafka: lần sau gửi lại


async def fetch_page(
    client: httpx.AsyncClient, pacer: Pacer, api_key: str, hashtag: str, cursor: str, hashtag_id: str | None = None
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    """Một trang hashtag_videos -> (video gốc, cursor kế tiếp hoặc None khi hết, hashtag_id). Có `hashtag_id` (lưu từ trang
    trước) thì gọi bằng `id` - Omar bỏ qua bước tra tên hashtag, nhanh hơn và tránh lỗi tra tên."""
    params = {"id": hashtag_id, "cursor": cursor} if hashtag_id else {"name": hashtag, "cursor": cursor}
    data = await omar_get(client, pacer.wait, api_key, "hashtag_videos", params)
    if data is None:
        return [], None, hashtag_id  # hashtag không tồn tại
    return videos_of(data), next_cursor(data), str(data.get("hashtag_id") or "") or hashtag_id


async def hashtag_exists(hashtag: str) -> bool | None:
    """Hashtag có video trên TikTok không (trang 1 của hashtag_videos). None khi không biết (chưa có key, Omar lỗi khác).
    Omar trả 424 cho hashtag không tồn tại (đo biến thể kiểu #ChiChiEmEm32026 luôn 424, #chichiemem3 có video)."""
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    if not api_key:
        return None
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        try:
            data = await omar_get(
                client,
                Pacer(settings.omar_requests_per_minute).wait,
                api_key,
                "hashtag_videos",
                {"name": hashtag.lstrip("#"), "cursor": "0"},
                retries=1,
            )
        except httpx.HTTPStatusError as exc:
            return False if exc.response.status_code == 424 else None
        except Exception:  # noqa: BLE001 - không kiểm tra được thì coi như chưa biết
            return None
    return bool(data and data.get("videos"))


async def fetch_hashtag_info(client: httpx.AsyncClient, pacer: Pacer, api_key: str, hashtag: str) -> dict[str, Any]:
    """Tổng số video / lượt xem TikTok báo cho hashtag (hashtag_info, format=clean) - để biết đã cào được bao nhiêu phần."""
    # 424 ở endpoint này thường là "Hashtag not found or lookup failed" - chỉ thử lại một lần.
    data = await omar_get(client, pacer.wait, api_key, "hashtag_info", {"name": hashtag, "format": "clean"}, retries=1)
    return hashtag_info(data)


async def search_hashtags(client: httpx.AsyncClient, pacer: Pacer, api_key: str, query: str) -> list[dict[str, Any]]:
    """Các hashtag TikTok có tên chứa `query` (search_hashtags, format=clean), kèm số video / lượt xem của từng cái. Một trang
    (tối đa ~10 hashtag) là đủ để thấy hashtag chính và các biến thể gần giống."""
    data = await omar_get(
        client,
        pacer.wait,
        api_key,
        "search_hashtags",
        {"query": query.lstrip("#"), "cursor": "0", "format": "clean"},
        retries=1,
    )
    return hashtag_search_results(data)
