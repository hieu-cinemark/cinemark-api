"""Cào video TikTok theo hashtag qua Omar API (dev.omar-thing.site, từ 2026-10-12) - chạy MỖI GIỜ (scheduler._omar_tick),
song song với spider TikTok của spider-hub. Video đi qua ĐÚNG đường ingest của spider: publish lên topic raw_posts với
key "tiktok:<video_id>" và cùng tên trường TikTokVideoItem (map_aweme), nên ingest consumer lọc liên quan, chống trùng,
cập nhật tương tác và chọn bài cào comment như mọi bài TikTok khác.

API key đọc từ bảng omar_api_settings trên Supabase (nhập ở Settings). Không có key thì bỏ qua.

Ngân sách: gói hiện tại 10 request/phút, 3.000/ngày (GET /usage). Mỗi giờ dùng tối đa settings.omar_requests_per_hour
(mặc định 100 ~ 2.400/ngày) chia đều cho các hashtag TikTok đang bật: mỗi hashtag 1..omar_max_pages_per_keyword trang
(20 video/trang). Nhiều hashtag hơn ngân sách thì xoay vòng qua các giờ (offset trong Redis). Nhịp gọi giữ dưới giới hạn
mỗi phút; 429 chờ theo Retry-After, 424 (TikTok phía họ lỗi) thử lại một lần, 401/403 (key sai/hết hạn) dừng lượt.

Định dạng mặc định (không format=clean) là response gốc của TikTok - có đủ tác giả (format=clean thì không), nên mỗi bài
lưu kèm tín hiệu tác giả (tick xanh, follower, tài khoản shop, nhãn quảng cáo/hợp tác) trong raw_json."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from app.clients.kafka import publish_raw_post
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.d1 import get_enabled_keywords
from app.services.platform_config_db import get_omar_api_settings

logger = get_logger(__name__)

_OFFSET_KEY = f"{REDIS_KEY_PREFIX}omar:tiktok_hashtag_offset"


class OmarStop(Exception):
    """Key sai/hết hạn/hết hạn mức ngày - dừng cả lượt, không thử tiếp các hashtag khác."""


def _first_url(node: Any) -> str | None:
    urls = (node or {}).get("url_list") if isinstance(node, dict) else None
    return next((u for u in urls or [] if isinstance(u, str) and u.startswith("http")), None)


def map_aweme(item: dict[str, Any]) -> dict[str, Any] | None:
    """Một video dạng gốc của TikTok (aweme) -> cùng tên trường với TikTokVideoItem của spider-hub
    (spiders/tiktok/features/hashtag_search/extract.py), cộng tín hiệu tác giả. None khi thiếu id."""
    video_id = str(item.get("aweme_id") or "").strip()
    if not video_id:
        return None
    author = item.get("author") or {}
    stats = item.get("statistics") or {}
    video = item.get("video") or {}
    music = item.get("music") or {}
    username = author.get("unique_id")
    duration_ms = video.get("duration") or 0
    return {
        "video_id": video_id,
        "url": f"https://www.tiktok.com/@{username}/video/{video_id}" if username else item.get("share_url"),
        "desc": item.get("desc"),
        "text_language": item.get("desc_language"),
        "location_created": item.get("region"),
        "create_time": item.get("create_time"),
        "author_id": author.get("uid"),
        "author_username": username,
        "author_name": author.get("nickname"),
        "author_avatar_url": _first_url(author.get("avatar_thumb")),
        "duration": round(duration_ms / 1000) if duration_ms else None,
        "cover_url": _first_url(video.get("origin_cover")) or _first_url(video.get("cover")),
        "play_url": _first_url(video.get("play_addr")),
        "music_title": music.get("title"),
        "hashtags": [
            t["hashtag_name"] for t in item.get("text_extra") or [] if isinstance(t, dict) and t.get("hashtag_name")
        ],
        "play_count": stats.get("play_count"),
        "like_count": stats.get("digg_count"),
        "comment_count": stats.get("comment_count"),
        "share_count": stats.get("share_count"),
        "collect_count": stats.get("collect_count"),
        # Tín hiệu tác giả cho phân loại tầng (Organic/Paid/Owned/Media) không cần AI. Đo 2026-10-12 trên hashtag_videos:
        # verification_type = 1 với MỌI tài khoản (không phải tick xanh - tick xanh là custom_verify), follower_count luôn
        # 0 (endpoint này không trả) nên để None.
        "author_followers": author.get("follower_count") or None,
        "author_verified": bool(author.get("custom_verify")),
        "author_is_business": bool(author.get("enterprise_verify_reason"))
        or (author.get("commerce_user_level") or 0) > 0,
        "author_is_seller": bool(author.get("with_shop_entry") or author.get("with_commerce_entry")),
        "author_signature": (author.get("signature") or "")[:300] or None,
        "is_ad": bool(item.get("is_ads")),
        "is_branded": bool(item.get("branded_content_accounts"))
        or ((item.get("commerce_info") or {}).get("branded_content_type") or 0) > 0,
        "source": "omar",
    }


class _Pacer:
    """Giữ khoảng cách tối thiểu giữa hai request để không vượt giới hạn mỗi phút."""

    def __init__(self, per_minute: int) -> None:
        self.gap = 60.0 / max(per_minute, 1) + 0.3
        self.last = 0.0

    async def wait(self) -> None:
        delay = self.last + self.gap - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        self.last = time.monotonic()


async def _fetch_page(
    client: httpx.AsyncClient, pacer: _Pacer, api_key: str, hashtag: str, cursor: str
) -> tuple[list[dict[str, Any]], str | None]:
    """Một trang hashtag_videos -> (video gốc, cursor kế tiếp hoặc None khi hết)."""
    for attempt in range(3):
        await pacer.wait()
        resp = await client.get(
            f"{settings.omar_api_base_url.rstrip('/')}/hashtag_videos",
            params={"name": hashtag, "cursor": cursor},
            headers={"X-API-Key": api_key},
        )
        if resp.status_code == 429:
            retry = min(float(resp.headers.get("Retry-After") or 30), 120)
            body = resp.text[:200]
            if "day" in body.lower() and retry > 60:
                raise OmarStop(f"daily_limit: {body}")
            logger.warning("omar_rate_limited", hashtag=hashtag, retry_after=retry)
            await asyncio.sleep(retry)
            continue
        if resp.status_code in (401, 403):
            raise OmarStop(f"http_{resp.status_code}: {resp.text[:200]}")
        if resp.status_code == 424 and attempt == 0:
            continue  # TikTok phía Omar lỗi tạm thời - thử lại một lần
        if resp.status_code == 404:
            return [], None  # hashtag không tồn tại
        resp.raise_for_status()
        data = (resp.json() or {}).get("data") or {}
        has_more = str(data.get("has_more")).lower() == "true"
        return list(data.get("videos") or []), (
            str(data.get("next_cursor")) if has_more and data.get("next_cursor") else None
        )
    return [], None


async def run_omar_round() -> dict[str, Any]:
    """Một lượt mỗi giờ. Trả về thống kê."""
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    stats: dict[str, Any] = {"keywords": 0, "requests": 0, "videos": 0, "published": 0, "failed": 0}
    if not api_key:
        logger.info("omar_round_skipped_no_key")
        return {**stats, "skipped": "no_key"}
    keywords = [k for k in await get_enabled_keywords(platform="tiktok") if str(k["keyword"]).startswith("#")]
    if not keywords:
        return {**stats, "skipped": "no_keywords"}

    budget = max(settings.omar_requests_per_hour, 1)
    pages = max(1, min(settings.omar_max_pages_per_keyword, budget // len(keywords)))
    per_round = min(len(keywords), budget // pages)
    # Nhiều hashtag hơn ngân sách một giờ: xoay vòng, giờ sau tiếp từ chỗ giờ này dừng.
    redis = get_redis_client()
    try:
        offset = int(await redis.get(_OFFSET_KEY) or 0) % len(keywords)
    except Exception:  # noqa: BLE001 - không có Redis thì bắt đầu từ đầu
        offset = 0
    ordered = sorted(keywords, key=lambda k: k["id"])
    batch = (ordered[offset:] + ordered[:offset])[:per_round]

    pacer = _Pacer(settings.omar_requests_per_minute)
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        for kw in batch:
            hashtag = str(kw["keyword"]).lstrip("#")
            stats["keywords"] += 1
            cursor: str | None = "0"
            for _ in range(pages):
                if cursor is None:
                    break
                try:
                    videos, cursor = await _fetch_page(client, pacer, api_key, hashtag, cursor)
                except OmarStop as exc:
                    logger.error("omar_round_stopped", error=str(exc), telegram=True)
                    return {**stats, "stopped": str(exc)[:100]}
                except Exception as exc:  # noqa: BLE001 - một hashtag lỗi không chặn hashtag khác
                    stats["failed"] += 1
                    logger.warning("omar_page_failed", hashtag=hashtag, error=str(exc)[:200])
                    break
                stats["requests"] += 1
                for raw in videos:
                    video = map_aweme(raw)
                    if video is None:
                        continue
                    stats["videos"] += 1
                    ok = await publish_raw_post(
                        key=f"tiktok:{video['video_id']}",
                        value={"platform": "tiktok", "keyword_id": kw["id"], **video},
                    )
                    stats["published"] += int(ok)
    try:
        await redis.set(_OFFSET_KEY, (offset + per_round) % len(keywords))
    except Exception as exc:  # noqa: BLE001 - mất offset chỉ làm giờ sau quét lại từ đầu
        logger.warning("omar_offset_save_failed", error=str(exc))
    logger.info("omar_round_finished", pages_per_keyword=pages, total_keywords=len(keywords), **stats)
    return stats
