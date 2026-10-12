"""Cào video TikTok theo hashtag qua Omar API (dev.omar-thing.site, từ 2026-10-12) - chạy MỖI GIỜ (scheduler._omar_tick),
song song với spider TikTok của spider-hub. Video đi qua ĐÚNG đường ingest của spider: publish lên topic raw_posts với
key "tiktok:<video_id>" và cùng tên trường TikTokVideoItem (map_aweme), nên ingest consumer lọc liên quan, chống trùng,
cập nhật tương tác và chọn bài cào comment như mọi bài TikTok khác.

API key đọc từ bảng omar_api_settings trên Supabase (nhập ở Settings). Không có key thì bỏ qua.

Từ 2026-10-12 mỗi hashtag được cào TỚI HẾT kết quả, con trỏ lưu trong Redis (omar:hashtag_state) - hết ngân sách thì
lượt sau đi tiếp (xem run_omar_round). Gói hiện tại 10 request/phút, 3.000/ngày (GET /usage): ngân sách mỗi giờ = hạn
mức ngày còn lại chia đều cho số giờ tới lúc reset. Nhịp gọi giữ dưới giới hạn mỗi phút; 429 chờ theo Retry-After, 424
(TikTok phía họ lỗi) thử lại một lần, 401/403 (key sai/hết hạn) dừng lượt.

Định dạng mặc định (không format=clean) là response gốc của TikTok - có đủ tác giả (format=clean thì không), nên mỗi bài
lưu kèm tín hiệu tác giả (tick xanh, follower, tài khoản shop, nhãn quảng cáo/hợp tác) trong raw_json."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.clients.kafka import publish_raw_post
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.d1 import d1_query, get_enabled_keywords
from app.services.platform_config_db import get_omar_api_settings

logger = get_logger(__name__)


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
    for attempt in range(4):
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
        if resp.status_code == 424 and attempt < 3:
            # FETCH_FAILED: Omar gọi TikTok lỗi tạm thời (đo 2026-10-12: ~15% trang). Thử ngay thì thường lỗi tiếp - chờ
            # lâu dần rồi thử lại.
            logger.info("omar_fetch_failed_retry", hashtag=hashtag, cursor=cursor, attempt=attempt + 1)
            await asyncio.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 404:
            return [], None  # hashtag không tồn tại
        resp.raise_for_status()
        data = (resp.json() or {}).get("data") or {}
        has_more = str(data.get("has_more")).lower() == "true"
        return list(data.get("videos") or []), (
            str(data.get("next_cursor")) if has_more and data.get("next_cursor") else None
        )
    # Hết lượt thử: báo lỗi chứ KHÔNG trả "hết trang" - không thì run_omar_round đánh dấu hashtag đã cào xong và bỏ nó
    # 7 ngày. Lỗi thì con trỏ giữ nguyên, lượt sau thử lại đúng trang này.
    raise RuntimeError(f"omar_retries_exhausted status={resp.status_code}")


_STATE_KEY = f"{REDIS_KEY_PREFIX}omar:hashtag_state"
# Trang rỗng liên tiếp mà vẫn has_more: coi như đã hết (tránh lặp vô hạn nếu Omar trả cursor lỗi).
_MAX_EMPTY_PAGES = 3


async def _usage(client: httpx.AsyncClient, api_key: str) -> tuple[int, int, int] | None:
    """(request còn lại trong ngày, số giây tới lúc reset, giới hạn mỗi phút của gói) từ GET /usage, hoặc None khi không
    đọc được. Đọc giới hạn của GÓI mỗi lượt nên nâng gói Omar thì cào nhanh/nhiều hơn mà không phải sửa gì."""
    try:
        resp = await client.get(f"{settings.omar_api_base_url.rstrip('/')}/usage", headers={"X-API-Key": api_key})
        usage = (((resp.json() or {}).get("data") or {}).get("usage")) or {}
        day, minute = usage.get("day") or {}, usage.get("minute") or {}
        return int(day.get("remaining")), int(day.get("resets_in") or 0), int(minute.get("limit") or 0)
    except Exception as exc:  # noqa: BLE001 - không đọc được thì dùng ngân sách mặc định
        logger.warning("omar_usage_failed", error=str(exc)[:200])
        return None


async def _movie_priority() -> dict[str, float]:
    """movie_id -> số ngày lệch khỏi ngày chiếu (nhỏ = gần ngày chiếu, cào trước)."""
    rows = await d1_query("SELECT id, released_at FROM movies", quiet=True) or []
    today = datetime.now(tz=UTC).date()
    out: dict[str, float] = {}
    for row in rows:
        try:
            out[row["id"]] = abs((datetime.fromisoformat(str(row["released_at"])[:10]).date() - today).days)
        except TypeError, ValueError:
            out[row["id"]] = 365.0
    return out


_ROTATE_KEY = f"{REDIS_KEY_PREFIX}omar:rotation_offset"


async def run_omar_round() -> dict[str, Any]:
    """Một lượt mỗi giờ, CHIA ĐỀU cho mọi hashtag chưa cào xong (xoay vòng): mỗi hashtag được budget // số hashtag trang,
    phần dư +1 trang cho vài hashtag - vị trí bắt đầu xoay qua từng lượt (Redis omar:rotation_offset) để công bằng.
    Hashtag nào cào xong giữa lượt thì phần trang còn lại chia tiếp cho các hashtag khác. Con trỏ mỗi hashtag lưu trong
    Redis sau mỗi trang, lượt sau / hôm sau đi tiếp; cào tới has_more = false thì xong, cào lại từ đầu sau
    settings.omar_recrawl_days ngày, trong lúc chờ ngân sách dư lấy trang 1 để bắt video mới.

    Ngân sách mỗi giờ = hạn mức ngày còn lại (GET /usage, trừ settings.omar_daily_reserve) chia cho số giờ tới lúc reset,
    trần = số request làm được trong một giờ ở giới hạn mỗi phút của gói (và settings.omar_requests_per_hour nếu > 0).
    Nhịp gọi cũng theo giới hạn mỗi phút của gói - nâng gói Omar thì tự chạy nhanh/nhiều hơn."""
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    stats: dict[str, Any] = {"keywords": 0, "requests": 0, "videos": 0, "published": 0, "failed": 0, "finished": 0}
    if not api_key:
        logger.info("omar_round_skipped_no_key")
        return {**stats, "skipped": "no_key"}
    keywords = [k for k in await get_enabled_keywords(platform="tiktok") if str(k["keyword"]).startswith("#")]
    if not keywords:
        return {**stats, "skipped": "no_keywords"}

    redis = get_redis_client()
    raw_state = await redis.hgetall(_STATE_KEY) or {}
    state: dict[str, dict[str, Any]] = {}
    for key, value in raw_state.items():
        try:
            state[key.decode() if isinstance(key, bytes) else key] = json.loads(value)
        except TypeError, ValueError:
            continue
    now = datetime.now(tz=UTC)
    recrawl_before = (now - timedelta(days=settings.omar_recrawl_days)).isoformat()

    async def save(kw_id: str, entry: dict[str, Any]) -> None:
        state[kw_id] = entry
        try:
            await redis.hset(_STATE_KEY, kw_id, json.dumps(entry))
        except Exception as exc:  # noqa: BLE001 - mất con trỏ chỉ làm lần sau cào lại từ đầu
            logger.warning("omar_state_save_failed", error=str(exc)[:200])

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        usage = await _usage(client, api_key)
        per_minute = (usage[2] if usage and usage[2] > 0 else 0) or settings.omar_requests_per_minute
        hour_cap = per_minute * 55
        if settings.omar_requests_per_hour > 0:
            hour_cap = min(hour_cap, settings.omar_requests_per_hour)
        if usage is not None:
            remaining, resets_in, _ = usage
            hours_left = max(1, -(-resets_in // 3600))
            budget = min(hour_cap, max(0, remaining - settings.omar_daily_reserve) // hours_left)
        else:
            budget = hour_cap // 4
        stats["budget"] = budget
        stats["per_minute"] = per_minute
        if budget <= 0:
            logger.info("omar_round_no_budget", remaining=usage[0] if usage else None)
            return {**stats, "skipped": "no_budget"}
        pacer = _Pacer(per_minute)

        priority = await _movie_priority()

        def todo(kw: dict[str, Any]) -> bool:
            entry = state.get(kw["id"]) or {}
            return not entry.get("done_at") or entry["done_at"] < recrawl_before

        # Thứ tự gốc: phim gần ngày chiếu trước; rồi xoay vòng theo offset để phần dư chia công bằng qua các lượt.
        deep = sorted(
            (kw for kw in keywords if todo(kw)), key=lambda kw: (priority.get(kw["movie_id"], 365.0), kw["id"])
        )
        try:
            offset = int(await redis.get(_ROTATE_KEY) or 0) % max(len(deep), 1)
        except Exception:  # noqa: BLE001
            offset = 0
        deep = deep[offset:] + deep[:offset]
        fresh = sorted(
            (kw for kw in keywords if not todo(kw)), key=lambda kw: (state.get(kw["id"]) or {}).get("fresh_at") or ""
        )
        # Vòng 1 chia đều: mỗi hashtag `quota` trang, `extra` hashtag đầu (theo vòng xoay) thêm 1 trang.
        quota, extra = divmod(budget, max(len(deep), 1))

        async def fetch(kw: dict[str, Any], cursor: str) -> tuple[list[dict[str, Any]], str | None] | None:
            try:
                videos, next_cursor = await _fetch_page(client, pacer, api_key, str(kw["keyword"]).lstrip("#"), cursor)
            except OmarStop:
                raise
            except Exception as exc:  # noqa: BLE001 - một trang lỗi: để lượt sau thử lại đúng trang đó
                stats["failed"] += 1
                logger.warning("omar_page_failed", hashtag=kw["keyword"], cursor=cursor, error=str(exc)[:200])
                return None
            stats["requests"] += 1
            for raw in videos:
                video = map_aweme(raw)
                if video is None:
                    continue
                stats["videos"] += 1
                stats["published"] += int(
                    await publish_raw_post(
                        key=f"tiktok:{video['video_id']}", value={"platform": "tiktok", "keyword_id": kw["id"], **video}
                    )
                )
            return videos, next_cursor

        async def crawl(kw: dict[str, Any], pages: int) -> bool:
            """Tối đa `pages` trang tiếp theo của hashtag. True khi hashtag còn trang để lượt sau / vòng sau đi tiếp."""
            entry = state.get(kw["id"]) or {}
            if entry.get("done_at"):  # đến hạn cào lại từ đầu
                entry = {}
            entry.setdefault("started_at", now.isoformat())
            cursor = entry.get("cursor") or "0"
            empty = int(entry.get("empty") or 0)
            for _ in range(pages):
                if stats["requests"] >= budget:
                    return True
                result = await fetch(kw, cursor)
                if result is None:
                    return False  # trang lỗi: con trỏ giữ nguyên, lượt sau thử lại
                videos, next_cursor = result
                entry["pages"] = int(entry.get("pages") or 0) + 1
                empty = 0 if videos else empty + 1
                if next_cursor is None or empty >= _MAX_EMPTY_PAGES:
                    entry.update(cursor=None, empty=0, done_at=datetime.now(tz=UTC).isoformat(), fresh_at=None)
                    stats["finished"] += 1
                    logger.info("omar_hashtag_finished", hashtag=kw["keyword"], pages=entry["pages"])
                    await save(kw["id"], entry)
                    return False
                cursor = next_cursor
                entry.update(cursor=cursor, empty=empty)
                await save(kw["id"], entry)
            return True

        try:
            # Vòng 1: chia đều. Các vòng sau: phần ngân sách hashtag cào xong/lỗi để lại, mỗi hashtag còn trang thêm 1 trang.
            active = []
            for i, kw in enumerate(deep):
                pages = quota + (1 if i < extra else 0)
                if pages <= 0 or stats["requests"] >= budget:
                    break
                stats["keywords"] += 1
                if await crawl(kw, pages):
                    active.append(kw)
            while active and stats["requests"] < budget:
                active = [kw for kw in active if stats["requests"] < budget and await crawl(kw, 1)]
            # Còn dư ngân sách: trang 1 của các hashtag đã cào hết, lâu chưa làm mới nhất trước.
            for kw in fresh:
                if stats["requests"] >= budget:
                    break
                if await fetch(kw, "0") is not None:
                    await save(kw["id"], {**(state.get(kw["id"]) or {}), "fresh_at": datetime.now(tz=UTC).isoformat()})
        except OmarStop as exc:
            logger.error("omar_round_stopped", error=str(exc), telegram=True)
            stats["stopped"] = str(exc)[:100]

    try:
        # Mọi hashtag đều có phần (quota > 0): dịch đúng số hashtag vừa nhận trang dư, để lượt sau trang dư rơi vào
        # nhóm kế tiếp. Ngân sách ít hơn số hashtag: dịch qua những hashtag vừa được cào.
        step = extra if quota > 0 else stats["keywords"]
        await redis.set(_ROTATE_KEY, (offset + max(step, 1)) % max(len(deep), 1))
    except Exception as exc:  # noqa: BLE001
        logger.warning("omar_rotation_save_failed", error=str(exc)[:200])
    pending = sum(1 for kw in keywords if todo(kw))
    logger.info("omar_round_finished", total_keywords=len(keywords), pending_keywords=pending, **stats)
    return {**stats, "pending_keywords": pending}
