"""Client ShortVid API (api.shortvidapi.com/v1) - nguồn TikTok của cinemark-api (thay Omar từ 16/11/2026).

Mọi endpoint là GET, key ở header `x-api-key` (để trong query string thì ShortVid trả 400). Response luôn có dạng
{code, provider, status, msg, data}; chỉ `code == 0` là thành công (và mới bị tính tiền). Dữ liệu video ở dạng "đã làm gọn"
(aweme_id, title, play_count, author.unique_id...), không phải response gốc của TikTok như Omar - xem map_video.

Endpoint dùng (tài liệu: shortvidapi.com/docs):
- challenge/info   challenge_name -> id hashtag, số video, lượt xem (10 credit/request)
- challenge/posts  cid, count <= 30, cursor -> video của hashtag (2 credit/video)
- video/search     keywords, region, count <= 30, cursor, sort_type (0 liên quan / 1 nhiều tim / 3 mới nhất),
                   publish_time (0 / 1 / 7 / 30 / 90 / 180 ngày)
- user/posts       unique_id, count <= 35, cursor -> video của kênh, mới nhất trước
- comment/list     url (id video), count <= 50, cursor; comment/reply comment_id -> trả lời
- video/detail     url (id video) -> số liệu một video (10 credit/request)

ShortVid không có GET /usage: tính phí theo credit, không có hạn mức request mỗi ngày. Hạn mức ngày ở đây do mình đặt
(settings.shortvid_daily_requests, đếm trong Redis theo ngày giờ VN) để khống chế chi phí; số credit còn lại đọc từ header
`x-credits-remaining` của response gần nhất. usage_snapshot() trả cùng dạng /usage cũ để phần ngân sách/tạm dừng/tab dashboard
dùng như trước."""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import httpx

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = get_logger(__name__)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
_USAGE_KEY = f"{REDIS_KEY_PREFIX}shortvid:usage:{{day}}"
_CREDITS_KEY = f"{REDIS_KEY_PREFIX}shortvid:credits"

# Số mục mỗi trang tối đa ShortVid cho phép (lấy tối đa để ít request nhất).
PAGE_SIZE = {"challenge/posts": 30, "video/search": 30, "user/posts": 35, "comment/list": 50, "comment/reply": 50}


class ShortVidStop(Exception):
    """Key sai / hết credit / hết hạn mức ngày tự đặt - dừng cả lượt, không thử tiếp việc khác."""


def _vn_day(now: datetime | None = None) -> str:
    return (now or datetime.now(tz=UTC)).astimezone(VN_TZ).strftime("%Y-%m-%d")


def _seconds_to_vn_midnight(now: datetime | None = None) -> int:
    local = (now or datetime.now(tz=UTC)).astimezone(VN_TZ)
    midnight = (local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((midnight - local).total_seconds()))


async def _count_request(credits_remaining: str | None) -> None:
    """Đếm request đã tính tiền trong ngày (giờ VN) + lưu số credit còn lại mới nhất."""
    try:
        redis = get_redis_client()
        key = _USAGE_KEY.format(day=_vn_day())
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, 3 * 86_400)
        if credits_remaining is not None:
            pipe.set(_CREDITS_KEY, credits_remaining)
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001 - đếm lỗi không làm hỏng request
        logger.warning("shortvid_usage_count_failed", error=str(exc)[:200])


async def usage_snapshot() -> dict[str, Any]:
    """Cùng dạng GET /usage của Omar trước đây: {"plan", "status", "day": {limit, used, remaining, resets_in}, "minute":
    {limit}, "credits_remaining"}. Hạn mức ngày = settings.shortvid_daily_requests (0 = không giới hạn, chỉ dừng khi hết
    credit)."""
    redis = get_redis_client()
    used_raw, credits_raw = await asyncio.gather(redis.get(_USAGE_KEY.format(day=_vn_day())), redis.get(_CREDITS_KEY))
    used = int(used_raw or 0)
    limit = int(settings.shortvid_daily_requests or 0)
    credits = int(float(credits_raw)) if credits_raw not in (None, b"", "") else None
    day: dict[str, Any] = {"used": used, "resets_in": _seconds_to_vn_midnight()}
    if limit > 0:
        day.update(limit=limit, remaining=max(0, limit - used))
    return {
        "plan": settings.shortvid_plan or None,
        "status": "active" if credits is None or credits > 0 else "no_credits",
        "expires_at": None,
        "day": day,
        "minute": {"limit": settings.shortvid_requests_per_minute},
        "credits_remaining": credits,
    }


async def shortvid_get(
    client: httpx.AsyncClient,
    wait: Callable[[], Awaitable[None]],
    api_key: str,
    path: str,
    params: dict[str, Any],
    *,
    retries: int = 2,
) -> dict[str, Any] | list[Any] | None:
    """Một request GET -> `data` của response, hoặc None khi không tồn tại (404 / code != 0 kiểu "not found").
    `wait`: chờ tới lượt theo nhịp mỗi phút (pacer của lượt). 429 chờ Retry-After; 5xx và code != 0 tạm thời thử lại lâu dần;
    401/403 (key sai) và 402 (hết credit) -> ShortVidStop. Hết lượt thử thì raise - bên gọi giữ nguyên con trỏ, lượt sau thử
    lại đúng trang đó."""
    url = f"{settings.shortvid_api_base_url.rstrip('/')}/{path.lstrip('/')}"
    clean = {key: str(value) for key, value in params.items() if value is not None and value != ""}
    last: str = ""
    for attempt in range(retries + 1):
        await wait()
        resp = await client.get(url, params=clean, headers={"x-api-key": api_key})
        if resp.status_code == 429:
            retry = min(float(resp.headers.get("Retry-After") or 30), 120)
            logger.warning("shortvid_rate_limited", path=path, retry_after=retry)
            await asyncio.sleep(retry)
            continue
        if resp.status_code in (401, 403):
            raise ShortVidStop(f"http_{resp.status_code}: {resp.text[:200]}")
        if resp.status_code == 402:
            raise ShortVidStop(f"no_credits: {resp.text[:200]}")
        if resp.status_code == 404:
            return None
        if resp.status_code >= 500:
            last = f"http_{resp.status_code}"
            await asyncio.sleep(5 * (attempt + 1))
            continue
        resp.raise_for_status()
        body = resp.json() or {}
        code = body.get("code")
        if code == 0:
            await _count_request(resp.headers.get("x-credits-remaining"))
            return body.get("data")
        msg = str(body.get("msg") or "")
        if any(word in msg.lower() for word in ("credit", "balance", "insufficient")):
            raise ShortVidStop(f"no_credits: {msg[:200]}")
        if any(word in msg.lower() for word in ("not found", "not exist", "no data", "private")):
            return None
        last = f"code_{code}: {msg[:120]}"
        logger.info("shortvid_upstream_retry", path=path, attempt=attempt + 1, error=last)
        await asyncio.sleep(5 * (attempt + 1))
    raise RuntimeError(f"shortvid_retries_exhausted path={path} last={last}")


def next_cursor(data: dict[str, Any] | None) -> str | None:
    """Cursor trang kế tiếp (`data.cursor` khi `data.hasMore`), hoặc None khi hết."""
    if not isinstance(data, dict):
        return None
    has_more = data.get("hasMore", data.get("has_more"))
    cursor = data.get("cursor")
    return str(cursor) if has_more in (True, "true", 1) and cursor not in (None, "") else None


def videos_of(data: Any) -> list[dict[str, Any]]:
    """Danh sách video trong `data`: `data.videos` (đa số endpoint) hoặc chính `data` (feed/list)."""
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return [item for item in (data or {}).get("videos") or [] if isinstance(item, dict)]


def _int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except TypeError, ValueError:
        return None


def _hashtags(item: dict[str, Any]) -> list[str]:
    """Hashtag của video: trường `hashtags` (user/posts) hoặc tách từ `title` (các endpoint khác không có mảng riêng)."""
    tags = item.get("hashtags")
    if isinstance(tags, list) and tags:
        return [str(tag.get("name") if isinstance(tag, dict) else tag).lstrip("#") for tag in tags if tag]
    return re.findall(r"#([^\s#.,!?;:]+)", str(item.get("title") or ""))


def map_video(item: dict[str, Any], source: str = "shortvid") -> dict[str, Any] | None:
    """Một video ShortVid -> cùng tên trường với TikTokVideoItem của spider-hub (spiders/tiktok/features/hashtag_search/
    extract.py), để ingest consumer xử lý như bài của spider. None khi thiếu id. ShortVid chỉ trả id/unique_id/nickname/avatar
    của tác giả - không có follower, tick xanh, tài khoản business (để None/False)."""
    video_id = str(item.get("aweme_id") or item.get("video_id") or item.get("id") or "").strip()
    if not video_id:
        return None
    author = item.get("author") or {}
    username = author.get("unique_id")
    kind = "photo" if item.get("content_type") == "image" or item.get("images") else "video"
    commerce = item.get("commerce_info") or {}
    return {
        "video_id": video_id,
        "url": f"https://www.tiktok.com/@{username}/{kind}/{video_id}" if username else item.get("share_url"),
        "desc": item.get("title"),
        "text_language": None,
        "location_created": item.get("region"),
        "create_time": _int(item.get("create_time")),
        "author_id": str(author.get("id")) if author.get("id") else None,
        "author_username": username,
        "author_name": author.get("nickname"),
        "author_avatar_url": author.get("avatar"),
        "duration": _int(item.get("duration")),
        "cover_url": item.get("origin_cover") or item.get("cover"),
        "play_url": item.get("play"),
        "music_title": (item.get("music_info") or {}).get("title"),
        "hashtags": _hashtags(item),
        "play_count": _int(item.get("play_count")),
        "like_count": _int(item.get("digg_count")),
        "comment_count": _int(item.get("comment_count")),
        "share_count": _int(item.get("share_count")),
        "collect_count": _int(item.get("collect_count")),
        "author_followers": None,
        "author_verified": False,
        "author_is_business": bool(item.get("is_ecommerce")),
        "author_is_seller": bool(item.get("is_ecommerce")),
        "author_signature": None,
        "is_ad": bool(item.get("is_ad")),
        "is_branded": bool(item.get("is_paid_partnership")) or (_int(commerce.get("branded_content_type")) or 0) > 0,
        "source": source,
    }


def map_comment(raw: dict[str, Any], *, parent_comment_id: str | None = None) -> dict[str, Any] | None:
    """Một bình luận ShortVid (comment/list, comment/reply) -> tên trường TikTokCommentItem của spider-hub
    (spiders/tiktok/features/comments/extract.py). None khi thiếu id."""
    comment_id = str(raw.get("id") or raw.get("cid") or "").strip()
    if not comment_id:
        return None
    user = raw.get("user") or {}
    return {
        "comment_id": comment_id,
        "message": raw.get("text"),
        "timestamp": _int(raw.get("create_time")),
        "like_count": _int(raw.get("digg_count")) or 0,
        "reply_count": _int(raw.get("reply_total")) or 0,
        "author_id": str(user.get("id")) if user.get("id") else None,
        "author_username": user.get("unique_id"),
        "author_name": user.get("nickname"),
        "author_avatar_url": user.get("avatar"),
        "parent_comment_id": parent_comment_id,
    }


def hashtag_info(data: dict[str, Any] | None) -> dict[str, Any]:
    """`data` của challenge/info -> {hashtag_id, video_count, view_count}."""
    info = data or {}
    return {
        "hashtag_id": str(info.get("id") or "") or None,
        "video_count": _int(info.get("user_count") or info.get("userCount")) or 0,
        "view_count": _int(info.get("view_count") or info.get("viewCount")) or 0,
    }
