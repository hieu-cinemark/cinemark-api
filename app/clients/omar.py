"""Client Omar TikTok API (dev.omar-thing.site/api/v1) - nguồn TikTok của cinemark-api.

Chỉ dùng bốn endpoint (tài liệu: dev.omar-thing.site/docs.html):
- hashtag_videos   name | id, cursor -> video của hashtag (dạng gốc của TikTok - có đủ tác giả)
- hashtag_info     name | id -> số video / lượt xem của hashtag (format=clean)
- comments         video (id), cursor -> bình luận cấp một
- comment_replies  video, comment_id, cursor -> trả lời của một bình luận
Cộng GET /usage (không tính vào rate limit) để đọc giới hạn ngày / phút của gói.

Xác thực bằng header `X-API-Key`. Thành công: {"status": "success", "data": {...}}; lỗi: {"status": "error", "code", "message"}
(401 MISSING_KEY, 403 INVALID_KEY / KEY_DISABLED / KEY_EXPIRED, 404 NOT_FOUND, 424 FETCH_FAILED - TikTok phía Omar lỗi tạm thời,
429 RATE_LIMIT_EXCEEDED kèm Retry-After). Phân trang bằng cursor: truyền `next_cursor` của response trước; còn trang sau khi
`has_more` là true và có `next_cursor`.

API key đọc từ bảng omar_api_settings trên Supabase (nhập ở Settings > Omar trên dashboard), không từ env."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import httpx

from app.core.config import settings
from app.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = get_logger(__name__)


class OmarStop(Exception):
    """Key sai / hết hạn / hết hạn mức ngày - dừng cả lượt, không thử tiếp các việc khác."""


async def omar_get(
    client: httpx.AsyncClient,
    wait: Callable[[], Awaitable[None]],
    api_key: str,
    path: str,
    params: dict[str, Any],
    *,
    retries: int = 3,
) -> dict[str, Any] | None:
    """Một request GET -> `data` của response, hoặc None khi 404 (không tồn tại).

    `wait`: chờ tới lượt theo nhịp mỗi phút (pacer của lượt). 429 chờ Retry-After (kèm "day" và chờ lâu thì là hết hạn mức
    ngày -> OmarStop); 424 (TikTok phía Omar lỗi tạm thời, đo ~15% trang) chờ lâu dần rồi thử lại; 401/403 (key sai / hết
    hạn) -> OmarStop. Hết lượt thử thì raise chứ không trả "rỗng" - bên gọi giữ nguyên con trỏ, lượt sau thử lại đúng
    trang đó."""
    url = f"{settings.omar_api_base_url.rstrip('/')}/{path.lstrip('/')}"
    clean = {key: str(value) for key, value in params.items() if value is not None and value != ""}
    resp: httpx.Response | None = None
    for attempt in range(retries + 1):
        await wait()
        resp = await client.get(url, params=clean, headers={"X-API-Key": api_key})
        if resp.status_code == 429:
            retry = min(float(resp.headers.get("Retry-After") or 30), 120)
            body = resp.text[:200]
            if "day" in body.lower() and retry > 60:
                raise OmarStop(f"daily_limit: {body}")
            logger.warning("omar_rate_limited", path=path, retry_after=retry)
            await asyncio.sleep(retry)
            continue
        if resp.status_code in (401, 403):
            raise OmarStop(f"http_{resp.status_code}: {resp.text[:200]}")
        if resp.status_code == 424 and attempt < retries:
            logger.info("omar_fetch_failed_retry", path=path, attempt=attempt + 1)
            await asyncio.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return (resp.json() or {}).get("data") or {}
    raise RuntimeError(f"omar_retries_exhausted path={path} status={resp.status_code if resp else None}")


async def fetch_usage(api_key: str, client: httpx.AsyncClient | None = None) -> dict[str, Any] | None:
    """GET /usage: {"plan", "status", "expires_at", "day": {...}, "minute": {...}} hoặc None khi không đọc được. Đọc giới hạn
    của GÓI mỗi lượt nên nâng gói Omar thì cào nhanh/nhiều hơn mà không phải sửa gì. Dùng cho cả lượt cào và tab Omar."""
    url = f"{settings.omar_api_base_url.rstrip('/')}/usage"
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=20) as own:
                resp = await own.get(url, headers={"X-API-Key": api_key})
        else:
            resp = await client.get(url, headers={"X-API-Key": api_key})
        data = (resp.json() or {}).get("data") or {}
        return {
            "plan": data.get("plan"),
            "status": data.get("status"),
            "expires_at": data.get("expires_at"),
            **(data.get("usage") or {}),
        }
    except Exception as exc:  # noqa: BLE001 - không đọc được thì dùng ngân sách mặc định
        logger.warning("omar_usage_failed", error=str(exc)[:200])
        return None


def next_cursor(data: dict[str, Any] | None) -> str | None:
    """Cursor trang kế tiếp từ `data` của một endpoint phân trang, hoặc None khi hết."""
    if not isinstance(data, dict):
        return None
    has_more = str(data.get("has_more")).lower() == "true"
    return str(data["next_cursor"]) if has_more and data.get("next_cursor") else None


def videos_of(data: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Video gốc trong `data.videos` của hashtag_videos."""
    return [item for item in (data or {}).get("videos") or [] if isinstance(item, dict)]


def _first_url(node: Any) -> str | None:
    urls = (node or {}).get("url_list") if isinstance(node, dict) else None
    return next((u for u in urls or [] if isinstance(u, str) and u.startswith("http")), None)


def map_video(item: dict[str, Any]) -> dict[str, Any] | None:
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
    # Bài ảnh/slideshow có image_post_info - link /photo/.
    kind = "photo" if item.get("image_post_info") else "video"
    return {
        "video_id": video_id,
        "url": f"https://www.tiktok.com/@{username}/{kind}/{video_id}" if username else item.get("share_url"),
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
        # Tín hiệu tác giả cho phân loại tầng (Organic/Paid/Owned/Media) không cần AI. verification_type = 1 với MỌI tài
        # khoản (không phải tick xanh - tick xanh là custom_verify); follower_count thường 0 (endpoint không trả) nên để None.
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


def map_comment(raw: dict[str, Any], *, parent_comment_id: str | None = None) -> dict[str, Any] | None:
    """Một bình luận dạng gốc của TikTok -> tên trường TikTokCommentItem của spider-hub. None khi thiếu id."""
    cid = raw.get("cid")
    if not cid:
        return None
    user = raw.get("user") or {}
    reply_id = str(raw.get("reply_id") or "")
    parent = parent_comment_id or (reply_id if reply_id and reply_id != "0" else None)
    return {
        "comment_id": str(cid),
        "message": raw.get("text"),
        "timestamp": raw.get("create_time"),
        "like_count": raw.get("digg_count") or 0,
        "reply_count": raw.get("reply_comment_total") or 0,
        "author_id": user.get("uid"),
        "author_username": user.get("unique_id"),
        "author_name": user.get("nickname"),
        "author_avatar_url": ((user.get("avatar_thumb") or {}).get("url_list") or [None])[0],
        "parent_comment_id": parent,
    }


def hashtag_info(data: dict[str, Any] | None) -> dict[str, Any]:
    """`data` của hashtag_info (format=clean) -> {video_count, view_count}. Tên trường khác nhau giữa format=clean và dạng gốc
    của app (ch_info.user_count = số video) - lấy cái có số."""
    info = (data or {}).get("hashtag") or {}
    nested = {**(info.get("ch_info") or {}), **(info.get("stats") or {}), **(info.get("statistics") or {})}

    def pick(*names: str) -> int:
        for source in (info, nested):
            for name in names:
                try:
                    if int(source.get(name) or 0) > 0:
                        return int(source[name])
                except TypeError, ValueError:
                    continue
        return 0

    return {
        "hashtag_id": str(info.get("id") or info.get("cid") or "") or None,
        "video_count": pick("video_count", "videoCount", "user_count", "use_count"),
        "view_count": pick("view_count", "viewCount", "play_count"),
    }
