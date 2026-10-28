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
import hashlib
import heapq
import itertools
import json
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.clients.kafka import publish_raw_post
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.store import OMAR_COOCCUR_KEY_TMPL, d1_query, get_enabled_keywords
from app.services.accounts.platform_config_db import get_omar_api_settings
from app.services.omar import omar_channels, omar_comments, omar_search, omar_yield

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
    # Bài ảnh/slideshow có image_post_info - link /photo/ (trước 12/10/2026 mọi bài đều lưu /video/).
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


def map_web_item(item: dict[str, Any]) -> dict[str, Any] | None:
    """Một kết quả tìm kiếm của Omar (search_videos / search_photos trả dạng TikTok WEB: id, createTime, author.uniqueId,
    stats.diggCount...) -> cùng tên trường với map_aweme. Port của extract_video ở spider-hub
    (spiders/tiktok/features/hashtag_search/extract.py)."""
    video_id = str(item.get("id") or "").strip()
    if not video_id:
        return None
    author = item.get("author") or {}
    stats = item.get("statsV2") or item.get("stats") or {}
    video = item.get("video") or {}
    username = author.get("uniqueId")
    kind = "photo" if item.get("imagePost") else "video"

    def num(key: str) -> int | None:
        try:
            value = stats.get(key)
            return int(value) if value is not None else None
        except TypeError, ValueError:
            return None

    return {
        "video_id": video_id,
        "url": f"https://www.tiktok.com/@{username}/{kind}/{video_id}" if username else None,
        "desc": item.get("desc"),
        "text_language": item.get("textLanguage"),
        "location_created": item.get("locationCreated"),
        "create_time": item.get("createTime"),
        "author_id": author.get("id"),
        "author_username": username,
        "author_name": author.get("nickname"),
        "author_avatar_url": author.get("avatarThumb"),
        "duration": video.get("duration") or None,
        "cover_url": video.get("originCover") or video.get("cover"),
        "play_url": video.get("playAddr") or None,
        "music_title": (item.get("music") or {}).get("title"),
        "hashtags": [
            extra["hashtagName"]
            for content in item.get("contents") or []
            for extra in content.get("textExtra") or []
            if extra.get("hashtagName")
        ],
        "play_count": num("playCount"),
        "like_count": num("diggCount"),
        "comment_count": num("commentCount"),
        "share_count": num("shareCount"),
        "collect_count": num("collectCount"),
        "author_followers": (item.get("authorStats") or {}).get("followerCount") or None,
        "author_verified": bool(author.get("verified")),
        "is_ad": bool(item.get("isAd")),
        "source": "omar_search",
    }


async def publish_videos(
    mapped: list[dict[str, Any]], keyword_id: str, stats: dict[str, Any], source: str | None = None
) -> None:
    """Gửi video đã map lên raw_posts như spider (chống trùng qua tập seen dùng chung với spider, _claim_unseen).
    `source` (hashtag/search/channel): ghi số gửi đi và số mới thật vào omar_yield."""
    brand_new: set[str] = set()
    fresh_ids = await _claim_unseen([video["video_id"] for video in mapped], brand_new)
    sent_before = int(stats.get("published") or 0)
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
            await _forget_seen(video["video_id"])  # chưa tới Kafka: lần sau gửi lại
    if source:
        await omar_yield.add(source, sent=int(stats.get("published") or 0) - sent_before, new=len(brand_new))


class _Pacer:
    """Rải đều thời điểm BẮT ĐẦU các request theo giới hạn mỗi phút của gói - dùng chung cho mọi worker chạy song song
    (request chạy chồng lên nhau, chỉ lúc bắt đầu bị giãn cách), nên tận dụng hết giới hạn mà không bị 429."""

    def __init__(self, per_minute: int) -> None:
        self.gap = 60.0 / max(per_minute, 1) + 0.05
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            start = max(now, self.next_at)
            self.next_at = start + self.gap
        if start > now:
            await asyncio.sleep(start - now)


class Budget:
    """Ngân sách request của một lượt, dùng chung cho mọi việc trong lượt (bình luận tuần, hashtag, làm mới trang 1) và
    mọi worker song song - giữ chỗ TRƯỚC khi gọi nên chạy song song không vượt."""

    def __init__(self, limit: int, parent: Budget | None = None) -> None:
        self.limit = limit
        self.used = 0
        # Ngân sách con (vd. phần bình luận tuần): mỗi request trừ cả vào ngân sách cha của lượt.
        self.parent = parent

    def take(self) -> bool:
        if self.used >= self.limit or (self.parent is not None and not self.parent.take()):
            return False
        self.used += 1
        return True

    @property
    def exhausted(self) -> bool:
        return self.used >= self.limit or (self.parent is not None and self.parent.exhausted)


async def omar_get(
    client: httpx.AsyncClient, pacer: _Pacer, api_key: str, path: str, params: dict[str, str], *, retries_424: int = 3
) -> dict[str, Any] | None:
    """Một request GET tới Omar -> `data` của response, hoặc None khi 404 (không tồn tại). Dùng chung cho mọi endpoint
    (hashtag_videos, hashtag_info, comments, comment_replies): giữ nhịp theo pacer, 429 chờ Retry-After, 424 (TikTok phía
    Omar lỗi tạm thời, đo 2026-10-12 ~15% trang) chờ lâu dần rồi thử lại, 401/403/hết hạn mức ngày -> OmarStop. Hết lượt
    thử thì raise chứ không trả "rỗng" - bên gọi giữ nguyên con trỏ, lượt sau thử lại đúng trang đó."""
    resp: httpx.Response | None = None
    for attempt in range(retries_424 + 1):
        await pacer.wait()
        resp = await client.get(
            f"{settings.omar_api_base_url.rstrip('/')}/{path}", params=params, headers={"X-API-Key": api_key}
        )
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
        if resp.status_code == 424 and attempt < retries_424:
            logger.info("omar_fetch_failed_retry", path=path, attempt=attempt + 1)
            await asyncio.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return (resp.json() or {}).get("data") or {}
    raise RuntimeError(f"omar_retries_exhausted path={path} status={resp.status_code if resp else None}")


def next_cursor(data: dict[str, Any]) -> str | None:
    """Cursor trang kế tiếp từ `data` của một endpoint phân trang, hoặc None khi hết."""
    has_more = str(data.get("has_more")).lower() == "true"
    return str(data["next_cursor"]) if has_more and data.get("next_cursor") else None


async def _fetch_page(
    client: httpx.AsyncClient, pacer: _Pacer, api_key: str, hashtag: str, cursor: str, hashtag_id: str | None = None
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    """Một trang hashtag_videos -> (video gốc, cursor kế tiếp hoặc None khi hết, hashtag_id). Có `hashtag_id` (lưu từ trang
    trước) thì gọi bằng `id` - Omar bỏ qua bước tra tên hashtag, nhanh hơn và tránh lỗi tra tên."""
    params = {"id": hashtag_id, "cursor": cursor} if hashtag_id else {"name": hashtag, "cursor": cursor}
    data = await omar_get(client, pacer, api_key, "hashtag_videos", params)
    if data is None:
        return [], None, hashtag_id  # hashtag không tồn tại
    return list(data.get("videos") or []), next_cursor(data), str(data.get("hashtag_id") or "") or hashtag_id


async def hashtag_exists(hashtag: str) -> bool | None:
    """Hashtag có video trên TikTok không (trang 1 của hashtag_videos). None khi không biết (chưa có key, Omar lỗi khác).
    Omar trả 424 cho hashtag không tồn tại (đo 12/10/2026: biến thể kiểu #ChiChiEmEm32026 luôn 424, #chichiemem3 có video)."""
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    if not api_key:
        return None
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        try:
            data = await omar_get(
                client,
                _Pacer(settings.omar_requests_per_minute),
                api_key,
                "hashtag_videos",
                {"name": hashtag.lstrip("#"), "cursor": "0"},
                retries_424=1,
            )
        except httpx.HTTPStatusError as exc:
            return False if exc.response.status_code == 424 else None
        except Exception:  # noqa: BLE001 - không kiểm tra được thì coi như chưa biết
            return None
    return bool(data and data.get("videos"))


async def fetch_hashtag_info(client: httpx.AsyncClient, pacer: _Pacer, api_key: str, hashtag: str) -> dict[str, Any]:
    """Tổng số video / lượt xem TikTok báo cho hashtag (hashtag_info, format=clean) - để biết đã cào được bao nhiêu phần."""
    # 424 ở endpoint này thường là "Hashtag not found or lookup failed" (đo 2026-10-12) - chỉ thử lại một lần.
    data = await omar_get(client, pacer, api_key, "hashtag_info", {"name": hashtag, "format": "clean"}, retries_424=1)
    info = (data or {}).get("hashtag") or {}
    nested = {**(info.get("ch_info") or {}), **(info.get("stats") or {}), **(info.get("statistics") or {})}

    def pick(*names: str) -> int:
        # Tên trường khác nhau giữa format=clean / dạng gốc của app (cha_info.user_count = số video) - lấy cái có số.
        for source in (info, nested):
            for name in names:
                try:
                    if int(source.get(name) or 0) > 0:
                        return int(source[name])
                except TypeError, ValueError:
                    continue
        return 0

    return {
        "video_count": pick("video_count", "videoCount", "user_count", "use_count"),
        "view_count": pick("view_count", "viewCount", "play_count"),
    }


_STATE_KEY = f"{REDIS_KEY_PREFIX}omar:hashtag_state"
# Trang rỗng liên tiếp mà vẫn has_more: coi như đã hết (tránh lặp vô hạn nếu Omar trả cursor lỗi).
_MAX_EMPTY_PAGES = 3
_MAX_PAGE_FAILURES = 3
# Trang rỗng GIỮA CHỪNG (cursor khác "0") thường là Omar/TikTok giới hạn tạm thời - gọi lại sau vài phút lại có data (đo
# 16/10/2026). Để từ khoá đó chờ rồi gọi lại đúng cursor, lần chờ thứ n = n x _EMPTY_RETRY_SECONDS (3, 6, 9 phút), tối
# đa _EMPTY_RETRIES lần; vẫn rỗng thì để đó (giữ cursor, lượt sau đi tiếp) và làm từ khoá khác. Hashtag "để đó" _MAX_STALL_ROUNDS lượt liền thì coi là đã hết.
_EMPTY_RETRY_SECONDS = 180.0
_EMPTY_RETRIES = 3
_MAX_STALL_ROUNDS = 3
# Khoảng tối thiểu giữa hai request của CÙNG một từ khoá - các worker xen kẽ giữa các từ khoá thay vì gọi dồn một từ khoá.
_KEYWORD_GAP_SECONDS = 3.0


async def run_round_robin(
    items: list[Any],
    step: Callable[[Any], Awaitable[float | None]],
    *,
    workers: int,
    should_stop: Callable[[], bool],
) -> None:
    """Chạy `step` xoay vòng trên `items` với tối đa `workers` worker song song: mỗi lần một worker lấy item sẵn sàng sớm
    nhất, gọi step (một trang), step trả về số giây chờ trước lần gọi kế của item đó (xếp lại hàng) hoặc None (item xong).
    Một item chỉ ở một worker tại một thời điểm, nên các request xen kẽ giữa các từ khoá."""
    if not items:
        return
    order = itertools.count()
    heap: list[tuple[float, int, Any]] = [(0.0, next(order), item) for item in items]
    busy = 0

    async def worker() -> None:
        nonlocal busy
        while not should_stop():
            if not heap:
                if busy == 0:
                    return
                await asyncio.sleep(0.5)  # item đang ở worker khác có thể được xếp lại
                continue
            wait = heap[0][0] - time.monotonic()
            if wait > 0:
                await asyncio.sleep(min(wait, 1.0))
                continue
            _, _, item = heapq.heappop(heap)
            busy += 1
            try:
                delay = await step(item)
            finally:
                busy -= 1
            if delay is not None:
                heapq.heappush(heap, (time.monotonic() + delay, next(order), item))

    await asyncio.gather(*(worker() for _ in range(max(1, min(workers, len(items))))))


async def fetch_usage(api_key: str, client: httpx.AsyncClient | None = None) -> dict[str, Any] | None:
    """GET /usage: {"plan", "status", "expires_at", "day": {...}, "minute": {...}} hoặc None khi không đọc được. Đọc giới hạn
    của GÓI mỗi lượt nên nâng gói Omar thì cào nhanh/nhiều hơn mà không phải sửa gì. Dùng cho cả lượt cào và tab Omar."""
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=20) as own:
                resp = await own.get(f"{settings.omar_api_base_url.rstrip('/')}/usage", headers={"X-API-Key": api_key})
        else:
            resp = await client.get(f"{settings.omar_api_base_url.rstrip('/')}/usage", headers={"X-API-Key": api_key})
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


_SEEN_PREFIX = f"{REDIS_KEY_PREFIX}omar:seen:"
# Set video TikTok đã gửi lên raw_posts của spider-hub (constants/tiktok.py SEEN_POSTS_KEY, cùng prefix) - dùng chung để
# spider không gửi lại video Omar đã gửi và ngược lại.
_SPIDER_SEEN_VIDEOS_KEY = f"{REDIS_KEY_PREFIX}tiktok:seen_video_ids"


async def _claim_unseen(video_ids: list[str], brand_new: set[str] | None = None) -> set[str]:
    """Video cần gửi lên raw_posts: video MỚI (chưa có trong set đã gửi dùng chung với spider) hoặc video cũ đã quá
    settings.omar_republish_hours giờ chưa gửi lại (cập nhật tương tác). Đánh dấu luôn những video đó. Với gói lớn (~1 triệu
    video/ngày, phần lớn trùng) gửi lại mọi video làm ingest/D1 quá tải. Redis lỗi thì coi như mới hết (thà ghi trùng còn
    hơn mất bài)."""
    if not video_ids:
        return set()
    ttl = int(settings.omar_republish_hours * 3600)
    try:
        pipe = get_redis_client().pipeline()
        for video_id in video_ids:
            pipe.sadd(_SPIDER_SEEN_VIDEOS_KEY, video_id)
            if ttl > 0:
                pipe.set(f"{_SEEN_PREFIX}{video_id}", "1", nx=True, ex=ttl)
        results = await pipe.execute()
    except Exception as exc:  # noqa: BLE001
        logger.warning("omar_seen_check_failed", error=str(exc)[:200])
        return set(video_ids)
    step = 2 if ttl > 0 else 1
    out = set()
    for i, video_id in enumerate(video_ids):
        is_new = bool(results[i * step])
        refresh_due = bool(results[i * step + 1]) if ttl > 0 else True
        if is_new or refresh_due:
            out.add(video_id)
        if is_new and brand_new is not None:
            brand_new.add(video_id)  # lần đầu thấy (chưa có trong tập chống trùng dùng chung với spider)
    return out


async def _forget_seen(video_id: str) -> None:
    """Gửi Kafka lỗi: bỏ đánh dấu để lần sau gửi lại."""
    try:
        pipe = get_redis_client().pipeline()
        pipe.srem(_SPIDER_SEEN_VIDEOS_KEY, video_id)
        pipe.delete(f"{_SEEN_PREFIX}{video_id}")
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001
        logger.warning("omar_seen_forget_failed", error=str(exc)[:200])


# BFS: hashtag đi kèm trong video Omar lấy về, đếm theo từ khoá (hash tag -> số video), giữ 14 ngày. d1.related_hashtags_for_keywords
# gộp với hashtag liên quan của spider - cùng một nguồn cho chip duyệt trên dashboard và cho Kira duyệt (hashtag_discovery).
_COOCCUR_TTL_SECONDS = 14 * 24 * 3600


async def _count_cooccur(keyword: str, keyword_id: str, videos: list[dict[str, Any]]) -> None:
    """Cộng số video có mỗi hashtag đi kèm (khác chính hashtag đang cào). Giữ nguyên dấu, chỉ hạ chữ thường: TikTok không
    phân biệt hoa thường nhưng #trạibuônngười và #traibuonnguoi là hai hashtag khác nhau."""
    own = keyword.lstrip("#").strip().lower()
    counts: dict[str, int] = {}
    for video in videos:
        for tag in {str(t).strip().lower() for t in video.get("hashtags") or [] if str(t).strip()}:
            if tag != own and 3 <= len(tag) <= 60:
                counts[tag] = counts.get(tag, 0) + 1
    if not counts:
        return
    key = OMAR_COOCCUR_KEY_TMPL.format(keyword_id=keyword_id)
    try:
        pipe = get_redis_client().pipeline()
        for tag, n in counts.items():
            pipe.hincrby(key, tag, n)
        pipe.expire(key, _COOCCUR_TTL_SECONDS)
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001 - thiếu số đếm chỉ làm BFS chậm hơn
        logger.warning("omar_cooccur_failed", error=str(exc)[:200])


# Tạm dừng khi hết hạn mức (2026-10-15): lượt kết thúc vì dùng hết hạn mức ngày (hoặc key sai) thì ghi lại tới khi nào
# chạy được - scheduler không gọi Omar nữa (kể cả /usage) cho tới lúc đó rồi mở lượt NGAY khi hạn mức reset, không đợi
# mốc giờ kế tiếp. Con trỏ hashtag / hàng đợi bình luận / tìm kiếm vốn lưu sau mỗi trang nên lượt sau đi tiếp đúng chỗ.
_PAUSE_KEY = f"{REDIS_KEY_PREFIX}omar:paused"
_LAST_ROUND_KEY = f"{REDIS_KEY_PREFIX}omar:last_round"
# Key sai/hết hạn: thử lại khi đổi key, hoặc sau chừng này giờ nếu vẫn key cũ.
_KEY_PAUSE_HOURS = 6
# Hạn mức reset xong chừng này giây mới mở lượt - tránh gọi đúng lúc phía Omar chưa kịp reset.
_RESUME_SLACK_SECONDS = 60


def key_fingerprint(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()[:12] if api_key else ""


def _log_stop(exc: OmarStop) -> None:
    quota = str(exc).startswith("daily_limit")
    # Hết hạn mức ngày là chuyện bình thường của chế độ chạy liên tục - chỉ báo Telegram khi key có vấn đề.
    if quota:
        logger.info("omar_round_stopped", error=str(exc))
    else:
        logger.error("omar_round_stopped", error=str(exc), telegram=True)


async def get_pause() -> dict[str, Any] | None:
    try:
        raw = await get_redis_client().get(_PAUSE_KEY)
        return json.loads(raw) if raw else None
    except Exception as exc:  # noqa: BLE001 - không đọc được thì coi như không dừng (lượt tự kiểm tra hạn mức)
        logger.warning("omar_pause_read_failed", error=str(exc)[:200])
        return None


async def set_pause(reason: str, until: datetime, *, api_key: str = "") -> None:
    entry = {
        "reason": reason,  # "quota" (hết hạn mức ngày) | "key" (key sai/hết hạn)
        "until": until.astimezone(UTC).isoformat(),
        "at": datetime.now(tz=UTC).isoformat(),
        "key": key_fingerprint(api_key),
    }
    ttl = max(60, int((until - datetime.now(tz=UTC)).total_seconds()) + 7 * 24 * 3600)
    await get_redis_client().set(_PAUSE_KEY, json.dumps(entry), ex=ttl)
    logger.info("omar_paused", **entry)


async def clear_pause() -> None:
    await get_redis_client().delete(_PAUSE_KEY)


async def pause_over(pause: dict[str, Any]) -> bool:
    """Hết dừng chưa: tới giờ `until`, hoặc (dừng vì key) key trong Supabase đã đổi."""
    try:
        until = datetime.fromisoformat(pause["until"])
    except KeyError, TypeError, ValueError:
        return True
    if datetime.now(tz=UTC) >= until:
        return True
    if pause.get("reason") == "key":
        row = await get_omar_api_settings()
        current = key_fingerprint(((row or {}).get("api_key") or "").strip())
        return bool(current) and current != pause.get("key")
    return False


async def _pause_after_round(stats: dict[str, Any], api_key: str, reset_at: datetime | None, exhausted: bool) -> None:
    """Ghi lại khi nào lượt kế tiếp chạy được, theo lý do lượt vừa dừng."""
    stopped = str(stats.get("stopped") or "")
    now = datetime.now(tz=UTC)
    if stopped.startswith(("http_401", "http_403")):
        await set_pause("key", now + timedelta(hours=_KEY_PAUSE_HOURS), api_key=api_key)
        return
    if not (stopped.startswith("daily_limit") or (exhausted and not settings.omar_spread_daily)):
        return
    # Đọc lại /usage (không tính vào hạn mức) để có giờ reset chính xác; không đọc được thì dùng giờ reset lúc mở lượt.
    usage = await fetch_usage(api_key)
    day = (usage or {}).get("day") or {}
    if day.get("resets_in") is not None:
        reset_at = now + timedelta(seconds=int(day["resets_in"]))
        if int(day.get("remaining") or 0) > settings.omar_daily_reserve and not stopped.startswith("daily_limit"):
            return  # vẫn còn hạn mức (vd. gói vừa được nâng) - lượt sau chạy bình thường
    await set_pause("quota", (reset_at or now + timedelta(hours=1)) + timedelta(seconds=_RESUME_SLACK_SECONDS))


async def last_round() -> dict[str, Any] | None:
    try:
        raw = await get_redis_client().get(_LAST_ROUND_KEY)
        return json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        return None


# Trạng thái lượt đang/vừa chạy - cho tab Omar trên dashboard (app/api/routes/omar.py) và scheduler (không mở lượt chồng).
_round: dict[str, Any] = {"running": False, "trigger": None, "started_at": None, "finished_at": None, "stats": {}}
_stop_requested = False


def round_status() -> dict[str, Any]:
    return {**_round, "stop_requested": _stop_requested}


async def round_status_full() -> dict[str, Any]:
    """Như round_status, nhưng sau restart (chưa có lượt nào trong tiến trình này) thì lấy lượt gần nhất đã lưu ở Redis."""
    if _round["running"] or _round["started_at"]:
        return round_status()
    saved = await last_round()
    return {**(saved or _round), "running": False, "stop_requested": False}


def is_running() -> bool:
    return bool(_round["running"])


def request_stop() -> bool:
    """Dừng lượt đang chạy sau request hiện tại (con trỏ đã lưu sau mỗi trang). False khi không có lượt nào chạy."""
    global _stop_requested
    if not _round["running"]:
        return False
    _stop_requested = True
    return True


def claim_round(trigger: str) -> bool:
    """Bật cờ đang chạy NGAY khi ra lệnh (đồng bộ, trước khi task kịp chạy) - không thì bấm "Chạy ngay" hai lần, hoặc bấm
    đúng lúc lượt theo lịch vừa kích hoạt, mở hai lượt song song tranh con trỏ và tốn gấp đôi request. False khi đang
    có lượt chạy."""
    global _stop_requested
    if _round["running"]:
        return False
    _stop_requested = False
    _round.update(
        running=True, trigger=trigger, started_at=datetime.now(tz=UTC).isoformat(), finished_at=None, stats={}
    )
    return True


async def run_omar_round_tracked(trigger: str) -> dict[str, Any]:
    """run_omar_round kèm thống kê sống cho dashboard. Bên gọi phải claim_round(trigger) thành công trước."""
    global _stop_requested
    try:
        return await run_omar_round(live=_round["stats"])
    finally:
        _round.update(running=False, finished_at=datetime.now(tz=UTC).isoformat())
        _stop_requested = False
        try:
            await get_redis_client().set(_LAST_ROUND_KEY, json.dumps(_round, default=str), ex=30 * 24 * 3600)
        except Exception as exc:  # noqa: BLE001 - chỉ là thông tin hiển thị
            logger.warning("omar_last_round_save_failed", error=str(exc)[:200])


# Hashtag đang được cào riêng (crawl_hashtag_now) - lượt Omar bỏ qua để không cào trùng con trỏ.
_manual_running: set[str] = set()
_manual_tasks: set[asyncio.Task] = set()
_RESET_KEEP = ("video_count", "view_count", "info_at", "hashtag_id")


async def _crawl_hashtag_now(keyword: dict[str, Any]) -> None:
    kw_id = keyword["id"]
    api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
    if not api_key:
        return
    redis = get_redis_client()
    raw = await redis.hget(_STATE_KEY, kw_id)
    entry = json.loads(raw) if raw else {}
    if entry.get("done_at"):
        entry = {k: entry[k] for k in _RESET_KEEP if k in entry}
    entry.pop("missing_at", None)
    entry.setdefault("started_at", datetime.now(tz=UTC).isoformat())
    cursor = entry.get("cursor") or "0"
    stats: dict[str, Any] = {"pages": 0, "videos": 0, "published": 0, "skipped_seen": 0}
    pacer = _Pacer(settings.omar_manual_requests_per_minute)
    failed = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        while True:
            try:
                videos, following, hashtag_id = await _fetch_page(
                    client, pacer, api_key, str(keyword["keyword"]).lstrip("#"), cursor, entry.get("hashtag_id")
                )
            except OmarStop as exc:
                logger.error("omar_manual_stopped", hashtag=keyword["keyword"], error=str(exc)[:200])
                break
            except Exception as exc:  # noqa: BLE001 - thử lại đúng trang, lỗi liền thì để lượt Omar đi tiếp
                failed += 1
                logger.warning(
                    "omar_manual_page_failed", hashtag=keyword["keyword"], cursor=cursor, error=str(exc)[:200]
                )
                if failed >= _MAX_PAGE_FAILURES:
                    if cursor == "0" and not entry.get("pages"):
                        entry["missing_at"] = datetime.now(tz=UTC).isoformat()
                    break
                await asyncio.sleep(30 * failed)
                continue
            failed = 0
            if hashtag_id:
                entry["hashtag_id"] = hashtag_id
            mapped = [video for video in (map_aweme(item) for item in videos) if video is not None]
            stats["pages"] += 1
            stats["videos"] += len(mapped)
            await _count_cooccur(str(keyword["keyword"]), kw_id, mapped)
            await publish_videos(mapped, kw_id, stats, source="hashtag")
            entry["pages"] = int(entry.get("pages") or 0) + 1
            if following is None:
                entry.update(cursor=None, done_at=datetime.now(tz=UTC).isoformat())
                break
            cursor = following
            entry["cursor"] = cursor
            await redis.hset(_STATE_KEY, kw_id, json.dumps(entry))
    await redis.hset(_STATE_KEY, kw_id, json.dumps(entry))
    logger.info("omar_manual_finished", hashtag=keyword["keyword"], done=bool(entry.get("done_at")), **stats)


def crawl_hashtag_now(keyword: dict[str, Any]) -> bool:
    """Cào NGAY một hashtag tới hết (ô "Thêm hashtag" ở tab Omar), độc lập với lượt đang chạy - lượt chế độ liên tục có
    thể kéo dài nhiều giờ và chỉ nạp danh sách hashtag lúc bắt đầu. False khi hashtag đang được cào riêng."""
    kw_id = keyword["id"]
    if kw_id in _manual_running:
        return False
    _manual_running.add(kw_id)

    async def run() -> None:
        try:
            await _crawl_hashtag_now(keyword)
        except Exception as exc:  # noqa: BLE001
            logger.error("omar_manual_failed", hashtag=keyword.get("keyword"), error=str(exc)[:300])
        finally:
            _manual_running.discard(kw_id)

    task = asyncio.create_task(run())
    _manual_tasks.add(task)
    task.add_done_callback(_manual_tasks.discard)
    return True


async def hashtag_progress() -> list[dict[str, Any]]:
    """Mỗi hashtag TikTok đang bật: tên, phim, số trang đã lấy, cursor, đã cào xong lúc nào (từ Redis omar:hashtag_state)."""
    keywords = [k for k in await get_enabled_keywords(platform="tiktok") if str(k["keyword"]).startswith("#")]
    titles = {row["id"]: row["title"] for row in await d1_query("SELECT id, title FROM movies", quiet=True) or []}
    raw = await get_redis_client().hgetall(_STATE_KEY) or {}
    state = {}
    for key, value in raw.items():
        try:
            state[key.decode() if isinstance(key, bytes) else key] = json.loads(value)
        except TypeError, ValueError:
            continue
    out = []
    for kw in keywords:
        entry = state.get(kw["id"]) or {}
        out.append(
            {
                "keyword_id": kw["id"],
                "hashtag": kw["keyword"],
                "movie": titles.get(kw["movie_id"]),
                "pages": int(entry.get("pages") or 0),
                "cursor": entry.get("cursor"),
                "done_at": entry.get("done_at"),
                "fresh_at": entry.get("fresh_at"),
                "video_count": entry.get("video_count"),
                "view_count": entry.get("view_count"),
                "info_at": entry.get("info_at"),
                "missing_at": entry.get("missing_at"),
            }
        )
    return sorted(out, key=lambda row: (row["done_at"] is not None, -row["pages"]))


async def run_omar_round(live: dict[str, Any] | None = None) -> dict[str, Any]:
    """Một lượt mỗi giờ: settings.omar_concurrency worker, mỗi worker cào một hashtag TỚI HẾT video rồi nhận hashtag kế
    (hashtag chưa bắt đầu đứng đầu hàng), tới khi hết ngân sách. Con trỏ mỗi hashtag lưu trong
    Redis sau mỗi trang, lượt sau / hôm sau đi tiếp; cào tới has_more = false thì xong, cào lại từ đầu sau
    settings.omar_recrawl_days ngày, hoặc sớm hơn khi lượt còn ngân sách dư (hashtag xong lâu nhất trước).

    Ngân sách: mặc định CHẠY LIÊN TỤC - toàn bộ hạn mức ngày còn lại (GET /usage, trừ settings.omar_daily_reserve) dùng
    ngay trong lượt, hết thì chờ reset / key mới (key đọc lại từ Supabase mỗi lượt). settings.omar_spread_daily = True thì
    rải đều: phần còn lại chia cho số giờ tới lúc reset, trần = số request một giờ ở giới hạn mỗi phút của gói.
    Nhịp gọi cũng theo giới hạn mỗi phút của gói - nâng gói Omar thì tự chạy nhanh/nhiều hơn."""
    row = await get_omar_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    stats: dict[str, Any] = live if live is not None else {}
    stats.update(keywords=0, requests=0, videos=0, published=0, skipped_seen=0, failed=0, finished=0)
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
        usage = await fetch_usage(api_key, client)
        day = (usage or {}).get("day") or {}
        per_minute = int(((usage or {}).get("minute") or {}).get("limit") or 0) or settings.omar_requests_per_minute
        hour_cap = per_minute * 55
        if settings.omar_requests_per_hour > 0:
            hour_cap = min(hour_cap, settings.omar_requests_per_hour)
        reset_at: datetime | None = None
        if day.get("remaining") is not None:
            remaining, resets_in = int(day["remaining"]), int(day.get("resets_in") or 0)
            reset_at = datetime.now(tz=UTC) + timedelta(seconds=resets_in)
            spendable = max(0, remaining - settings.omar_daily_reserve)
            if settings.omar_spread_daily:
                # Rải đều: phần còn lại chia cho số giờ tới lúc reset, trần một giờ.
                budget = min(hour_cap, spendable // max(1, -(-resets_in // 3600)))
            else:
                # Chạy liên tục (mặc định từ 2026-10-12): dùng hết hạn mức còn lại ngay trong lượt này - lượt có thể dài
                # nhiều giờ (scheduler không mở lượt chồng lên); hết thì chờ reset ngày / key mới.
                budget = spendable
        else:
            budget = hour_cap // 4
        stats["budget"] = budget
        stats["per_minute"] = per_minute
        if budget <= 0:
            logger.info("omar_round_no_budget", remaining=day.get("remaining"))
            if reset_at is not None:
                await set_pause("quota", reset_at + timedelta(seconds=_RESUME_SLACK_SECONDS))
            return {**stats, "skipped": "no_budget"}
        pacer = _Pacer(per_minute)
        spend = Budget(budget)

        def should_stop() -> bool:
            return _stop_requested or halted

        halted = False
        # Bình luận (app/services/omar/omar_comments.py) chạy SAU phần video mới (hashtag chưa xong, tìm kiếm, kênh), tối đa
        # settings.omar_comments_budget_share ngân sách, và TRƯỚC phần cào lại hashtag đã xong (phần đó dùng hết hạn mức
        # còn lại nên đặt sau thì bình luận không bao giờ tới lượt). Ưu tiên video (20/10/2026); trước đó bình luận chạy
        # song song và tranh nhịp gọi mỗi phút với video. Trước 12/10/2026 nó chạy trước và chặn mọi hashtag.
        comment_budget = Budget(int(budget * settings.omar_comments_budget_share), parent=spend)

        async def run_comments() -> None:
            nonlocal halted
            try:
                await omar_comments.process_queue(client, pacer, api_key, comment_budget, stats, should_stop)
            except OmarStop as exc:
                if not halted:
                    halted = True
                    stats["stopped"] = str(exc)[:100]
                    _log_stop(exc)
            except Exception as exc:  # noqa: BLE001 - phần bình luận lỗi không chặn phần hashtag
                logger.error("omar_comments_failed", error=str(exc)[:300])

        # Tìm theo từ khoá (app/services/omar/omar_search.py) mỗi ngày một lần mỗi câu - bắt bài không gắn/gắn sai hashtag phim.
        # Phần của tìm kiếm / kênh chỉnh theo hiệu quả đo được (bài mới trên mỗi request so với hashtag - omar_yield).
        shares = await omar_yield.adaptive_shares()
        stats["shares"] = shares
        search_budget = Budget(int(budget * shares["search"]), parent=spend)

        async def run_search() -> None:
            nonlocal halted
            try:
                await omar_search.run_search(client, pacer, api_key, search_budget, stats, should_stop)
            except OmarStop as exc:
                if not halted:
                    halted = True
                    stats["stopped"] = str(exc)[:100]
                    _log_stop(exc)
            except Exception as exc:  # noqa: BLE001 - phần tìm kiếm lỗi không chặn phần hashtag
                logger.error("omar_search_failed", error=str(exc)[:300])

        search_task = asyncio.create_task(run_search())
        # Cào theo kênh (app/services/omar/omar_channels.py): kênh TikTok đã có bài liên quan trong D1, mỗi kênh mỗi ngày.
        channel_budget = Budget(int(budget * shares["channel"]), parent=spend)

        async def run_channels() -> None:
            nonlocal halted
            try:
                await omar_channels.run_channels(client, pacer, api_key, channel_budget, stats, should_stop)
            except OmarStop as exc:
                if not halted:
                    halted = True
                    stats["stopped"] = str(exc)[:100]
                    _log_stop(exc)
            except Exception as exc:  # noqa: BLE001 - phần kênh lỗi không chặn phần hashtag
                logger.error("omar_channels_failed", error=str(exc)[:300])

        channels_task = asyncio.create_task(run_channels())
        # 2. Tổng video / lượt xem của từng hashtag (hashtag_info), làm mới mỗi settings.omar_hashtag_info_days ngày.
        info_before = (now - timedelta(days=settings.omar_hashtag_info_days)).isoformat()
        stale_info = [kw for kw in keywords if ((state.get(kw["id"]) or {}).get("info_at") or "") < info_before]

        async def refresh_info(kw: dict[str, Any]) -> None:
            if should_stop() or not spend.take():
                return
            stats["requests"] += 1
            try:
                info = await fetch_hashtag_info(client, pacer, api_key, str(kw["keyword"]).lstrip("#"))
            except OmarStop:
                raise
            except Exception as exc:  # noqa: BLE001 - thử lại sau một ngày (lùi info_at), không phải mỗi lượt
                logger.warning("omar_hashtag_info_failed", hashtag=kw["keyword"], error=str(exc)[:200])
                retry_at = datetime.now(tz=UTC) - timedelta(days=settings.omar_hashtag_info_days - 1)
                await save(kw["id"], {**(state.get(kw["id"]) or {}), "info_at": retry_at.isoformat()})
                return
            await save(kw["id"], {**(state.get(kw["id"]) or {}), **info, "info_at": datetime.now(tz=UTC).isoformat()})

        priority = await _movie_priority()

        missing_before = (now - timedelta(hours=settings.omar_missing_recheck_hours)).isoformat()
        recrawl_gap_before = (now - timedelta(hours=settings.omar_recrawl_min_hours)).isoformat()

        def missing(kw: dict[str, Any]) -> bool:
            return ((state.get(kw["id"]) or {}).get("missing_at") or "") >= missing_before

        def todo(kw: dict[str, Any]) -> bool:
            entry = state.get(kw["id"]) or {}
            return not missing(kw) and (not entry.get("done_at") or entry["done_at"] < recrawl_before)

        # Hashtag chưa cào trang nào trước (hashtag mới thêm), rồi phim gần ngày chiếu trước.
        deep = sorted(
            (kw for kw in keywords if todo(kw)),
            key=lambda kw: (
                int((state.get(kw["id"]) or {}).get("pages") or 0) > 0,
                priority.get(kw["movie_id"], 365.0),
                kw["id"],
            ),
        )
        fresh = sorted(
            (
                kw
                for kw in keywords
                if not todo(kw)
                and not missing(kw)
                and ((state.get(kw["id"]) or {}).get("done_at") or "") < recrawl_gap_before
            ),
            key=lambda kw: (state.get(kw["id"]) or {}).get("done_at") or "",
        )

        async def fetch(
            kw: dict[str, Any], cursor: str, hashtag_id: str | None = None
        ) -> tuple[list[dict[str, Any]], str | None, str | None] | None:
            try:
                videos, next_cursor, hashtag_id = await _fetch_page(
                    client, pacer, api_key, str(kw["keyword"]).lstrip("#"), cursor, hashtag_id
                )
            except OmarStop:
                raise
            except Exception as exc:  # noqa: BLE001 - một trang lỗi: để lượt sau thử lại đúng trang đó
                stats["failed"] += 1
                logger.warning("omar_page_failed", hashtag=kw["keyword"], cursor=cursor, error=str(exc)[:200])
                return None
            stats["requests"] += 1
            await omar_yield.add("hashtag", requests=1)
            mapped = [video for video in (map_aweme(raw) for raw in videos) if video is not None]
            stats["videos"] += len(mapped)
            await _count_cooccur(str(kw["keyword"]), kw["id"], mapped)
            await publish_videos(mapped, kw["id"], stats, source="hashtag")
            return videos, next_cursor, hashtag_id

        entries: dict[str, dict[str, Any]] = {}
        failures: dict[str, int] = {}
        empty_tries: dict[str, int] = {}

        async def crawl_page(kw: dict[str, Any]) -> float | None:
            """MỘT trang của hashtag `kw` (cào tới hết bằng nhiều lần gọi xen kẽ với hashtag khác - run_round_robin). Trả về
            số giây chờ trước trang kế, hoặc None khi hashtag xong / để đó tới lượt sau / hết ngân sách. Con trỏ lưu sau mỗi
            trang nên lượt sau đi tiếp."""
            nonlocal halted
            kw_id = kw["id"]
            if kw_id in _manual_running:
                return None  # đang được cào riêng (crawl_hashtag_now)
            entry = entries.get(kw_id)
            if entry is None:
                entry = state.get(kw_id) or {}
                if entry.get("done_at"):  # đến hạn cào lại từ đầu (giữ số liệu hashtag_info)
                    entry = {k: entry[k] for k in _RESET_KEEP if k in entry}
                entry.setdefault("started_at", now.isoformat())
                entries[kw_id] = entry
            if should_stop() or not spend.take():
                return None
            cursor = entry.get("cursor") or "0"
            try:
                result = await fetch(kw, cursor, entry.get("hashtag_id"))
            except OmarStop as exc:
                if not halted:
                    halted = True
                    stats["stopped"] = str(exc)[:100]
                    _log_stop(exc)
                return None
            ever_done = bool((state.get(kw_id) or {}).get("done_at"))  # đã từng cào xong: có thật, chỉ lỗi tạm
            if result is None and cursor == "0" and not entry.get("pages") and not ever_done:
                # Trang 1 lỗi (sau các lần thử lại trong omar_get) ở hashtag chưa từng lấy được trang nào: hashtag không
                # có trên TikTok (Omar trả 424 - biến thể AI tự đặt như #ChiChiEmEm32026). Bỏ qua, kiểm tra lại sau
                # settings.omar_missing_recheck_hours giờ (phim sắp chiếu có thể có hashtag sau).
                entry["missing_at"] = datetime.now(tz=UTC).isoformat()
                stats["missing"] = int(stats.get("missing") or 0) + 1
                await save(kw_id, entry)
                return None
            if result is None:
                # Trang lỗi (sau các lần thử lại trong omar_get): thử lại đúng trang đó sau một lúc, xen các hashtag khác.
                failures[kw_id] = failures.get(kw_id, 0) + 1
                return None if failures[kw_id] >= _MAX_PAGE_FAILURES else 30.0 * failures[kw_id]
            failures[kw_id] = 0
            entry.pop("missing_at", None)
            videos, next_cursor, hashtag_id = result
            if hashtag_id:
                entry["hashtag_id"] = hashtag_id
            if not videos and cursor != "0":
                # Rỗng giữa chừng: thường chỉ bị giới hạn tạm thời - chờ vài phút rồi gọi lại đúng cursor.
                empty_tries[kw_id] = empty_tries.get(kw_id, 0) + 1
                if empty_tries[kw_id] <= _EMPTY_RETRIES:
                    stats["empty_retries"] = int(stats.get("empty_retries") or 0) + 1
                    return _EMPTY_RETRY_SECONDS * empty_tries[kw_id]
                if next_cursor is None:
                    # Vẫn rỗng: để đó (giữ cursor, lượt sau gọi lại); _MAX_STALL_ROUNDS lượt liền thì coi là đã hết.
                    entry["stalls"] = int(entry.get("stalls") or 0) + 1
                    if entry["stalls"] < _MAX_STALL_ROUNDS:
                        stats["stalled"] = int(stats.get("stalled") or 0) + 1
                        logger.info(
                            "omar_hashtag_stalled", hashtag=kw["keyword"], cursor=cursor, stalls=entry["stalls"]
                        )
                        await save(kw_id, entry)
                        return None
            empty_tries[kw_id] = 0
            entry["pages"] = int(entry.get("pages") or 0) + 1
            if videos:
                entry.pop("stalls", None)
            empty = 0 if videos else int(entry.get("empty") or 0) + 1
            if next_cursor is None or empty >= _MAX_EMPTY_PAGES:
                entry.update(cursor=None, empty=0, stalls=0, done_at=datetime.now(tz=UTC).isoformat(), fresh_at=None)
                stats["finished"] += 1
                logger.info("omar_hashtag_finished", hashtag=kw["keyword"], pages=entry["pages"])
                await save(kw_id, entry)
                return None
            entry.update(cursor=next_cursor, empty=empty)
            await save(kw_id, entry)
            return _KEYWORD_GAP_SECONDS

        async def pool(job, items: list[tuple[dict[str, Any], int]]) -> list[dict[str, Any]]:
            """Chạy `items` (hashtag, số trang) trên tối đa settings.omar_concurrency worker song song - mỗi hashtag chỉ
            nằm ở một worker. Trả về các hashtag còn trang."""
            nonlocal halted
            queue: asyncio.Queue[tuple[dict[str, Any], int]] = asyncio.Queue()
            for item in items:
                queue.put_nowait(item)
            still: list[dict[str, Any]] = []

            async def worker() -> None:
                nonlocal halted
                while not queue.empty() and not should_stop() and not spend.exhausted:
                    kw, pages = queue.get_nowait()
                    try:
                        if await job(kw, pages):
                            still.append(kw)
                    except OmarStop as exc:
                        if not halted:
                            halted = True
                            stats["stopped"] = str(exc)[:100]
                            _log_stop(exc)

            await asyncio.gather(*(worker() for _ in range(max(1, min(settings.omar_concurrency, len(items))))))
            return still

        # settings.omar_concurrency worker xoay vòng giữa các hashtag, mỗi lần một trang (run_round_robin) - không gọi dồn một
        # hashtag; hashtag chưa bắt đầu đứng đầu hàng nên hashtag mới thêm được cào ngay.
        stats["keywords"] = len(deep)
        stats["concurrency"] = settings.omar_concurrency
        await run_round_robin(
            deep, crawl_page, workers=settings.omar_concurrency, should_stop=lambda: should_stop() or spend.exhausted
        )
        await search_task
        await channels_task
        if not spend.exhausted and not should_stop():
            await run_comments()
        # Còn ngân sách: cào lại TỪ ĐẦU các hashtag đã xong lâu nhất (crawl tự đặt lại con trỏ khi có done_at) - bắt video
        # mới và cập nhật tương tác thay vì để hạn mức ngày bỏ phí.
        stats["recrawl_keywords"] = len(fresh)
        if not spend.exhausted and not should_stop():
            entries.clear()  # cào lại từ đầu: crawl_page tự đặt lại con trỏ khi có done_at
            await run_round_robin(
                fresh,
                crawl_page,
                workers=settings.omar_concurrency,
                should_stop=lambda: should_stop() or spend.exhausted,
            )
        if stale_info and not should_stop():

            async def info_job(kw: dict[str, Any], _pages: int) -> bool:
                await refresh_info(kw)
                return False

            await pool(info_job, [(kw, 1) for kw in stale_info])

    pending = sum(1 for kw in keywords if todo(kw))
    stats["pending_keywords"] = pending
    if not _stop_requested:
        try:
            await _pause_after_round(stats, api_key, reset_at, spend.exhausted)
        except Exception as exc:  # noqa: BLE001 - không ghi được thì lượt sau tự kiểm tra hạn mức như trước
            logger.warning("omar_pause_save_failed", error=str(exc)[:200])
    if _stop_requested:
        stats["stopped"] = "manual_stop"
    logger.info("omar_round_finished", total_keywords=len(keywords), **stats)
    return {**stats, "pending_keywords": pending}
