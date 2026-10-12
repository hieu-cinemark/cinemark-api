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
from app.services import omar_comments
from app.services.d1 import OMAR_COOCCUR_KEY_TMPL, d1_query, get_enabled_keywords
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
    raise RuntimeError(f"omar_retries_exhausted path={path} status={resp.status_code}")


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


async def _claim_unseen(video_ids: list[str]) -> set[str]:
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


# Trạng thái lượt đang/vừa chạy - cho tab Omar trên dashboard (app/api/routes/omar.py) và scheduler (không mở lượt chồng).
_round: dict[str, Any] = {"running": False, "trigger": None, "started_at": None, "finished_at": None, "stats": {}}
_stop_requested = False


def round_status() -> dict[str, Any]:
    return {**_round, "stop_requested": _stop_requested}


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
        if day.get("remaining") is not None:
            remaining, resets_in = int(day["remaining"]), int(day.get("resets_in") or 0)
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
            return {**stats, "skipped": "no_budget"}
        pacer = _Pacer(per_minute)
        spend = Budget(budget)

        def should_stop() -> bool:
            return _stop_requested or halted

        halted = False
        # Bình luận tuần (app/services/omar_comments.py) chạy SONG SONG với phần hashtag, tối đa
        # settings.omar_comments_budget_share ngân sách. Trước 12/10/2026 nó chạy trước và hàng đợi tuần (hàng chục nghìn
        # request, vài giờ) chặn mọi hashtag - hashtag mới đứng "Chưa bắt đầu" cả buổi.
        comment_budget = Budget(int(budget * settings.omar_comments_budget_share), parent=spend)

        async def run_comments() -> None:
            nonlocal halted
            try:
                await omar_comments.process_queue(client, pacer, api_key, comment_budget, stats, should_stop)
            except OmarStop as exc:
                if not halted:
                    halted = True
                    stats["stopped"] = str(exc)[:100]
                    logger.error("omar_round_stopped", error=str(exc), telegram=True)
            except Exception as exc:  # noqa: BLE001 - phần bình luận lỗi không chặn phần hashtag
                logger.error("omar_comments_failed", error=str(exc)[:300])

        comments_task = asyncio.create_task(run_comments())
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
            (kw for kw in keywords if not todo(kw) and not missing(kw)),
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
            mapped = [video for video in (map_aweme(raw) for raw in videos) if video is not None]
            stats["videos"] += len(mapped)
            await _count_cooccur(str(kw["keyword"]), kw["id"], mapped)
            fresh_ids = await _claim_unseen([video["video_id"] for video in mapped])
            for video in mapped:
                if video["video_id"] not in fresh_ids:
                    stats["skipped_seen"] += 1
                    continue
                ok = await publish_raw_post(
                    key=f"tiktok:{video['video_id']}", value={"platform": "tiktok", "keyword_id": kw["id"], **video}
                )
                stats["published"] += int(ok)
                if not ok:
                    await _forget_seen(video["video_id"])  # chưa tới Kafka: lần sau gửi lại
            return videos, next_cursor, hashtag_id

        async def crawl(kw: dict[str, Any], _pages: int) -> bool:
            """Cào hashtag TỚI HẾT video (has_more = false) - tuần tự, trang sau cần cursor của trang trước. Dừng sớm chỉ
            khi hết ngân sách / bị dừng / một trang lỗi _MAX_PAGE_FAILURES lần liền; con trỏ đã lưu nên lượt sau đi tiếp."""
            entry = state.get(kw["id"]) or {}
            if entry.get("done_at"):  # đến hạn cào lại từ đầu (giữ số liệu hashtag_info)
                entry = {k: entry[k] for k in ("video_count", "view_count", "info_at", "hashtag_id") if k in entry}
            entry.setdefault("started_at", now.isoformat())
            cursor = entry.get("cursor") or "0"
            empty = int(entry.get("empty") or 0)
            failed = 0
            while True:
                if should_stop() or not spend.take():
                    return True
                result = await fetch(kw, cursor, entry.get("hashtag_id"))
                ever_done = bool((state.get(kw["id"]) or {}).get("done_at"))  # đã từng cào xong: có thật, chỉ lỗi tạm
                if result is None and cursor == "0" and not entry.get("pages") and not ever_done:
                    # Trang 1 lỗi (sau các lần thử lại trong omar_get) ở hashtag chưa từng lấy được trang nào: hashtag không
                    # có trên TikTok (Omar trả 424 - biến thể AI tự đặt như #ChiChiEmEm32026). Bỏ qua, kiểm tra lại sau
                    # settings.omar_missing_recheck_hours giờ (phim sắp chiếu có thể có hashtag sau).
                    entry["missing_at"] = datetime.now(tz=UTC).isoformat()
                    stats["missing"] = int(stats.get("missing") or 0) + 1
                    await save(kw["id"], entry)
                    return False
                if result is None:
                    # Trang lỗi (sau các lần thử lại trong omar_get): đợi rồi thử lại đúng trang đó. Trước 12/10/2026 lỗi một
                    # trang là bỏ hashtag tới hết lượt - lượt kết thúc khi còn hơn nửa hạn mức ngày.
                    failed += 1
                    if failed >= _MAX_PAGE_FAILURES:
                        return True
                    await asyncio.sleep(30 * failed)
                    continue
                failed = 0
                entry.pop("missing_at", None)
                videos, next_cursor, hashtag_id = result
                if hashtag_id:
                    entry["hashtag_id"] = hashtag_id
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
                            logger.error("omar_round_stopped", error=str(exc), telegram=True)

            await asyncio.gather(*(worker() for _ in range(max(1, min(settings.omar_concurrency, len(items))))))
            return still

        # Mỗi worker nhận một hashtag và cào tới hết rồi mới nhận hashtag kế (settings.omar_concurrency hashtag song song);
        # hashtag chưa bắt đầu đứng đầu hàng nên hashtag mới thêm được cào ngay.
        stats["keywords"] = len(deep)
        stats["concurrency"] = settings.omar_concurrency
        await pool(crawl, [(kw, 0) for kw in deep])
        # Còn ngân sách: cào lại TỪ ĐẦU các hashtag đã xong lâu nhất (crawl tự đặt lại con trỏ khi có done_at) - bắt video
        # mới và cập nhật tương tác thay vì để hạn mức ngày bỏ phí.
        stats["recrawl_keywords"] = len(fresh)
        if not spend.exhausted and not should_stop():
            await pool(crawl, [(kw, 0) for kw in fresh])
        if stale_info and not should_stop():

            async def info_job(kw: dict[str, Any], _pages: int) -> bool:
                await refresh_info(kw)
                return False

            await pool(info_job, [(kw, 1) for kw in stale_info])
        await comments_task

    pending = sum(1 for kw in keywords if todo(kw))
    stats["pending_keywords"] = pending
    if _stop_requested:
        stats["stopped"] = "manual_stop"
    logger.info("omar_round_finished", total_keywords=len(keywords), **stats)
    return {**stats, "pending_keywords": pending}
