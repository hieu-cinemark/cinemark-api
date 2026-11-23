"""Cào lại MỖI NGÀY các bài booking đang theo dõi (bảng tracked_posts của D1 - trang "Theo dõi bài booking" của cinemark
nhập từ link Google Sheet, xem cinemark api/lib/trackedPosts.ts), ở làn ưu tiên.

- TikTok: Omar video/detail từng video (số liệu hiện tại), gửi thẳng lên raw_posts với source="tracked" - không qua tập
  seen chống trùng của Omar (bài đã có vẫn phải cập nhật số).
- Facebook: bài đã có trong D1 (đã khớp external_id) -> MỘT job post_metrics vào hàng đợi rảnh của spider-hub (Redis
  crawl_idle:facebook - chạy khi spider-hub không có việc khác hoặc trong giờ yên lặng Meta, vì job này không dùng tài
  khoản): đọc từng trang bài KHÔNG đăng nhập, chỉ cập nhật like/comment/share (tắt bằng
  TRACKED_FACEBOOK_LOGGED_OUT=false). Job mang sẵn job page_feed dự phòng của từng page - bài nào spider-hub không lấy
  được số (IP bị chặn...) thì nó đẩy page_feed của page đó vào làn ưu tiên. Bài chưa có trong D1 (cần tạo mới, mà trang
  không đăng nhập chỉ có số) đi thẳng page_feed vào làn ưu tiên (Redis crawl_priority:facebook - spider-hub xả làn này
  trước mọi message Kafka) như trước: gom theo PAGE, lùi tới bài cũ nhất đang theo dõi của page, không chống trùng. Page
  lấy từ link bài (facebook.com/<page>/posts/...), link bài đã khớp, hoặc dòng khác cùng tên page.
- Threads / group / trang cá nhân / YouTube: chưa cào (nhập số tay trên trang).

Ingest ép gán bài khớp danh sách theo dõi vào phim (tracked_keyword_for) - bỏ cổng tên phim/liên quan, vì người dùng đã
xác nhận đây là bài booking của phim. Theo dõi tới TRACK_DAYS_AFTER_RELEASE ngày sau công chiếu hoặc tới khi bấm dừng.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import Column, Integer, Table, Text, select, update

from app.clients import crawl_queues as queues
from app.clients.kafka import publish_raw_post
from app.clients.omar import OmarStop, map_video, omar_get
from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.keywords import get_or_create_keyword
from app.repositories.d1.sql import d1_exec, json_values, metadata
from app.repositories.d1.tables import movies, posts
from app.services.accounts.config_db.omar import get_omar_api_settings
from app.services.omar.omar_tiktok import _Pacer

logger = get_logger(__name__)

# Bài theo dõi nhập từ sheet booking (cinemark tạo bảng).
tracked = Table(
    "tracked_posts",
    metadata,
    Column("id", Text, primary_key=True),
    *(Column(name, Text) for name in ("movie_id", "sheet_url", "platform", "url", "url_key", "page", "posted_date")),
    *(Column(name, Text) for name in ("phase", "grp")),
    *(Column(name, Integer) for name in ("sheet_reacts", "sheet_comments", "sheet_shares", "sheet_views", "crawlable")),
    *(Column(name, Text) for name in ("external_id", "match_kind", "status", "created_at", "last_queued_at")),
)

TRACK_DAYS_AFTER_RELEASE = 30
IDLE_QUEUE_KEY = queues.IDLE_FACEBOOK
_NOT_A_PAGE = {
    "photo",
    "photo.php",
    "reel",
    "reels",
    "watch",
    "groups",
    "share",
    "story.php",
    "permalink.php",
    "people",
    "videos",
}

_running = False
_VN = ZoneInfo("Asia/Ho_Chi_Minh")


def _today() -> date:
    return datetime.now(_VN).date()


def is_running() -> bool:
    return _running


async def _active_rows() -> list[dict[str, Any]]:
    try:
        rows = await d1_exec(
            select(tracked, movies.c.title.label("movie_title"), movies.c.released_at)
            .select_from(tracked.join(movies, movies.c.id == tracked.c.movie_id))
            .where(tracked.c.status == "active"),
            quiet=True,
        )
    except Exception:  # noqa: BLE001 - bảng chưa có (chưa ai nhập sheet)
        return []
    today = _today()
    out = []
    for row in rows or []:
        released = str(row.get("released_at") or "")[:10]
        try:
            if released and date.fromisoformat(released) + timedelta(days=TRACK_DAYS_AFTER_RELEASE) < today:
                continue
        except ValueError:
            pass  # ngày phát hành sai định dạng: vẫn theo dõi bài
        out.append(row)
    return out


def _tiktok_id(url: str) -> str | None:
    match = re.search(r"/(?:video|photo)/(\d+)", url)
    return match.group(1) if match else None


def _fb_page_slug(url: str | None) -> str | None:
    if not url or "facebook.com" not in url:
        return None
    path = urlparse(url).path.strip("/").split("/")
    if not path or not path[0] or path[0].lower() in _NOT_A_PAGE or path[0].endswith(".php"):
        return None
    return path[0]


async def _keyword_id(movie_id: str, platform: str, title: str) -> str | None:
    keyword = await get_or_create_keyword(movie_id, platform, title)
    return keyword["id"] if keyword else None


async def _refresh_tiktok(rows: list[dict[str, Any]], stats: dict[str, Any]) -> None:
    api_key = (((await get_omar_api_settings()) or {}).get("api_key") or "").strip()
    if not api_key:
        stats["tiktok_skipped"] = "no_api_key"
        return
    pacer = _Pacer(settings.omar_manual_requests_per_minute)
    keywords: dict[str, str | None] = {}
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        for row in rows:
            video_id = _tiktok_id(row["url"])
            if not video_id:
                continue
            movie_id = row["movie_id"]
            if movie_id not in keywords:
                keywords[movie_id] = await _keyword_id(movie_id, "tiktok", row["movie_title"])
            try:
                data = await omar_get(client, pacer.wait, api_key, "video_info", {"video": video_id})
            except OmarStop as exc:
                stats["tiktok_stopped"] = str(exc)[:120]
                return
            except Exception as exc:  # noqa: BLE001 - một video lỗi: hôm sau thử lại
                logger.warning("tracked_tiktok_failed", video_id=video_id, error=exc)
                continue
            mapped = map_video(data) if isinstance(data, dict) else None
            if not mapped:
                stats["tiktok_missing"] = stats.get("tiktok_missing", 0) + 1
                continue
            ok = await publish_raw_post(
                key=f"tiktok:{mapped['video_id']}",
                value={"platform": "tiktok", "keyword_id": keywords[movie_id], "source": "tracked", **mapped},
            )
            stats["tiktok_sent"] = stats.get("tiktok_sent", 0) + int(ok)


async def _queue_facebook(rows: list[dict[str, Any]], stats: dict[str, Any]) -> set[str]:
    """Bài đã có trong D1 -> một job post_metrics (không đăng nhập). Bài chưa có trong D1 thì chưa xếp được (trang không
    đăng nhập chỉ có số, không tạo được bài; đường page_feed có tài khoản đã bỏ 17/11/2026): trả về id các bài đó để
    không ghi last_queued_at cho chúng."""
    matched_urls: dict[str, str] = {}
    ids = [row["external_id"] for row in rows if row.get("external_id")]
    if ids:
        found = await d1_exec(
            select(posts.c.external_id, posts.c.url).where(
                posts.c.platform == "facebook", posts.c.external_id.in_(json_values(ids))
            ),
            quiet=True,
        )
        matched_urls.update({r["external_id"]: r["url"] for r in found or []})
    slug_by_name: dict[str, str] = {}
    slugs: list[str | None] = []
    for row in rows:
        slug = _fb_page_slug(row["url"]) or _fb_page_slug(matched_urls.get(row.get("external_id") or ""))
        slugs.append(slug)
        if slug and row.get("page"):
            slug_by_name.setdefault(row["page"].strip().lower(), slug)
    metrics_posts: list[dict[str, Any]] = []
    deferred: set[str] = set()
    for row, slug in zip(rows, slugs, strict=True):
        slug = slug or slug_by_name.get((row.get("page") or "").strip().lower())
        external_id = row.get("external_id") or ""
        known_url = matched_urls.get(external_id)
        if known_url:
            # Link đã lưu trong D1 (dạng chuẩn spider-hub tạo ra), không dùng link sheet (share/..., m.facebook...).
            metrics_posts.append(
                {
                    "external_id": external_id,
                    "url": known_url,
                    "page_url": f"https://www.facebook.com/{slug}" if slug else None,
                }
            )
        else:
            deferred.add(row.get("id"))
        if not slug:
            if not known_url:
                stats["facebook_no_page"] = stats.get("facebook_no_page", 0) + 1
            continue
    redis = get_redis_client()
    if metrics_posts:
        request = {
            "platform": "facebook",
            "type": "post_metrics",
            "posts": metrics_posts,
            "run_id": str(uuid.uuid4()),
            "bypass_drain": True,
            "published_at": time.time(),
            "priority": "tracked",
        }
        await redis.lpush(IDLE_QUEUE_KEY, json.dumps(request, ensure_ascii=False))
        stats["facebook_metrics_posts"] = len(metrics_posts)
    if deferred:
        stats["facebook_deferred"] = len(deferred)
    return deferred


async def run_daily() -> dict[str, Any]:
    """Một lượt cào lại mọi bài đang theo dõi (gọi mỗi ngày từ scheduler)."""
    global _running
    _running = True
    stats: dict[str, Any] = {}
    try:
        rows = [row for row in await _active_rows() if int(row.get("crawlable") or 0)]
        by_platform: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_platform[row["platform"]].append(row)
        stats["rows"] = {platform: len(items) for platform, items in by_platform.items()}
        deferred: set[str] = set()
        if by_platform.get("facebook"):
            deferred = await _queue_facebook(by_platform["facebook"], stats)
        if by_platform.get("tiktok"):
            await _refresh_tiktok(by_platform["tiktok"], stats)
        queued_at = datetime.now(tz=UTC).isoformat(timespec="seconds")
        if rows and not deferred:
            await d1_exec(
                update(tracked).where(tracked.c.status == "active").values(last_queued_at=queued_at), quiet=True
            )
        elif rows:
            queued_ids = [row["id"] for row in rows if row["id"] not in deferred]
            await d1_exec(
                update(tracked).where(tracked.c.id.in_(json_values(queued_ids))).values(last_queued_at=queued_at),
                quiet=True,
            )
        logger.info("tracked_posts_refreshed", **stats)
        return stats
    finally:
        _running = False


# --- Ingest: ép gán bài theo dõi vào phim ---------------------------------------------------------------------------------

_TOKEN_TTL_SECONDS = 300
_token_cache: tuple[float, dict[str, tuple[str, str]]] | None = None


def _link_tokens(text: str) -> set[str]:
    album = set(re.findall(r"set=a\.(\d+)", text))
    tokens = set(re.findall(r"pfbid[0-9A-Za-z]+", text)) | set(re.findall(r"(?<!\d)\d{9,}(?!\d)", text))
    return tokens - album


async def _tracked_tokens() -> dict[str, tuple[str, str]]:
    """mã trong link / mã bài đã khớp -> (movie_id, tên phim) của các bài đang theo dõi; cache vài phút."""
    global _token_cache
    now = time.monotonic()
    if _token_cache and now - _token_cache[0] < _TOKEN_TTL_SECONDS:
        return _token_cache[1]
    tokens: dict[str, tuple[str, str]] = {}
    for row in await _active_rows():
        for token in _link_tokens(f"{row['url']} {row.get('external_id') or ''}"):
            tokens[token] = (row["movie_id"], row["movie_title"])
    _token_cache = (now, tokens)
    return tokens


async def tracked_keyword_for(platform: str, payload: dict[str, Any]) -> str | None:
    """keyword_id của phim khi bài này là một bài booking đang theo dõi (khớp mã bài / mã trong link), không thì None."""
    tokens = await _tracked_tokens()
    if not tokens:
        return None
    text = " ".join(str(payload.get(k) or "") for k in ("post_id", "video_id", "url", "media_url"))
    for token in _link_tokens(text):
        if token in tokens:
            movie_id, title = tokens[token]
            return await _keyword_id(movie_id, platform, title)
    return None
