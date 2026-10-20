"""Follower của kênh TikTok/Threads cho trang "Xuất thông tin theo kênh" (cinemark, file Social Booking) - từ 20/10/2026.

Kênh cần hỏi = đúng các kênh trang xuất hiển thị với mặc định "Số bài tối thiểu" = CHANNEL_MIN_POSTS: kênh có từ
CHANNEL_MIN_POSTS bài liên quan cho CÙNG một phim trong LOOKBACK_DAYS ngày, chưa có follower hoặc số đã cũ hơn
REFRESH_DAYS ngày, không phải follower nhập tay (followers_source='manual'). Ghi bảng D1 channel_profiles (cùng bảng
Worker ghi tệp khán giả - api/lib/channelNiches.ts của cinemark).

- TikTok: bài lấy qua Omar không có follower (raw_json.author_followers luôn trống), bài spider chỉ có ở ~14% bài - nên
  hỏi Omar GET /profile?username= (1 request mỗi kênh, tối đa MAX_PER_RUN mỗi lượt).
- Threads: qua session Threads của spider-hub. Bên này SADD handle vào THREADS_QUEUE_KEY rồi xếp type=profiles lên
  crawl_requests (publish_threads_profiles_request); worker spider-hub (spiders/threads/features/profiles/) hỏi từng
  handle và ghi kết quả vào hash THREADS_RESULTS_KEY; collect_threads_results gom hash đó ở mỗi vòng scheduler.
  Tài khoản Threads hiếm và dễ cháy nên tối đa THREADS_MAX_PER_RUN kênh mỗi lượt.

Scheduler chạy mỗi settings.channel_profiles_interval_hours giờ (app/services/scheduler.py:_channel_profiles_tick)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.clients.kafka import publish_threads_profiles_request
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services.d1 import d1_query
from app.services.platform_config_db import get_omar_api_settings

logger = get_logger(__name__)

LOOKBACK_DAYS = 180
# Trùng mặc định "Số bài tối thiểu" của trang xuất (ChannelExportPage.tsx của cinemark) - đếm theo từng phim.
CHANNEL_MIN_POSTS = 2
REFRESH_DAYS = 7
MAX_PER_RUN = 1000
CONCURRENCY = 4
THREADS_MAX_PER_RUN = 200
# Mỗi lần xếp hàng hằng ngày lấy tối đa chừng này kênh (cả tồn đọng) - mẻ THREADS_MAX_PER_RUN chạy dần trong ngày.
THREADS_FILL_LIMIT = 5000

# Trùng PROFILE_QUEUE_REDIS_KEY/PROFILE_RESULTS_REDIS_KEY ở constants/threads.py của spider-hub (RedisCache bên đó tự thêm
# prefix social_crawler:).
THREADS_QUEUE_KEY = f"{REDIS_KEY_PREFIX}threads:profile_queue"
THREADS_RESULTS_KEY = f"{REDIS_KEY_PREFIX}threads:profile_results"

# Giống CHANNEL_PROFILES_SQL ở api/lib/channelNiches.ts (cinemark) - bên nào chạy trước thì tạo bảng.
TABLE_SQL = """CREATE TABLE IF NOT EXISTS channel_profiles (
  platform TEXT NOT NULL,
  handle TEXT NOT NULL,
  name TEXT,
  followers INTEGER,
  followers_at TEXT,
  niche TEXT,
  niche_confidence INTEGER,
  niche_reason TEXT,
  niche_source TEXT,
  niche_at TEXT,
  custom_name TEXT,
  custom_url TEXT,
  followers_source TEXT,
  PRIMARY KEY (platform, handle)
)"""
# Cột thêm sau (bảng tạo trước 20/10/2026 chưa có) - follower nhập tay ở trang xuất có followers_source = 'manual', không ghi đè.
_EXTRA_COLUMNS = ("custom_name TEXT", "custom_url TEXT", "followers_source TEXT")


async def _ensure_table() -> None:
    await d1_query(TABLE_SQL)
    have = {str(row.get("name")) for row in (await d1_query("PRAGMA table_info(channel_profiles)") or [])}
    for column in _EXTRA_COLUMNS:
        if column.split(" ")[0] not in have:
            await d1_query(f"ALTER TABLE channel_profiles ADD COLUMN {column}")

# Handle trong link bài: https://www.tiktok.com/@<handle>/video/..., https://www.threads.com/@<handle>/post/...
_HANDLE_SQL = "lower(substr(p.url, instr(p.url, '@') + 1, instr(substr(p.url, instr(p.url, '@') + 1) || '/', '/') - 1))"

_running = False
_threads_running = False
_table_ready = False


def is_running() -> bool:
    return _running


def is_threads_running() -> bool:
    return _threads_running


async def _ensure_table_once() -> None:
    global _table_ready
    if not _table_ready:
        await _ensure_table()
        _table_ready = True


async def _candidates(platform: str, limit: int) -> list[dict[str, Any]]:
    """Kênh `platform` cần hỏi follower (xem docstring module), nhiều bài nhất trước."""
    since = (datetime.now(tz=UTC) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    fresh_after = (datetime.now(tz=UTC) - timedelta(days=REFRESH_DAYS)).isoformat()
    rows = await d1_query(
        f"""
        SELECT c.handle, SUM(c.posts) AS posts FROM (
          SELECT p.movie_id, {_HANDLE_SQL} AS handle, COUNT(*) AS posts
          FROM posts p
          WHERE p.platform = ?1 AND p.posted_at >= ?2 AND p.movie_id IS NOT NULL AND instr(p.url, '@') > 0
            AND {RELEVANT_POST_SQL}
          GROUP BY p.movie_id, handle HAVING COUNT(*) >= ?3
        ) c
        LEFT JOIN channel_profiles cp ON cp.platform = ?1 AND cp.handle = c.handle
        WHERE c.handle != '' AND COALESCE(cp.followers_source, '') != 'manual' AND (cp.followers_at IS NULL OR cp.followers_at < ?4)
        GROUP BY c.handle
        ORDER BY posts DESC LIMIT ?5
        """,
        [platform, since, CHANNEL_MIN_POSTS, fresh_after, limit],
        timeout=60.0,
    )
    return rows or []


async def _upsert(platform: str, rows: list[dict[str, Any]], now: str) -> None:
    """rows: {handle, name, followers}. Không tìm thấy (followers None) vẫn ghi followers_at để không hỏi lại tới lượt
    REFRESH_DAYS sau; follower nhập tay (followers_source='manual') không bao giờ bị ghi đè."""
    for start in range(0, len(rows), 200):
        await d1_query(
            """INSERT INTO channel_profiles (platform, handle, name, followers, followers_at)
            SELECT ?3, value ->> 'handle', value ->> 'name', value ->> 'followers', ?2 FROM json_each(?1) WHERE true
            ON CONFLICT(platform, handle) DO UPDATE SET
              followers = COALESCE(excluded.followers, channel_profiles.followers),
              name = COALESCE(excluded.name, channel_profiles.name), followers_at = excluded.followers_at
            WHERE COALESCE(channel_profiles.followers_source, '') != 'manual'""",
            [json.dumps(rows[start : start + 200], ensure_ascii=False), now, platform],
            timeout=60.0,
        )


async def _fetch_profile(client: httpx.AsyncClient, api_key: str, handle: str) -> dict[str, Any] | None:
    base = settings.omar_api_base_url.rstrip("/")
    for attempt in range(3):
        resp = await client.get(f"{base}/profile", params={"username": handle}, headers={"X-API-Key": api_key})
        if resp.status_code == 429:
            await asyncio.sleep(min(60, int(resp.headers.get("Retry-After") or 10)))
            continue
        if resp.status_code in (404, 424) or resp.status_code >= 500:
            if attempt < 2 and resp.status_code >= 500:
                await asyncio.sleep(3)
                continue
            return None
        resp.raise_for_status()
        data = (resp.json() or {}).get("data") or {}
        stats = data.get("stats") or {}
        user = data.get("user") or (data.get("userInfo") or {}).get("user") or {}
        followers = stats.get("followerCount")
        return {
            "followers": int(followers) if followers is not None else None,
            "name": user.get("nickname") or None,
        }
    return None


async def refresh_followers(limit: int = MAX_PER_RUN) -> dict[str, int]:
    """Một lượt: hỏi follower cho tối đa `limit` kênh TikTok, ghi channel_profiles."""
    global _running
    if _running:
        return {"skipped": 1}
    _running = True
    try:
        await _ensure_table_once()
        api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
        if not api_key:
            return {"no_key": 1}
        candidates = await _candidates("tiktok", limit)
        sem = asyncio.Semaphore(CONCURRENCY)
        rows: list[dict[str, Any]] = []
        failed = 0
        now = datetime.now(tz=UTC).isoformat()

        async def one(client: httpx.AsyncClient, handle: str) -> None:
            nonlocal failed
            async with sem:
                try:
                    profile = await _fetch_profile(client, api_key, handle)
                except httpx.HTTPError as exc:
                    failed += 1
                    logger.warning("channel_profile_failed", handle=handle, error=str(exc)[:200])
                    return
                # Không tìm thấy: vẫn ghi followers_at để không hỏi lại tới lượt REFRESH_DAYS sau.
                rows.append({"handle": handle, "followers": (profile or {}).get("followers"), "name": (profile or {}).get("name")})

        async with httpx.AsyncClient(timeout=30.0) as client:
            await asyncio.gather(*(one(client, str(row["handle"])) for row in candidates))
        await _upsert("tiktok", rows, now)
        found = sum(1 for row in rows if row["followers"] is not None)
        logger.info("channel_profiles_refreshed", candidates=len(candidates), found=found, failed=failed)
        return {"candidates": len(candidates), "found": found, "failed": failed}
    finally:
        _running = False


async def enqueue_threads_profiles(fill: bool, limit: int = THREADS_MAX_PER_RUN) -> dict[str, int]:
    """fill=True (mỗi ngày): SADD mọi kênh cần hỏi (tối đa THREADS_FILL_LIMIT, trùng thì set tự bỏ) vào hàng đợi. Sau đó
    hàng đợi còn handle thì xếp một mẻ type=profiles tối đa `limit` kênh cho spider-hub (scheduler gọi mỗi
    settings.threads_profiles_batch_hours giờ)."""
    global _threads_running
    if _threads_running:
        return {"skipped": 1}
    _threads_running = True
    try:
        await _ensure_table_once()
        redis = get_redis_client()
        pending = int(await redis.scard(THREADS_QUEUE_KEY) or 0)
        added = 0
        if fill:
            candidates = await _candidates("threads", THREADS_FILL_LIMIT)
            handles = [str(row["handle"]) for row in candidates if row.get("handle")]
            if handles:
                added = int(await redis.sadd(THREADS_QUEUE_KEY, *handles) or 0)
        queued = pending + added
        published = await publish_threads_profiles_request(limit) if queued else False
        logger.info("threads_profiles_enqueued", pending=pending, added=added, published=published)
        return {"pending": pending, "added": added, "published": int(published)}
    except Exception as exc:  # noqa: BLE001 - chạy nền từ scheduler; chu kỳ sau thử lại
        logger.error("threads_profiles_enqueue_failed", error=str(exc)[:300])
        return {"failed": 1}
    finally:
        _threads_running = False


async def collect_threads_results() -> dict[str, int]:
    """Gom kết quả spider-hub đã ghi vào THREADS_RESULTS_KEY, ghi D1 channel_profiles (platform='threads') rồi xoá
    đúng những trường đã ghi. Rẻ khi hash trống (một HGETALL) - scheduler gọi mỗi vòng."""
    redis = get_redis_client()
    raw = await redis.hgetall(THREADS_RESULTS_KEY)
    if not raw:
        return {"collected": 0}
    rows: list[dict[str, Any]] = []
    for handle, value in raw.items():
        try:
            result = json.loads(value)
        except ValueError:
            result = {}
        followers = result.get("follower_count") if result.get("found") else None
        rows.append(
            {
                "handle": str(handle).lower(),
                "followers": int(followers) if isinstance(followers, (int, float)) else None,
                "name": (result.get("full_name") or None) if result.get("found") else None,
            }
        )
    await _ensure_table_once()
    await _upsert("threads", rows, datetime.now(tz=UTC).isoformat())
    await redis.hdel(THREADS_RESULTS_KEY, *raw.keys())
    found = sum(1 for row in rows if row["followers"] is not None)
    logger.info("threads_profiles_collected", collected=len(rows), found=found)
    return {"collected": len(rows), "found": found}
