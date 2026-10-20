"""Follower của kênh TikTok cho trang "Xuất thông tin theo kênh" (cinemark, file Social Booking) - từ 20/10/2026.

Bài lấy qua Omar không có follower (raw_json.author_followers luôn trống), bài spider chỉ có ở ~14% bài - nên kênh đang
hoạt động (từ CHANNEL_MIN_POSTS bài liên quan trong LOOKBACK_DAYS ngày) mà chưa có follower hoặc số đã cũ hơn
REFRESH_DAYS ngày được hỏi Omar GET /profile?username= (1 request mỗi kênh, tối đa MAX_PER_RUN mỗi lượt; scheduler chạy mỗi
settings.channel_profiles_interval_hours giờ). Ghi bảng D1 channel_profiles (cùng bảng Worker ghi tệp khán giả -
api/lib/channelNiches.ts của cinemark)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services.d1 import d1_query
from app.services.platform_config_db import get_omar_api_settings

logger = get_logger(__name__)

LOOKBACK_DAYS = 30
CHANNEL_MIN_POSTS = 2
REFRESH_DAYS = 7
MAX_PER_RUN = 1000
CONCURRENCY = 4

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
  PRIMARY KEY (platform, handle)
)"""

# Handle TikTok trong link bài: https://www.tiktok.com/@<handle>/video/...
_HANDLE_SQL = "lower(substr(p.url, instr(p.url, '@') + 1, instr(substr(p.url, instr(p.url, '@') + 1) || '/', '/') - 1))"

_running = False


def is_running() -> bool:
    return _running


async def _candidates(limit: int) -> list[dict[str, Any]]:
    since = (datetime.now(tz=UTC) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    fresh_after = (datetime.now(tz=UTC) - timedelta(days=REFRESH_DAYS)).isoformat()
    rows = await d1_query(
        f"""
        SELECT c.handle, c.posts FROM (
          SELECT {_HANDLE_SQL} AS handle, COUNT(*) AS posts
          FROM posts p
          WHERE p.platform = 'tiktok' AND p.posted_at >= ?1 AND instr(p.url, '@') > 0 AND {RELEVANT_POST_SQL}
          GROUP BY 1 HAVING COUNT(*) >= ?2
        ) c
        LEFT JOIN channel_profiles cp ON cp.platform = 'tiktok' AND cp.handle = c.handle
        WHERE c.handle != '' AND (cp.followers_at IS NULL OR cp.followers_at < ?3)
        ORDER BY c.posts DESC LIMIT ?4
        """,
        [since, CHANNEL_MIN_POSTS, fresh_after, limit],
        timeout=60.0,
    )
    return rows or []


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
        await d1_query(TABLE_SQL)
        api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
        if not api_key:
            return {"no_key": 1}
        candidates = await _candidates(limit)
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
        for start in range(0, len(rows), 200):
            await d1_query(
                """INSERT INTO channel_profiles (platform, handle, name, followers, followers_at)
                SELECT 'tiktok', value ->> 'handle', value ->> 'name', value ->> 'followers', ?2 FROM json_each(?1) WHERE true
                ON CONFLICT(platform, handle) DO UPDATE SET
                  followers = COALESCE(excluded.followers, channel_profiles.followers),
                  name = COALESCE(excluded.name, channel_profiles.name), followers_at = excluded.followers_at""",
                [json.dumps(rows[start : start + 200], ensure_ascii=False), now],
                timeout=60.0,
            )
        found = sum(1 for row in rows if row["followers"] is not None)
        logger.info("channel_profiles_refreshed", candidates=len(candidates), found=found, failed=failed)
        return {"candidates": len(candidates), "found": found, "failed": failed}
    finally:
        _running = False
