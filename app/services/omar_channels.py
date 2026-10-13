"""Cào video theo KÊNH TikTok qua Omar API (/posts, từ 2026-10-13) - thay spider channel_videos của spider-hub.

Kênh lấy từ chính D1: mọi tài khoản TikTok đã có từ settings.omar_channels_min_posts bài liên quan trong
settings.omar_channels_lookback_days ngày (trang của phim, nhà phát hành, kênh review, KOL...), nhiều bài nhất trước. Mỗi
kênh mỗi ngày lật /posts (mới nhất trước) tới khi gặp video cũ hơn settings.omar_channels_days ngày hoặc hết
settings.omar_channels_max_pages trang - bắt video của các kênh này kể cả khi không gắn hashtag phim.

Kênh đăng nhiều thứ ngoài phim, nên chỉ gửi video NHẮC tới một phim đang theo dõi (hashtag trùng từ khoá hashtag của phim,
hoặc mô tả chứa tên phim) - gắn keyword_id = hashtag đầu tiên của phim đó; video khác bỏ qua (khỏi tốn ingest và Jev).
Chạy song song trong mỗi lượt Omar (omar_tiktok.run_omar_round) với settings.omar_channels_budget_share ngân sách.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import httpx

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services.d1 import d1_query
from app.services.relevance_rules import normalize_title

if TYPE_CHECKING:
    from app.services.omar_tiktok import Budget, _Pacer

logger = get_logger(__name__)

# Hash {handle: ngày (giờ VN) đã cào xong}.
DONE_KEY = f"{REDIS_KEY_PREFIX}omar:channels:done"
# Hash {handle: create_time (epoch) của video mới nhất đã thấy} - lần sau chỉ lật tới mốc này (trừ 1 ngày), không lật lại
# cả settings.omar_channels_days ngày mỗi ngày.
SEEN_UNTIL_KEY = f"{REDIS_KEY_PREFIX}omar:channels:seen_until"
_WORKERS = 4


def _squash(text: str | None) -> str:
    return normalize_title(text).replace(" ", "")


async def load_channels() -> list[str]:
    """Handle TikTok (không @) có nhiều bài liên quan nhất gần đây, lấy từ URL bài trong D1."""
    since = (datetime.now(tz=UTC) - timedelta(days=settings.omar_channels_lookback_days)).isoformat()
    rows = (
        await d1_query(
            f"""
            SELECT lower(substr(rest, 1, instr(rest || '/', '/') - 1)) AS handle, COUNT(*) AS n
            FROM (SELECT substr(p.url, instr(p.url, '/@') + 2) AS rest FROM posts p
                  WHERE p.platform = 'tiktok' AND p.posted_at >= ? AND instr(p.url, '/@') > 0 AND {RELEVANT_POST_SQL})
            GROUP BY handle HAVING n >= ? AND handle != ''
            ORDER BY n DESC LIMIT ?
            """,
            [since, settings.omar_channels_min_posts, settings.omar_channels_max],
            quiet=True,
        )
        or []
    )
    return [str(row["handle"]) for row in rows]


async def load_movie_matchers() -> list[dict[str, Any]]:
    """[{keyword_id, title_key, tags}] cho mọi phim đang bật có hashtag TikTok - để biết video kênh nói về phim nào."""
    rows = (
        await d1_query(
            """
            SELECT k.id, k.movie_id, k.keyword, m.title FROM keywords k JOIN movies m ON m.id = k.movie_id
            WHERE k.platform = 'tiktok' AND k.enabled = 1 AND m.enabled = 1 AND k.keyword LIKE '#%'
            ORDER BY k.created_at
            """,
            quiet=True,
        )
        or []
    )
    movies: dict[str, dict[str, Any]] = {}
    for row in rows:
        movie = movies.setdefault(
            row["movie_id"], {"keyword_id": row["id"], "title_key": _squash(row["title"]), "tags": set()}
        )
        movie["tags"].add(_squash(row["keyword"].lstrip("#")))
    return list(movies.values())


def match_movie(video: dict[str, Any], movies: list[dict[str, Any]]) -> str | None:
    """keyword_id của phim mà video nhắc tới (hashtag trùng hashtag của phim, hoặc mô tả chứa tên phim), hoặc None."""
    tags = {_squash(tag) for tag in video.get("hashtags") or []}
    desc = _squash(video.get("desc"))
    for movie in movies:
        if tags & movie["tags"]:
            return movie["keyword_id"]
    for movie in movies:
        if len(movie["title_key"]) >= 6 and movie["title_key"] in desc:
            return movie["keyword_id"]
    return None


async def crawl_channel(
    client: httpx.AsyncClient,
    pacer: _Pacer,
    api_key: str,
    handle: str,
    movies: list[dict[str, Any]],
    stats: dict[str, Any],
    take: Callable[[], bool],
) -> bool:
    """Lật /posts của một kênh tới mốc settings.omar_channels_days ngày. True khi cào xong (không hết ngân sách giữa chừng)."""
    from app.services.omar_tiktok import map_web_item, next_cursor, omar_get, publish_videos

    redis = get_redis_client()
    cutoff = (datetime.now(tz=UTC) - timedelta(days=settings.omar_channels_days)).timestamp()
    try:
        seen_until = float(await redis.hget(SEEN_UNTIL_KEY, handle) or 0)
    except Exception:  # noqa: BLE001
        seen_until = 0.0
    if seen_until:
        cutoff = max(cutoff, seen_until - 86_400)
    newest = 0
    cursor = "0"
    for _ in range(settings.omar_channels_max_pages):
        if not take():
            return False
        stats["requests"] = int(stats.get("requests") or 0) + 1
        stats["channel_requests"] = int(stats.get("channel_requests") or 0) + 1
        data = await omar_get(client, pacer, api_key, "posts", {"username": handle, "cursor": cursor})
        if data is None:
            return True  # kênh không tồn tại / đã đổi tên
        videos = [v for v in (map_web_item(raw) for raw in data.get("posts") or []) if v]
        stats["channel_videos"] = int(stats.get("channel_videos") or 0) + len(videos)
        by_keyword: dict[str, list[dict[str, Any]]] = {}
        for video in videos:
            if (keyword_id := match_movie(video, movies)) is not None:
                by_keyword.setdefault(keyword_id, []).append({**video, "source": "omar_channel"})
        for keyword_id, matched in by_keyword.items():
            stats["channel_matched"] = int(stats.get("channel_matched") or 0) + len(matched)
            await publish_videos(matched, keyword_id, stats)
        oldest = min((int(v.get("create_time") or 0) for v in videos), default=0)
        newest = max([newest, *(int(v.get("create_time") or 0) for v in videos)])
        if newest > seen_until:
            await redis.hset(SEEN_UNTIL_KEY, handle, str(newest))
        following = next_cursor(data)
        if following is None or (oldest and oldest < cutoff):
            return True
        cursor = following
    return True


async def run_channels(
    client: httpx.AsyncClient,
    pacer: _Pacer,
    api_key: str,
    budget: Budget,
    stats: dict[str, Any],
    should_stop: Callable[[], bool],
) -> None:
    """Mỗi kênh một lần mỗi ngày, _WORKERS kênh song song, trong `budget`."""
    from app.services.omar_tiktok import OmarStop

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    redis = get_redis_client()
    done = await redis.hgetall(DONE_KEY) or {}
    channels = [handle for handle in await load_channels() if done.get(handle) != today]
    if not channels:
        return
    movies = await load_movie_matchers()
    stats["channels"] = len(channels)
    queue: asyncio.Queue[str] = asyncio.Queue()
    for handle in channels:
        queue.put_nowait(handle)
    stopped: list[BaseException] = []

    def take() -> bool:
        return not stopped and not should_stop() and budget.take()

    async def worker() -> None:
        while not queue.empty() and not stopped and not should_stop() and not budget.exhausted:
            handle = queue.get_nowait()
            try:
                if await crawl_channel(client, pacer, api_key, handle, movies, stats, take):
                    await redis.hset(DONE_KEY, handle, today)
            except OmarStop as exc:
                stopped.append(exc)
            except Exception as exc:  # noqa: BLE001 - kênh lỗi: lượt sau thử lại
                stats["failed"] = int(stats.get("failed") or 0) + 1
                logger.warning("omar_channel_failed", handle=handle, error=str(exc)[:200])

    await asyncio.gather(*(worker() for _ in range(min(_WORKERS, len(channels)))))
    logger.info(
        "omar_channels_processed",
        channels=len(channels),
        requests=stats.get("channel_requests", 0),
        videos=stats.get("channel_videos", 0),
        matched=stats.get("channel_matched", 0),
    )
    if stopped:
        raise stopped[0]


async def channel_status() -> dict[str, Any]:
    """Cho tab Omar: số kênh đã từng cào, đã cào hôm nay, và các kênh có video mới gần nhất."""
    redis = get_redis_client()
    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    done = await redis.hgetall(DONE_KEY) or {}
    seen = await redis.hgetall(SEEN_UNTIL_KEY) or {}
    latest = sorted(((handle, float(ts or 0)) for handle, ts in seen.items()), key=lambda item: -item[1])[:20]
    return {
        "tracked": len(seen),
        "done_today": sum(1 for day in done.values() if day == today),
        "max_channels": settings.omar_channels_max,
        "days": settings.omar_channels_days,
        "latest": [
            {"handle": handle, "newest_video_at": datetime.fromtimestamp(ts, tz=UTC).isoformat() if ts else None}
            for handle, ts in latest
        ],
    }


_manual_tasks: set[asyncio.Task] = set()


def crawl_channel_now(username: str, max_pages: int | None = None) -> bool:
    """Nút "Cào kênh" (POST /tiktok/channels/run): cào ngay một kênh qua Omar, độc lập với lượt đang chạy."""
    from app.services.omar_tiktok import _Pacer
    from app.services.platform_config_db import get_omar_api_settings

    handle = username.strip().lstrip("@").split("/")[0].lower()
    if not handle:
        return False

    async def run() -> None:
        api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
        if not api_key:
            logger.warning("omar_channel_manual_no_key", handle=handle)
            return
        stats: dict[str, Any] = {}
        pages = max_pages or settings.omar_channels_max_pages
        left = [pages]

        def take() -> bool:
            left[0] -= 1
            return left[0] >= 0

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                await crawl_channel(
                    client,
                    _Pacer(settings.omar_manual_requests_per_minute),
                    api_key,
                    handle,
                    await load_movie_matchers(),
                    stats,
                    take,
                )
        except Exception as exc:  # noqa: BLE001
            logger.error("omar_channel_manual_failed", handle=handle, error=str(exc)[:300])
        logger.info("omar_channel_manual_finished", handle=handle, **stats)

    task = asyncio.create_task(run())
    _manual_tasks.add(task)
    task.add_done_callback(_manual_tasks.discard)
    return True
