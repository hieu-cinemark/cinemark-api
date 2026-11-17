"""Video đang nổi - tốc độ tăng tương tác của video TikTok từng phim, đo qua ShortVid API.

Mỗi settings.shortvid_trending_interval_hours giờ (scheduler/shortvid.shortvid_trending_tick), refresh_top_videos lấy cho mỗi phim đang
bật settings.shortvid_trending_per_movie video TikTok liên quan đăng trong settings.shortvid_trending_days ngày qua, tương tác cao
nhất, đọc lại số liệu HIỆN TẠI của từng video qua ShortVid (GET video/detail?url=<id> - 10 credit, một
request mỗi video; đo 13/10/2026) rồi ghi một dòng snapshot vào bảng D1 shortvid_video_stats. Ngân sách mỗi lượt:
settings.shortvid_trending_max_requests, và không vượt hạn mức ngày còn lại của key trừ shortvid_daily_reserve (lượt ShortVid chính
ở chế độ liên tục dùng hết phần còn lại, nên lượt này chạy trước thì lấy phần của nó trước).

trending() tính tốc độ từ HAI snapshot gần nhất cách nhau >= 1 giờ: lượt xem/giờ, bình luận/giờ và % tăng lượt xem mỗi
giờ; cờ `hot` khi lượt xem/giờ nằm trong top 10% (của mọi video có tốc độ) hoặc tăng > 50%/giờ.

Video có bình luận/giờ cao (rising_video_ids) được xếp vào hàng đợi bình luận của ShortVid (shortvid_comments.enqueue_videos) -
lượt ShortVid kế tiếp cào bình luận của chúng trước phần hashtag, như hàng đợi tuần. Chạy độc lập với lượt ShortVid chính, nhịp
gọi riêng (_Pacer(settings.shortvid_manual_requests_per_minute))."""

from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import Column, Index, Integer, Table, Text, and_, func, select
from sqlalchemy.schema import CreateIndex, CreateTable

from app.clients.shortvid import ShortVidStop, map_video, shortvid_get
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec, d1_insert_many, json_values, metadata
from app.repositories.d1.tables import movies, posts
from app.services.accounts.platform_config_db import get_shortvid_api_settings
from app.services.shortvid import shortvid_comments
from app.services.shortvid.shortvid_tiktok import Budget, _Pacer, fetch_usage

logger = get_logger(__name__)

# Hai snapshot phải cách nhau ít nhất chừng này mới tính tốc độ (gần quá thì nhiễu).
MIN_SPAN = timedelta(hours=1)
HOT_TOP_SHARE = 0.1
HOT_GROWTH_PCT_PER_HOUR = 50.0
# 7 cột mỗi dòng, D1 tối đa 100 tham số mỗi câu.
_INSERT_CHUNK = 14

_table_ready = False
_running = False

video_stats = Table(
    "shortvid_video_stats",
    metadata,
    Column("video_id", Text, nullable=False),
    Column("movie_id", Text),
    Column("at", Text, nullable=False),
    *(Column(name, Integer) for name in ("views", "likes", "comments", "shares")),
    Index("idx_shortvid_video_stats_video_at", "video_id", "at"),
    Index("idx_shortvid_video_stats_at", "at", "movie_id"),  # trending() lọc theo thời gian (và phim)
)
STAT_COLUMNS = tuple(c.name for c in video_stats.columns)


async def _ensure_table() -> None:
    global _table_ready
    if _table_ready:
        return
    await d1_exec(CreateTable(video_stats, if_not_exists=True), quiet=True)
    for index in video_stats.indexes:
        await d1_exec(CreateIndex(index, if_not_exists=True), quiet=True)
    _table_ready = True


# --- Tính tốc độ (thuần, không I/O - có test) ---------------------------------------------------------------------


def _parse_at(value: Any) -> datetime:
    at = datetime.fromisoformat(str(value))
    return at if at.tzinfo else at.replace(tzinfo=UTC)


def velocity(snapshots: list[dict[str, Any]], *, min_span: timedelta = MIN_SPAN) -> dict[str, Any] | None:
    """Tốc độ của MỘT video từ các snapshot của nó ({at, views, likes, comments, shares}, thứ tự bất kỳ): so snapshot mới
    nhất với snapshot gần nhất trước nó ít nhất `min_span`. None khi chưa có cặp như vậy. Số giảm (TikTok đôi khi trừ bớt
    lượt xem/bình luận) tính là 0. growth_pct_per_hour = None khi lượt xem trước đó là 0 (không có gốc để tính %)."""
    if not snapshots:
        return None
    ordered = sorted(snapshots, key=lambda snap: _parse_at(snap["at"]))
    latest = ordered[-1]
    latest_at = _parse_at(latest["at"])
    previous = next((snap for snap in reversed(ordered[:-1]) if latest_at - _parse_at(snap["at"]) >= min_span), None)
    if previous is None:
        return None
    hours = (latest_at - _parse_at(previous["at"])).total_seconds() / 3600

    def rate(key: str) -> float:
        return round(max(0, int(latest.get(key) or 0) - int(previous.get(key) or 0)) / hours, 2)

    prev_views = int(previous.get("views") or 0)
    views_per_hour = rate("views")
    return {
        "views_per_hour": views_per_hour,
        "likes_per_hour": rate("likes"),
        "comments_per_hour": rate("comments"),
        "shares_per_hour": rate("shares"),
        "growth_pct_per_hour": round(views_per_hour / prev_views * 100, 2) if prev_views > 0 else None,
        "span_hours": round(hours, 2),
        "since": previous["at"],
    }


def mark_hot(
    items: list[dict[str, Any]], *, top_share: float = HOT_TOP_SHARE, growth_pct: float = HOT_GROWTH_PCT_PER_HOUR
) -> None:
    """Gắn items[i]["hot"] (sửa tại chỗ): lượt xem/giờ thuộc top `top_share` của cả danh sách (và > 0), hoặc lượt xem tăng
    hơn `growth_pct` %/giờ. Mỗi item có khoá "velocity" (kết quả của velocity())."""
    rates = sorted((item["velocity"]["views_per_hour"] for item in items), reverse=True)
    cutoff = rates[max(0, math.ceil(len(rates) * top_share) - 1)] if rates else 0
    for item in items:
        vel = item["velocity"]
        growth = vel.get("growth_pct_per_hour")
        item["hot"] = (vel["views_per_hour"] > 0 and vel["views_per_hour"] >= cutoff) or (
            growth is not None and growth > growth_pct
        )


# --- Làm mới số liệu qua ShortVid -----------------------------------------------------------------------------------


async def _candidates(since: str, per_movie: int) -> list[dict[str, Any]]:
    """Top `per_movie` video TikTok liên quan của mỗi phim đang bật, đăng từ `since`, theo tương tác (cùng công thức với
    hàng đợi bình luận tuần)."""
    p, m = posts.alias("p"), movies.alias("m")
    rn = func.row_number().over(partition_by=p.c.movie_id, order_by=shortvid_comments.video_score(p).desc()).label("rn")
    ranked = (
        select(p.c.external_id, p.c.movie_id, rn)
        .select_from(p.join(m, and_(m.c.id == p.c.movie_id, m.c.enabled == 1)))
        .where(p.c.platform == "tiktok", p.c.posted_at >= since, relevant_post(p))
        .subquery()
    )
    query = select(ranked.c.external_id, ranked.c.movie_id).where(ranked.c.rn <= per_movie)
    return await d1_exec(query, quiet=True) or []


async def _insert_snapshots(rows: list[tuple[Any, ...]]) -> int:
    saved = 0
    for start in range(0, len(rows), _INSERT_CHUNK):
        chunk = rows[start : start + _INSERT_CHUNK]
        result = await d1_insert_many(video_stats, [dict(zip(STAT_COLUMNS, row, strict=True)) for row in chunk])
        if result:
            saved += len(chunk)
    return saved


async def refresh_top_videos(*, max_requests: int | None = None) -> dict[str, Any]:
    """Một lượt làm mới: đọc lại số liệu top video mỗi phim qua ShortVid và ghi snapshot. ShortVidStop (key sai / hết hạn mức) được
    ném ra sau khi đã lưu các snapshot lấy được."""
    stats: dict[str, Any] = {"candidates": 0, "requests": 0, "saved": 0, "missing": 0, "failed": 0}
    row = await get_shortvid_api_settings()
    api_key = ((row or {}).get("api_key") or "").strip()
    if not api_key:
        return {**stats, "skipped": "no_api_key"}
    await _ensure_table()
    since = (datetime.now(tz=UTC) - timedelta(days=settings.shortvid_trending_days)).isoformat()
    candidates: dict[str, str] = {}
    for cand in await _candidates(since, settings.shortvid_trending_per_movie):
        video_id = str(cand.get("external_id") or "")
        if video_id and video_id not in candidates:  # một video thuộc nhiều phim: đo một lần
            candidates[video_id] = cand["movie_id"]
    stats["candidates"] = len(candidates)
    if not candidates:
        return stats

    rows: list[tuple[Any, ...]] = []
    stopped: list[ShortVidStop] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        limit = max_requests if max_requests is not None else settings.shortvid_trending_max_requests
        day = ((await fetch_usage(api_key, client)) or {}).get("day") or {}
        if day.get("remaining") is not None:
            limit = min(limit, max(0, int(day["remaining"]) - settings.shortvid_daily_reserve))
        stats["budget"] = limit
        if limit <= 0:
            return {**stats, "skipped": "no_budget"}
        budget = Budget(limit)
        pacer = _Pacer(settings.shortvid_manual_requests_per_minute)
        queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        for item in candidates.items():
            queue.put_nowait(item)

        async def worker() -> None:
            while not queue.empty() and not stopped and budget.take():
                video_id, movie_id = queue.get_nowait()
                stats["requests"] += 1
                try:
                    data = await shortvid_get(client, pacer.wait, api_key, "video/detail", {"url": video_id})
                except ShortVidStop as exc:
                    stopped.append(exc)
                    return
                except Exception as exc:  # noqa: BLE001 - một video lỗi: lượt sau thử lại
                    stats["failed"] += 1
                    logger.warning("shortvid_trending_video_failed", video_id=video_id, error=str(exc)[:200])
                    continue
                mapped = map_video(data) if isinstance(data, dict) else None
                if mapped is None:
                    stats["missing"] += 1  # đã xoá / riêng tư
                    continue
                rows.append(
                    (
                        video_id,
                        movie_id,
                        datetime.now(tz=UTC).isoformat(timespec="seconds"),
                        mapped.get("play_count"),
                        mapped.get("like_count"),
                        mapped.get("comment_count"),
                        mapped.get("share_count"),
                    )
                )

        workers = max(1, min(settings.shortvid_concurrency, len(candidates)))
        await asyncio.gather(*(worker() for _ in range(workers)))

    stats["saved"] = await _insert_snapshots(rows)
    logger.info("shortvid_trending_refreshed", **stats)
    if stopped:
        raise stopped[0]
    return stats


def is_running() -> bool:
    return _running


async def run_refresh_tracked() -> dict[str, Any]:
    """Cho scheduler: làm mới snapshot rồi xếp hàng bình luận cho video đang lên. Không chạy chồng; ShortVidStop chỉ log."""
    global _running
    if _running:
        return {"skipped": "running"}
    _running = True
    try:
        stats: dict[str, Any]
        try:
            stats = await refresh_top_videos()
        except ShortVidStop as exc:
            logger.warning("shortvid_trending_stopped", reason=str(exc)[:200])
            stats = {"stopped": str(exc)[:200]}
        if settings.shortvid_trending_rising_limit > 0:
            rising = await rising_video_ids(settings.shortvid_trending_rising_limit)
            stats["rising_enqueued"] = await shortvid_comments.enqueue_videos(rising, reason="shortvid_trending")
        return stats
    finally:
        _running = False


# --- Đọc kết quả ------------------------------------------------------------------------------------------------


async def _velocities(hours: float, movie_id: str | None = None) -> list[dict[str, Any]]:
    """Mọi video có snapshot trong `hours` giờ qua (lọc theo phim nếu có) kèm tốc độ, cờ hot và số liệu hiện tại. Phân vị
    top 10% tính trên MỌI phim, nên lọc theo phim không làm đổi cờ hot."""
    await _ensure_table()
    since = (datetime.now(tz=UTC) - timedelta(hours=hours)).isoformat(timespec="seconds")
    snaps = await d1_exec(select(video_stats).where(video_stats.c.at >= since), quiet=True) or []
    by_video: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for snap in snaps:
        by_video[snap["video_id"]].append(snap)
    items = []
    for video_id, video_snaps in by_video.items():
        vel = velocity(video_snaps)
        if vel is None:
            continue
        latest = max(video_snaps, key=lambda snap: _parse_at(snap["at"]))
        items.append(
            {
                "video_id": video_id,
                "movie_id": latest["movie_id"],
                "at": latest["at"],
                "views": latest["views"],
                "likes": latest["likes"],
                "comments": latest["comments"],
                "shares": latest["shares"],
                "velocity": vel,
            }
        )
    mark_hot(items)
    if movie_id:
        items = [item for item in items if item["movie_id"] == movie_id]
    return items


async def _post_info(video_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not video_ids:
        return {}
    rows = await d1_exec(
        select(*(posts.c[name] for name in ("id", "external_id", "url", "author", "content", "reply_count")))
        .add_columns(posts.c.comments_crawled_at)
        .where(posts.c.platform == "tiktok", posts.c.external_id.in_(json_values(video_ids))),
        quiet=True,
    )
    return {str(row["external_id"]): row for row in rows or []}


async def trending(movie_id: str | None = None, hours: float = 24, limit: int = 30) -> dict[str, Any]:
    """Top video theo lượt xem/giờ của mỗi phim: {"hours", "movies": [{"movie_id", "title", "videos": [...]}]}."""
    items = await _velocities(hours, movie_id)
    post_by_video = await _post_info([item["video_id"] for item in items])
    titles = {
        row["id"]: row["title"]
        for row in await d1_exec(select(movies.c.id, movies.c.title).where(movies.c.enabled == 1), quiet=True) or []
    }
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        post = post_by_video.get(item["video_id"]) or {}
        grouped[item["movie_id"]].append(
            {
                **item,
                "post_id": post.get("id"),
                "url": post.get("url"),
                "author": post.get("author"),
                "desc": post.get("content"),
            }
        )
    groups = []
    for mid, videos in grouped.items():
        videos.sort(key=lambda video: video["velocity"]["views_per_hour"], reverse=True)
        groups.append(
            {"movie_id": mid, "title": titles.get(mid), "hot": sum(v["hot"] for v in videos), "videos": videos[:limit]}
        )
    groups.sort(key=lambda movie: (movie["hot"], movie["videos"][0]["velocity"]["views_per_hour"]), reverse=True)
    return {"hours": hours, "movies": groups}


async def rising_video_ids(limit: int) -> list[dict[str, Any]]:
    """Video đang lên về bình luận (>= settings.shortvid_trending_rising_comments_per_hour bình luận/giờ, 24 giờ qua), nhiều
    nhất trước, bỏ video đã cào bình luận trong settings.shortvid_trending_comments_recrawl_hours giờ qua. Mỗi phần tử có đủ
    trường cho shortvid_comments.enqueue_videos: video_id, post_id, reply_count, comments_per_hour."""
    items = [
        item
        for item in await _velocities(24)
        if item["velocity"]["comments_per_hour"] >= settings.shortvid_trending_rising_comments_per_hour
    ]
    items.sort(key=lambda item: item["velocity"]["comments_per_hour"], reverse=True)
    post_by_video = await _post_info([item["video_id"] for item in items])
    fresh_after = datetime.now(tz=UTC) - timedelta(hours=settings.shortvid_trending_comments_recrawl_hours)
    rising = []
    for item in items:
        post = post_by_video.get(item["video_id"])
        if not post:
            continue
        crawled = post.get("comments_crawled_at")
        if crawled and _parse_at(crawled) >= fresh_after:
            continue
        rising.append(
            {
                "video_id": item["video_id"],
                "post_id": post["id"],
                "reply_count": int(item.get("comments") or post.get("reply_count") or 0),
                "comments_per_hour": item["velocity"]["comments_per_hour"],
            }
        )
        if len(rising) >= limit:
            break
    return rising
