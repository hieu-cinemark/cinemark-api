"""Tìm bài TikTok theo TỪ KHOÁ qua Omar API (search_videos + search_photos, từ 2026-10-12) - chạy song song trong mỗi
lượt Omar (omar_tiktok.run_omar_round), mỗi câu tìm một lần mỗi ngày.

Cào hashtag bỏ sót bài không gắn hashtag phim hoặc gắn sai ("#Trại Buôn Người" có dấu cách không thành hashtag) và cả bài
gắn đúng mà feed hashtag không trả về - đo 12/10/2026 trên 6 bài ảnh đầu của "trại buôn người": 2 bài (1,1 triệu và 2,2
triệu lượt xem, có bài của trang chính thức) chưa có trong D1.

Câu tìm của mỗi phim đang bật có từ khoá hashtag TikTok: tên phim có dấu, không dấu, và các câu tìm Facebook/Threads đang
bật của phim (người nhập hoặc AI query discovery). Mỗi câu lật tối đa settings.omar_search_pages trang ở cả hai endpoint.
Bài đi đúng đường của spider (raw_posts, keyword_id = hashtag đầu tiên của phim) nên bộ lọc liên quan ở ingest xử lý như
mọi bài khác."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import httpx

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.d1 import d1_query
from app.services.relevance_rules import normalize_title

if TYPE_CHECKING:
    from app.services.omar_tiktok import Budget, _Pacer

logger = get_logger(__name__)

# Hash {câu tìm đã chuẩn hoá: ngày (giờ VN) đã tìm xong} - mỗi câu một lần mỗi ngày.
DONE_KEY = f"{REDIS_KEY_PREFIX}omar:search:done"
ENDPOINTS = ("search_videos", "search_photos")
_WORKERS = 4


async def build_queries() -> list[dict[str, Any]]:
    """[{query, keyword_id, movie}] cho mọi phim đang bật có từ khoá hashtag TikTok đang bật."""
    rows = (
        await d1_query(
            """
            SELECT k.id, k.movie_id, k.platform, k.keyword, m.title
            FROM keywords k JOIN movies m ON m.id = k.movie_id
            WHERE k.enabled = 1 AND m.enabled = 1
            ORDER BY k.created_at
            """,
            quiet=True,
        )
        or []
    )
    anchor: dict[str, str] = {}
    for row in rows:
        if row["platform"] == "tiktok" and str(row["keyword"]).startswith("#"):
            anchor.setdefault(row["movie_id"], row["id"])
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(query: str, movie_id: str, title: str) -> None:
        key = normalize_title(query)
        if len(key) < 4 or key in seen or movie_id not in anchor:
            return
        seen.add(key)
        out.append({"query": query.strip(), "key": key, "keyword_id": anchor[movie_id], "movie": title})

    for row in rows:
        title = str(row.get("title") or "")
        if title:
            add(title, row["movie_id"], title)
            add(normalize_title(title), row["movie_id"], title)
        if row["platform"] in ("facebook", "threads") and not str(row["keyword"]).startswith("#"):
            add(str(row["keyword"]), row["movie_id"], title)
    return out


async def run_search(
    client: httpx.AsyncClient,
    pacer: _Pacer,
    api_key: str,
    budget: Budget,
    stats: dict[str, Any],
    should_stop: Callable[[], bool],
) -> None:
    from app.services.omar_tiktok import OmarStop, map_web_item, next_cursor, omar_get, publish_videos

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    redis = get_redis_client()
    done = await redis.hgetall(DONE_KEY) or {}
    queries = [q for q in await build_queries() if done.get(q["key"]) != today]
    if not queries:
        return
    stats.update(search_queries=len(queries), search_requests=0, search_videos=0)
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    for query in queries:
        queue.put_nowait(query)
    stopped: list[BaseException] = []

    def halted() -> bool:
        return bool(stopped) or should_stop() or budget.exhausted

    async def search(query: dict[str, Any]) -> bool:
        """Cả hai endpoint, tối đa settings.omar_search_pages trang mỗi endpoint. True khi tìm xong không lỗi - lỗi thì lượt
        sau tìm lại câu này (search_videos của Omar hay trả 424 "Video search failed" từng đợt, đo 12/10/2026)."""
        complete = True
        for path in ENDPOINTS:
            cursor = "0"
            for _ in range(settings.omar_search_pages):
                if halted() or not budget.take():
                    return False
                stats["requests"] += 1
                stats["search_requests"] += 1
                try:
                    data = await omar_get(client, pacer, api_key, path, {"query": query["query"], "cursor": cursor})
                except OmarStop:
                    raise
                except Exception as exc:  # noqa: BLE001 - trang lỗi: bỏ phần còn lại của endpoint này, hôm sau tìm lại
                    stats["failed"] += 1
                    logger.warning("omar_search_failed_page", query=query["query"], path=path, error=str(exc)[:200])
                    complete = False
                    break
                mapped = [v for v in (map_web_item(raw) for raw in (data or {}).get("results") or []) if v]
                stats["search_videos"] += len(mapped)
                stats["videos"] += len(mapped)
                await publish_videos(mapped, query["keyword_id"], stats)
                following = next_cursor(data or {})
                if following is None:
                    break
                cursor = following
        return complete

    async def worker() -> None:
        while not queue.empty() and not halted():
            query = queue.get_nowait()
            try:
                if await search(query):
                    await redis.hset(DONE_KEY, query["key"], today)
            except OmarStop as exc:
                stopped.append(exc)

    await asyncio.gather(*(worker() for _ in range(min(_WORKERS, len(queries)))))
    logger.info(
        "omar_search_processed",
        queries=len(queries),
        requests=stats["search_requests"],
        videos=stats["search_videos"],
    )
    if stopped:
        raise stopped[0]
