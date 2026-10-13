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
import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
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
# Hash {câu tìm đã chuẩn hoá: JSON {at, videos, published}} - kết quả lần tìm gần nhất, cho tab Omar.
STATS_KEY = f"{REDIS_KEY_PREFIX}omar:search:stats"
_CUSTOM_TABLE_READY = False
ENDPOINTS = ("search_videos", "search_photos")
_WORKERS = 4


async def _ensure_custom_table() -> None:
    """Câu tìm người dùng tự thêm ở tab Omar (ngoài tên phim và câu tìm Facebook/Threads)."""
    global _CUSTOM_TABLE_READY
    if _CUSTOM_TABLE_READY:
        return
    await d1_query(
        "CREATE TABLE IF NOT EXISTS omar_search_queries (id TEXT PRIMARY KEY, movie_id TEXT NOT NULL, query TEXT NOT NULL, "
        "created_at TEXT NOT NULL)",
        quiet=True,
    )
    _CUSTOM_TABLE_READY = True


async def add_custom_query(movie_id: str, query: str) -> dict[str, Any]:
    await _ensure_custom_table()
    row = {"id": f"oq_{uuid.uuid4().hex[:16]}", "movie_id": movie_id, "query": query.strip()}
    await d1_query(
        "INSERT INTO omar_search_queries (id, movie_id, query, created_at) VALUES (?, ?, ?, ?)",
        [row["id"], movie_id, row["query"], datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat()],
    )
    return row


async def delete_custom_query(query_id: str) -> None:
    await _ensure_custom_table()
    await d1_query("DELETE FROM omar_search_queries WHERE id = ?", [query_id])


async def build_queries() -> list[dict[str, Any]]:
    """[{query, key, keyword_id, movie, movie_id, source, id}] cho mọi phim đang bật có từ khoá hashtag TikTok đang bật.
    source: title (tên phim) | title_plain (không dấu) | social (câu tìm Facebook/Threads) | custom (tự thêm ở tab Omar)."""
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

    titles = {row["movie_id"]: str(row.get("title") or "") for row in rows}

    def add(query: str, movie_id: str, source: str, query_id: str | None = None) -> None:
        # Khoá phân biệt có dấu / không dấu: trên TikTok "trại buôn người" và "trai buon nguoi" cho kết quả khác nhau.
        key = " ".join(query.lower().split())
        if len(normalize_title(query)) < 4 or key in seen or movie_id not in anchor:
            return
        seen.add(key)
        out.append(
            {
                "query": query.strip(),
                "key": key,
                "keyword_id": anchor[movie_id],
                "movie": titles.get(movie_id, ""),
                "movie_id": movie_id,
                "source": source,
                "id": query_id,
            }
        )

    await _ensure_custom_table()
    for row in (
        await d1_query("SELECT id, movie_id, query FROM omar_search_queries ORDER BY created_at", quiet=True) or []
    ):
        add(str(row["query"]), row["movie_id"], "custom", row["id"])
    for row in rows:
        title = str(row.get("title") or "")
        if title:
            add(title, row["movie_id"], "title")
            add(normalize_title(title), row["movie_id"], "title_plain")
        if row["platform"] in ("facebook", "threads") and not str(row["keyword"]).startswith("#"):
            add(str(row["keyword"]), row["movie_id"], "social")
    return out


async def query_status() -> list[dict[str, Any]]:
    """Mọi câu tìm kèm kết quả lần tìm gần nhất - cho tab Omar."""
    raw = await get_redis_client().hgetall(STATS_KEY) or {}
    out = []
    for query in await build_queries():
        try:
            last = json.loads(raw.get(query["key"]) or "{}")
        except ValueError:
            last = {}
        out.append({**{k: query[k] for k in ("query", "movie", "movie_id", "source", "id")}, "last": last or None})
    return out


async def _search_one(
    client: httpx.AsyncClient,
    pacer: _Pacer,
    api_key: str,
    query: dict[str, Any],
    take: Callable[[], bool],
    stats: dict[str, Any],
) -> bool:
    """Cả hai endpoint, tối đa settings.omar_search_pages trang mỗi endpoint. True khi tìm xong không lỗi - lỗi thì lượt
    sau tìm lại câu này (search_videos của Omar hay trả 424 "Video search failed" từng đợt, đo 12/10/2026)."""
    from app.services.omar_tiktok import OmarStop, map_web_item, next_cursor, omar_get, publish_videos

    local: dict[str, Any] = {"published": 0, "skipped_seen": 0}
    videos = 0
    complete = True
    for path in ENDPOINTS:
        cursor = "0"
        for _ in range(settings.omar_search_pages):
            if not take():
                return False
            stats["requests"] = int(stats.get("requests") or 0) + 1
            stats["search_requests"] = int(stats.get("search_requests") or 0) + 1
            params = {"query": query["query"], "cursor": cursor}
            if path == "search_videos":
                # Video mới trong tuần trước (search_photos không có bộ lọc này).
                if settings.omar_search_date:
                    params["date"] = settings.omar_search_date
                if settings.omar_search_sort:
                    params["sort"] = settings.omar_search_sort
            try:
                data = await omar_get(client, pacer, api_key, path, params)
            except OmarStop:
                raise
            except Exception as exc:  # noqa: BLE001 - trang lỗi: bỏ phần còn lại của endpoint này, hôm sau tìm lại
                stats["failed"] = int(stats.get("failed") or 0) + 1
                logger.warning("omar_search_failed_page", query=query["query"], path=path, error=str(exc)[:200])
                complete = False
                break
            mapped = [v for v in (map_web_item(raw) for raw in (data or {}).get("results") or []) if v]
            videos += len(mapped)
            await publish_videos(mapped, query["keyword_id"], local)
            following = next_cursor(data or {})
            if following is None:
                break
            cursor = following
    stats["search_videos"] = int(stats.get("search_videos") or 0) + videos
    stats["videos"] = int(stats.get("videos") or 0) + videos
    for key in ("published", "skipped_seen"):
        stats[key] = int(stats.get(key) or 0) + local[key]
    await get_redis_client().hset(
        STATS_KEY,
        query["key"],
        json.dumps({"at": datetime.now(tz=UTC).isoformat(), "videos": videos, "published": local["published"]}),
    )
    return complete


_manual_tasks: set[asyncio.Task] = set()


def search_now(query: dict[str, Any]) -> None:
    """Tìm ngay một câu (vừa thêm ở tab Omar), độc lập với lượt đang chạy."""
    from app.services.omar_tiktok import _Pacer
    from app.services.platform_config_db import get_omar_api_settings

    async def run() -> None:
        api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
        if not api_key:
            return
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                await _search_one(
                    client, _Pacer(settings.omar_manual_requests_per_minute), api_key, query, lambda: True, {}
                )
            await get_redis_client().hset(
                DONE_KEY, query["key"], datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("omar_search_manual_failed", query=query.get("query"), error=str(exc)[:300])

    task = asyncio.create_task(run())
    _manual_tasks.add(task)
    task.add_done_callback(_manual_tasks.discard)


async def run_search(
    client: httpx.AsyncClient,
    pacer: _Pacer,
    api_key: str,
    budget: Budget,
    stats: dict[str, Any],
    should_stop: Callable[[], bool],
) -> None:
    from app.services.omar_tiktok import OmarStop

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

    def take() -> bool:
        return not halted() and budget.take()

    async def worker() -> None:
        while not queue.empty() and not halted():
            query = queue.get_nowait()
            try:
                if await _search_one(client, pacer, api_key, query, take, stats):
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
