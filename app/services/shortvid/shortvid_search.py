"""Tìm bài TikTok theo TỪ KHOÁ qua ShortVid API (video/search, vùng VN) - chạy song song trong mỗi
lượt ShortVid (shortvid_tiktok.run_shortvid_round), mỗi câu tìm một lần mỗi ngày.

Cào hashtag bỏ sót bài không gắn hashtag phim hoặc gắn sai ("#Trại Buôn Người" có dấu cách không thành hashtag) và cả bài
gắn đúng mà feed hashtag không trả về - đo 12/10/2026 trên 6 bài ảnh đầu của "trại buôn người": 2 bài (1,1 triệu và 2,2
triệu lượt xem, có bài của trang chính thức) chưa có trong D1.

Câu tìm của mỗi phim đang bật có từ khoá hashtag TikTok: tên phim có dấu, không dấu, và các câu tìm Facebook/Threads đang
bật của phim (người nhập hoặc AI query discovery). Mỗi câu lật tối đa settings.shortvid_search_pages trang mỗi kiểu tìm
(settings.shortvid_search_modes). video/search của ShortVid trả cả bài video lẫn bài ảnh nên không có lượt tìm ảnh riêng.
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
from sqlalchemy import Column, Table, Text, delete, insert, select
from sqlalchemy.schema import CreateTable

from app.clients.d1 import d1_query
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, metadata
from app.repositories.d1.tables import keywords, movies
from app.services.shortvid import shortvid_yield
from app.services.relevance.relevance_rules import normalize_title

if TYPE_CHECKING:
    from app.services.shortvid.shortvid_tiktok import Budget, _Pacer

logger = get_logger(__name__)

# Hash {câu tìm đã chuẩn hoá: ngày (giờ VN) đã tìm xong} - mỗi câu một lần mỗi ngày.
DONE_KEY = f"{REDIS_KEY_PREFIX}shortvid:search:done"
# Hash {câu tìm đã chuẩn hoá: JSON {at, videos, published}} - kết quả lần tìm gần nhất, cho tab ShortVid.
STATS_KEY = f"{REDIS_KEY_PREFIX}shortvid:search:stats"
_CUSTOM_TABLE_READY = False
# Hash {câu tìm đã chuẩn hoá: số lần tìm liên tiếp không ra video mới} - câu tự sinh (gợi ý TikTok) bị xoá khi quá ngưỡng.
ZERO_KEY = f"{REDIS_KEY_PREFIX}shortvid:search:zero"
# Con trỏ trang đang dở của từng (câu, kiểu tìm) khi lượt dừng giữa chừng vì hết ngân sách / hết hạn mức - lượt sau lật
# tiếp từ đó thay vì từ trang 1. Xoá khi kiểu tìm đó chạy xong.
CURSOR_KEY = f"{REDIS_KEY_PREFIX}shortvid:search:cursor"
_WORKERS = 4


# Kiểu tìm trong settings.shortvid_search_modes ("thời gian:sắp xếp") -> tham số video/search của ShortVid.
_PUBLISH_TIME = {"": 0, "all": 0, "day": 1, "week": 7, "month": 30, "3months": 90, "6months": 180}
_SORT_TYPE = {"": 0, "relevance": 0, "likes": 1, "popular": 1, "newest": 3}


def mode_params(mode: dict[str, str]) -> dict[str, int]:
    """{"date": "week", "sort": "newest"} -> {"publish_time": 7, "sort_type": 3}. Giá trị lạ thì về mặc định (mọi lúc, liên quan)."""
    return {
        "publish_time": _PUBLISH_TIME.get(mode.get("date", ""), 0),
        "sort_type": _SORT_TYPE.get(mode.get("sort", ""), 0),
    }


def search_modes() -> list[dict[str, str]]:
    """settings.shortvid_search_modes -> [{"date": "week", "sort": "newest"}, {"sort": "relevance"}, ...]."""
    modes = []
    for item in settings.shortvid_search_modes.split(","):
        date, _, sort = item.strip().partition(":")
        mode = {k: v for k, v in (("date", date.strip()), ("sort", sort.strip())) if v}
        if mode not in modes:
            modes.append(mode)
    return modes or [{}]


custom_queries = Table(
    "shortvid_search_queries",
    metadata,
    Column("id", Text, primary_key=True),
    Column("movie_id", Text, nullable=False),
    Column("query", Text, nullable=False),
    Column("created_at", Text, nullable=False),
    # custom (người dùng thêm) | suggest (tự sinh từ gợi ý tìm kiếm / xu hướng của TikTok). Cột thêm sau.
    Column("source", Text),
)


async def _ensure_custom_table() -> None:
    """Câu tìm người dùng tự thêm ở tab ShortVid (ngoài tên phim và câu tìm Facebook/Threads)."""
    global _CUSTOM_TABLE_READY
    if _CUSTOM_TABLE_READY:
        return
    await d1_exec(CreateTable(custom_queries, if_not_exists=True), quiet=True)
    # Bảng tạo trước khi có cột source: thêm cột (đã có thì lỗi, bỏ qua).
    await d1_query("ALTER TABLE shortvid_search_queries ADD COLUMN source TEXT DEFAULT 'custom'", quiet=True)
    _CUSTOM_TABLE_READY = True


async def add_custom_query(movie_id: str, query: str, source: str = "custom") -> dict[str, Any]:
    await _ensure_custom_table()
    row = {"id": f"oq_{uuid.uuid4().hex[:16]}", "movie_id": movie_id, "query": query.strip()}
    await d1_exec(
        insert(custom_queries).values(
            **row, created_at=datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat(), source=source
        )
    )
    return row


async def delete_custom_query(query_id: str) -> None:
    await _ensure_custom_table()
    await d1_exec(delete(custom_queries).where(custom_queries.c.id == query_id))


async def build_queries() -> list[dict[str, Any]]:
    """[{query, key, keyword_id, movie, movie_id, source, id}] cho mọi phim đang bật có từ khoá hashtag TikTok đang bật.
    source: title (tên phim) | title_plain (không dấu) | social (câu tìm Facebook/Threads) | custom (tự thêm ở tab ShortVid) |
    suggest (tự sinh từ gợi ý tìm kiếm / xu hướng TikTok - discover_queries)."""
    rows = (
        await d1_exec(
            select(keywords.c.id, keywords.c.movie_id, keywords.c.platform, keywords.c.keyword, movies.c.title)
            .select_from(keywords.join(movies, movies.c.id == keywords.c.movie_id))
            .where(keywords.c.enabled == 1, movies.c.enabled == 1)
            .order_by(keywords.c.created_at),
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
    custom = await d1_exec(
        select(
            custom_queries.c.id, custom_queries.c.movie_id, custom_queries.c.query, custom_queries.c.source
        ).order_by(custom_queries.c.created_at),
        quiet=True,
    )
    for row in custom or []:
        add(str(row["query"]), row["movie_id"], row.get("source") or "custom", row["id"])
    for row in rows:
        title = str(row.get("title") or "")
        if title:
            add(title, row["movie_id"], "title")
            add(normalize_title(title), row["movie_id"], "title_plain")
        if row["platform"] in ("facebook", "threads") and not str(row["keyword"]).startswith("#"):
            add(str(row["keyword"]), row["movie_id"], "social")
    return out


async def query_status() -> list[dict[str, Any]]:
    """Mọi câu tìm kèm kết quả lần tìm gần nhất - cho tab ShortVid."""
    raw = await get_redis_client().hgetall(STATS_KEY) or {}
    out = []
    for query in await build_queries():
        try:
            last = json.loads(raw.get(query["key"]) or "{}")
        except ValueError:
            last = {}
        out.append({**{k: query[k] for k in ("query", "movie", "movie_id", "source", "id")}, "last": last or None})
    return out


class _QuerySearch:
    """Tìm một câu theo từng kiểu trong settings.shortvid_search_modes; tối đa settings.shortvid_search_pages
    trang mỗi kiểu. Mỗi lần step() là MỘT trang, để run_search xen kẽ các câu (run_round_robin) thay vì gọi dồn một câu."""

    def __init__(self, query: dict[str, Any]) -> None:
        self.query = query
        self.passes = [("video/search", mode) for mode in search_modes()]
        self.index = 0
        self.cursor: str | None = None
        self.pages = 0
        self.empty_tries = 0
        self.local: dict[str, Any] = {"published": 0, "skipped_seen": 0}
        self.seen_ids: set[str] = set()
        # False khi một trang lỗi - lượt sau tìm lại câu này. halted: dừng vì hết ngân sách - không tính là đã tìm xong.
        self.complete = True
        self.halted = False

    def _field(self) -> str:
        path, mode = self.passes[self.index]
        return f"{self.query['key']}|{path}|{json.dumps(mode, sort_keys=True)}"

    async def _next_pass(self, *, keep_cursor: bool = False) -> None:
        redis = get_redis_client()
        if keep_cursor and self.cursor not in (None, "0"):
            await redis.hset(CURSOR_KEY, self._field(), self.cursor)
        else:
            await redis.hdel(CURSOR_KEY, self._field())
        self.index += 1
        self.cursor = None
        self.pages = 0
        self.empty_tries = 0

    async def step(
        self, client: httpx.AsyncClient, pacer: _Pacer, api_key: str, take: Callable[[], bool], stats: dict[str, Any]
    ) -> float | None:
        """Một trang. Trả về số giây chờ trước trang kế của câu này, hoặc None khi câu xong / dừng (self.halted)."""
        from app.clients.shortvid import PAGE_SIZE, ShortVidStop, map_video, next_cursor, shortvid_get, videos_of
        from app.services.shortvid.shortvid_tiktok import (
            _EMPTY_RETRIES,
            _EMPTY_RETRY_SECONDS,
            _KEYWORD_GAP_SECONDS,
            publish_videos,
        )

        if self.index >= len(self.passes):
            return None
        redis = get_redis_client()
        path, mode = self.passes[self.index]
        if self.cursor is None:
            saved = await redis.hget(CURSOR_KEY, self._field())
            self.cursor = (saved.decode() if isinstance(saved, bytes) else saved) or "0"
        if not take():
            if self.cursor != "0":
                await redis.hset(CURSOR_KEY, self._field(), self.cursor)
            self.halted = True
            return None
        stats["requests"] = int(stats.get("requests") or 0) + 1
        stats["search_requests"] = int(stats.get("search_requests") or 0) + 1
        await shortvid_yield.add("search", requests=1)
        params = {
            "keywords": self.query["query"],
            "region": settings.shortvid_search_region,
            "count": PAGE_SIZE["video/search"],
            "cursor": self.cursor,
            **mode_params(mode),
        }
        try:
            data = await shortvid_get(client, pacer.wait, api_key, path, params)
        except ShortVidStop:
            if self.cursor != "0":
                await redis.hset(CURSOR_KEY, self._field(), self.cursor)
            raise
        except Exception as exc:  # noqa: BLE001 - trang lỗi: bỏ phần còn lại của kiểu tìm này, hôm sau tìm lại
            stats["failed"] = int(stats.get("failed") or 0) + 1
            logger.warning("shortvid_search_failed_page", query=self.query["query"], path=path, error=str(exc)[:200])
            self.complete = False
            await self._next_pass()
            return _KEYWORD_GAP_SECONDS if self.index < len(self.passes) else None
        results = videos_of(data)
        if not results and self.cursor != "0":
            # Rỗng giữa chừng: thường bị giới hạn tạm thời - chờ vài phút gọi lại đúng cursor; vẫn rỗng thì để đó (giữ
            # cursor cho lần tìm sau) và sang kiểu tìm kế.
            self.empty_tries += 1
            if self.empty_tries <= _EMPTY_RETRIES:
                stats["search_empty_retries"] = int(stats.get("search_empty_retries") or 0) + 1
                return _EMPTY_RETRY_SECONDS * self.empty_tries  # 3, 6, 9 phút
            await self._next_pass(keep_cursor=True)
            return _KEYWORD_GAP_SECONDS if self.index < len(self.passes) else None
        self.empty_tries = 0
        mapped = [v for v in (map_video(raw, source="shortvid_search") for raw in results) if v]
        # Các kiểu tìm trùng nhau một phần - chỉ đếm/gửi video chưa gặp trong câu này.
        mapped = [v for v in mapped if v["video_id"] not in self.seen_ids]
        self.seen_ids.update(v["video_id"] for v in mapped)
        await publish_videos(mapped, self.query["keyword_id"], self.local, source="search")
        self.pages += 1
        following = next_cursor(data if isinstance(data, dict) else None)
        if following is None or self.pages >= settings.shortvid_search_pages:
            await self._next_pass()
        else:
            self.cursor = following
        return _KEYWORD_GAP_SECONDS if self.index < len(self.passes) else None

    async def finish(self, stats: dict[str, Any]) -> bool:
        """Cộng số liệu của câu vào lượt. True khi tìm xong không lỗi."""
        videos = len(self.seen_ids)
        stats["search_videos"] = int(stats.get("search_videos") or 0) + videos
        stats["videos"] = int(stats.get("videos") or 0) + videos
        for key in ("published", "skipped_seen"):
            stats[key] = int(stats.get(key) or 0) + self.local[key]
        await get_redis_client().hset(
            STATS_KEY,
            self.query["key"],
            json.dumps(
                {"at": datetime.now(tz=UTC).isoformat(), "videos": videos, "published": self.local["published"]}
            ),
        )
        if self.complete and self.query.get("source") == "suggest" and self.query.get("id"):
            await _track_zero(self.query, self.local["published"])
        return self.complete


async def _search_one(
    client: httpx.AsyncClient,
    pacer: _Pacer,
    api_key: str,
    query: dict[str, Any],
    take: Callable[[], bool],
    stats: dict[str, Any],
) -> bool:
    """Tìm hết một câu liền một mạch (tìm ngay một câu từ tab ShortVid). True khi tìm xong không lỗi."""
    search = _QuerySearch(query)
    while True:
        delay = await search.step(client, pacer, api_key, take, stats)
        if delay is None:
            break
        await asyncio.sleep(delay)
    if search.halted:
        return False
    return await search.finish(stats)


async def _track_zero(query: dict[str, Any], published: int) -> None:
    """Câu tự sinh không ra video mới settings.shortvid_search_prune_days lần tìm liên tiếp thì xoá (đỡ tốn ngân sách)."""
    redis = get_redis_client()
    if published:
        await redis.hdel(ZERO_KEY, query["key"])
        return
    streak = int(await redis.hincrby(ZERO_KEY, query["key"], 1))
    if streak >= settings.shortvid_search_prune_days:
        await delete_custom_query(query["id"])
        await redis.hdel(ZERO_KEY, query["key"])
        logger.info("shortvid_search_query_pruned", query=query["query"], movie=query.get("movie"))


_manual_tasks: set[asyncio.Task] = set()


def search_now(query: dict[str, Any]) -> None:
    """Tìm ngay một câu (vừa thêm ở tab ShortVid), độc lập với lượt đang chạy."""
    from app.services.accounts.platform_config_db import get_shortvid_api_settings
    from app.services.shortvid.shortvid_tiktok import _Pacer

    async def run() -> None:
        api_key = ((await get_shortvid_api_settings() or {}).get("api_key") or "").strip()
        if not api_key:
            return
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                await _search_one(
                    client, _Pacer(settings.shortvid_manual_requests_per_minute), api_key, query, lambda: True, {}
                )
            await get_redis_client().hset(
                DONE_KEY, query["key"], datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("shortvid_search_manual_failed", query=query.get("query"), error=str(exc)[:300])

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
    from app.services.shortvid.shortvid_tiktok import ShortVidStop, run_round_robin

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    redis = get_redis_client()
    done = await redis.hgetall(DONE_KEY) or {}
    queries = [q for q in await build_queries() if done.get(q["key"]) != today]
    if not queries:
        return
    stats.update(search_queries=len(queries), search_requests=0, search_videos=0)
    stopped: list[BaseException] = []

    def halted() -> bool:
        return bool(stopped) or should_stop() or budget.exhausted

    def take() -> bool:
        return not halted() and budget.take()

    async def step(search: _QuerySearch) -> float | None:
        try:
            delay = await search.step(client, pacer, api_key, take, stats)
        except ShortVidStop as exc:
            stopped.append(exc)
            return None
        if delay is None and not search.halted and await search.finish(stats):
            await redis.hset(DONE_KEY, search.query["key"], today)
        return delay

    # Xen kẽ giữa các câu, mỗi lần một trang (không gọi dồn một câu) - xem shortvid_tiktok.run_round_robin.
    await run_round_robin([_QuerySearch(q) for q in queries], step, workers=_WORKERS, should_stop=halted)
    logger.info(
        "shortvid_search_processed",
        queries=len(queries),
        requests=stats["search_requests"],
        videos=stats["search_videos"],
    )
    if stopped:
        raise stopped[0]
