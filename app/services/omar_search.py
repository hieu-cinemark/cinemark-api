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
from app.services import omar_yield
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
# Hash {câu tìm đã chuẩn hoá: số lần tìm liên tiếp không ra video mới} - câu tự sinh (gợi ý TikTok) bị xoá khi quá ngưỡng.
ZERO_KEY = f"{REDIS_KEY_PREFIX}omar:search:zero"
# Con trỏ trang đang dở của từng (câu, kiểu tìm) khi lượt dừng giữa chừng vì hết ngân sách / hết hạn mức - lượt sau lật
# tiếp từ đó thay vì từ trang 1. Xoá khi kiểu tìm đó chạy xong.
CURSOR_KEY = f"{REDIS_KEY_PREFIX}omar:search:cursor"
# Ngày (giờ VN) đã lấy gợi ý tìm kiếm của TikTok - mỗi ngày một lần.
SUGGEST_DAY_KEY = f"{REDIS_KEY_PREFIX}omar:search:suggest_day"
# Gợi ý chứa các cụm này là tìm kiếm nước ngoài (tên phim trùng chữ / phim Việt trên Netflix nước ngoài) - bỏ.
_FOREIGN = (" espanol ", " de que ", " pelicula ", " netflix ", " sub indo ", " english ", " trata ")
ENDPOINTS = ("search_videos", "search_photos")
_WORKERS = 4


def search_modes() -> list[dict[str, str]]:
    """settings.omar_search_modes -> [{"date": "week", "sort": "newest"}, {"sort": "relevance"}, ...]."""
    modes = []
    for item in settings.omar_search_modes.split(","):
        date, _, sort = item.strip().partition(":")
        mode = {k: v for k, v in (("date", date.strip()), ("sort", sort.strip())) if v}
        if mode not in modes:
            modes.append(mode)
    return modes or [{}]


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
    # source: custom (người dùng thêm) | suggest (tự sinh từ gợi ý tìm kiếm / xu hướng của TikTok). Cột thêm sau.
    await d1_query("ALTER TABLE omar_search_queries ADD COLUMN source TEXT DEFAULT 'custom'", quiet=True)
    _CUSTOM_TABLE_READY = True


async def add_custom_query(movie_id: str, query: str, source: str = "custom") -> dict[str, Any]:
    await _ensure_custom_table()
    row = {"id": f"oq_{uuid.uuid4().hex[:16]}", "movie_id": movie_id, "query": query.strip()}
    await d1_query(
        "INSERT INTO omar_search_queries (id, movie_id, query, created_at, source) VALUES (?, ?, ?, ?, ?)",
        [row["id"], movie_id, row["query"], datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat(), source],
    )
    return row


async def delete_custom_query(query_id: str) -> None:
    await _ensure_custom_table()
    await d1_query("DELETE FROM omar_search_queries WHERE id = ?", [query_id])


async def build_queries() -> list[dict[str, Any]]:
    """[{query, key, keyword_id, movie, movie_id, source, id}] cho mọi phim đang bật có từ khoá hashtag TikTok đang bật.
    source: title (tên phim) | title_plain (không dấu) | social (câu tìm Facebook/Threads) | custom (tự thêm ở tab Omar) |
    suggest (tự sinh từ gợi ý tìm kiếm / xu hướng TikTok - discover_queries)."""
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
    custom = await d1_query(
        "SELECT id, movie_id, query, source FROM omar_search_queries ORDER BY created_at", quiet=True
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


class _QuerySearch:
    """Tìm một câu: video theo từng kiểu trong settings.omar_search_modes, rồi bài ảnh; tối đa settings.omar_search_pages
    trang mỗi kiểu. Mỗi lần step() là MỘT trang, để run_search xen kẽ các câu (run_round_robin) thay vì gọi dồn một câu."""

    def __init__(self, query: dict[str, Any]) -> None:
        self.query = query
        self.passes = [("search_videos", mode) for mode in search_modes()] + [("search_photos", {})]
        self.index = 0
        self.cursor: str | None = None
        self.pages = 0
        self.empty_tries = 0
        self.local: dict[str, Any] = {"published": 0, "skipped_seen": 0}
        self.seen_ids: set[str] = set()
        # False khi một trang lỗi - lượt sau tìm lại câu này (search_videos của Omar hay trả 424 "Video search failed" từng
        # đợt, đo 12/10/2026). halted: dừng vì hết ngân sách - không tính là đã tìm xong.
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
        from app.services.omar_tiktok import (
            _EMPTY_RETRIES,
            _EMPTY_RETRY_SECONDS,
            _KEYWORD_GAP_SECONDS,
            OmarStop,
            map_web_item,
            next_cursor,
            omar_get,
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
        await omar_yield.add("search", requests=1)
        params = {"query": self.query["query"], "cursor": self.cursor, **mode}
        try:
            data = await omar_get(client, pacer, api_key, path, params)
        except OmarStop:
            if self.cursor != "0":
                await redis.hset(CURSOR_KEY, self._field(), self.cursor)
            raise
        except Exception as exc:  # noqa: BLE001 - trang lỗi: bỏ phần còn lại của kiểu tìm này, hôm sau tìm lại
            stats["failed"] = int(stats.get("failed") or 0) + 1
            logger.warning("omar_search_failed_page", query=self.query["query"], path=path, error=str(exc)[:200])
            self.complete = False
            await self._next_pass()
            return _KEYWORD_GAP_SECONDS if self.index < len(self.passes) else None
        results = (data or {}).get("results") or []
        if not results and self.cursor != "0":
            # Rỗng giữa chừng: thường bị giới hạn tạm thời - chờ vài phút gọi lại đúng cursor; vẫn rỗng thì để đó (giữ
            # cursor cho lần tìm sau) và sang kiểu tìm kế.
            self.empty_tries += 1
            if self.empty_tries <= _EMPTY_RETRIES:
                stats["search_empty_retries"] = int(stats.get("search_empty_retries") or 0) + 1
                return _EMPTY_RETRY_SECONDS
            await self._next_pass(keep_cursor=True)
            return _KEYWORD_GAP_SECONDS if self.index < len(self.passes) else None
        self.empty_tries = 0
        mapped = [v for v in (map_web_item(raw) for raw in results) if v]
        # Các kiểu tìm trùng nhau một phần - chỉ đếm/gửi video chưa gặp trong câu này.
        mapped = [v for v in mapped if v["video_id"] not in self.seen_ids]
        self.seen_ids.update(v["video_id"] for v in mapped)
        await publish_videos(mapped, self.query["keyword_id"], self.local, source="search")
        self.pages += 1
        following = next_cursor(data or {})
        if following is None or self.pages >= settings.omar_search_pages:
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
    """Tìm hết một câu liền một mạch (tìm ngay một câu từ tab Omar). True khi tìm xong không lỗi."""
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
    """Câu tự sinh không ra video mới settings.omar_search_prune_days lần tìm liên tiếp thì xoá (đỡ tốn ngân sách)."""
    redis = get_redis_client()
    if published:
        await redis.hdel(ZERO_KEY, query["key"])
        return
    streak = int(await redis.hincrby(ZERO_KEY, query["key"], 1))
    if streak >= settings.omar_search_prune_days:
        await delete_custom_query(query["id"])
        await redis.hdel(ZERO_KEY, query["key"])
        logger.info("omar_search_query_pruned", query=query["query"], movie=query.get("movie"))


def keep_suggestion(text: str, title_plain: str) -> bool:
    """Gợi ý đáng tìm: chứa tên phim (không dấu), không phải tìm kiếm nước ngoài, không quá dài."""
    plain = normalize_title(text)
    padded = f" {plain} "
    return (
        title_plain in plain
        and plain != title_plain
        and len(plain.split()) <= len(title_plain.split()) + 4
        and not any(marker in padded for marker in _FOREIGN)
    )


async def discover_queries(client: httpx.AsyncClient, pacer: _Pacer, api_key: str, take: Callable[[], bool]) -> int:
    """Mỗi ngày một lần: gợi ý tìm kiếm của TikTok (search_suggest) cho tên phim không dấu, và từ khoá xu hướng ở Việt Nam
    (trending_searches) chứa tên phim -> câu tìm mới (nguồn "suggest"), tối đa settings.omar_search_suggest_per_movie
    câu mỗi phim. Câu không ra video mới bị tự xoá (_track_zero)."""
    from app.services.omar_tiktok import omar_get

    redis = get_redis_client()
    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    if await redis.get(SUGGEST_DAY_KEY) == today:
        return 0
    queries = await build_queries()
    existing = {" ".join(q["query"].lower().split()) for q in queries}
    per_movie: dict[str, int] = {}
    for q in queries:
        if q["source"] == "suggest":
            per_movie[q["movie_id"]] = per_movie.get(q["movie_id"], 0) + 1
    movies = {q["movie_id"]: q["movie"] for q in queries if q["source"] == "title"}
    candidates: list[tuple[str, str]] = []
    for movie_id, title in movies.items():
        title_plain = normalize_title(title)
        if len(title_plain) < 4 or not take():
            continue
        await omar_yield.add("search", requests=1)
        try:
            data = await omar_get(client, pacer, api_key, "search_suggest", {"query": title_plain})
        except Exception as exc:  # noqa: BLE001 - gợi ý lỗi: hôm sau thử lại
            logger.warning("omar_search_suggest_failed", movie=title, error=str(exc)[:200])
            continue
        for item in (data or {}).get("suggestions") or []:
            text = str(item.get("content") or "").strip()
            if text and keep_suggestion(text, title_plain):
                candidates.append((movie_id, text))
    if take():
        await omar_yield.add("search", requests=1)
        try:
            data = await omar_get(client, pacer, api_key, "trending_searches", {"region": "VN"})
            for item in (data or {}).get("words") or []:
                word = str(item.get("trendingSearchWord") or "").strip()
                for movie_id, title in movies.items():
                    if word and len(normalize_title(title)) >= 6 and normalize_title(title) in normalize_title(word):
                        candidates.append((movie_id, word))
        except Exception as exc:  # noqa: BLE001
            logger.warning("omar_trending_searches_failed", error=str(exc)[:200])
    added = 0
    for movie_id, text in candidates:
        key = " ".join(text.lower().split())
        if key in existing or per_movie.get(movie_id, 0) >= settings.omar_search_suggest_per_movie:
            continue
        await add_custom_query(movie_id, text, source="suggest")
        existing.add(key)
        per_movie[movie_id] = per_movie.get(movie_id, 0) + 1
        added += 1
    await redis.set(SUGGEST_DAY_KEY, today, ex=3 * 24 * 3600)
    logger.info("omar_search_queries_discovered", added=added, candidates=len(candidates))
    return added


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
    from app.services.omar_tiktok import OmarStop, run_round_robin

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    redis = get_redis_client()
    if settings.omar_search_suggest:
        try:
            await discover_queries(client, pacer, api_key, lambda: not should_stop() and budget.take())
        except OmarStop:
            raise
        except Exception as exc:  # noqa: BLE001 - mở rộng câu tìm lỗi không chặn phần tìm
            logger.warning("omar_search_discover_failed", error=str(exc)[:200])
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
        except OmarStop as exc:
            stopped.append(exc)
            return None
        if delay is None and not search.halted and await search.finish(stats):
            await redis.hset(DONE_KEY, search.query["key"], today)
        return delay

    # Xen kẽ giữa các câu, mỗi lần một trang (không gọi dồn một câu) - xem omar_tiktok.run_round_robin.
    await run_round_robin([_QuerySearch(q) for q in queries], step, workers=_WORKERS, should_stop=halted)
    logger.info(
        "omar_search_processed",
        queries=len(queries),
        requests=stats["search_requests"],
        videos=stats["search_videos"],
    )
    if stopped:
        raise stopped[0]
