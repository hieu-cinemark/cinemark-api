"""Lấy tin sau công chiếu mỗi 72 giờ và lưu D1. Worker đọc/phân tích trực tiếp từ DB."""

import asyncio
import uuid
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from app.clients.d1 import d1_query, d1_strict
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.errors import ConflictError, NotFoundError, UpstreamError, ValidationError
from app.core.logging import get_logger
from app.repositories.d1 import press_reviews
from app.schemas.research import GoogleNewsRequest
from app.services import google_news, news_articles, press_groups

logger = get_logger(__name__)
INTERVAL = timedelta(days=3)
PREFIX = REDIS_KEY_PREFIX + "post_release_news:v2:"
UNLOCK = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"


def now() -> datetime:
    return datetime.now(UTC)


def released(value: str | None) -> bool:
    try:
        return bool(value) and date.fromisoformat(value[:10]) <= now().astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
    except ValueError, TypeError:
        return False


async def movie_for_news(movie_id: str) -> dict:
    with d1_strict():
        rows = await d1_query("SELECT id, title, released_at, enabled FROM movies WHERE id = ? LIMIT 1", [movie_id])
    if not rows:
        raise NotFoundError("Không tìm thấy phim.")
    movie = rows[0]
    if not movie.get("enabled") or not released(movie.get("released_at")):
        raise ValidationError("Chỉ xem tin với phim đã khởi chiếu và đang được theo dõi.")
    return movie


async def cached(movie_id: str) -> dict | None:
    return await press_reviews.load(movie_id)


async def read_news(movie_id: str) -> dict | None:
    await movie_for_news(movie_id)
    await press_reviews.ensure_table()
    return await cached(movie_id)


async def refresh_news(movie_id: str, *, force: bool = False) -> dict:
    movie = await movie_for_news(movie_id)
    await press_reviews.ensure_table()
    cache = get_redis_client()
    owner = uuid.uuid4().hex
    lock = PREFIX + movie_id + ":lock"
    if not await cache.set(lock, owner, nx=True, ex=600):
        old = await cached(movie_id)
        if old:
            return old
        raise ConflictError("Đang tải tin cho phim. Vui lòng thử lại sau.")
    old = None
    try:
        old = await cached(movie_id)
        if (
            not force
            and old
            and old["data"].get("groups_version") == 2
            and datetime.fromisoformat(old["nextRefreshAt"]) > now()
        ):
            if old["data"].get("extraction_version") != 1:
                previous_id = old["snapshotId"]
                old["data"]["sources"] = old["data"].get("sources", [])[: news_articles.MAX_ARTICLES]
                await news_articles.enrich(old["data"]["sources"])
                press_groups.sync_enriched(old["data"])
                old["data"]["analysis_input"] = news_articles.analysis_input(movie["title"], old["data"]["sources"])
                old["data"]["extraction_version"] = 1
                old["snapshotId"] = uuid.uuid4().hex
                old["analysis"] = None
                await press_reviews.update_extracted(old, previous_id)
                return await cached(movie_id) or old
            return old
        if not force and await cache.get(PREFIX + movie_id + ":retry"):
            if old:
                return old
            raise UpstreamError("Lượt tìm tin trước chưa thành công, vui lòng thử lại sau.")
        # Giãn retry khi quota/mạng lỗi, không gọi lại ở mỗi nhịp scheduler.
        await cache.set(PREFIX + movie_id + ":retry", "1", ex=3600)
        query = f"phim {movie['title']} đánh giá"
        news_query = f"phim {movie['title']} review"
        data, press = await asyncio.gather(
            google_news.search_web(GoogleNewsRequest(q=query, no_cache=force)),
            google_news.search(GoogleNewsRequest(q=news_query, no_cache=force)),
        )
        data["collection_queries"] = [query, news_query]
        press_groups.select_groups(data, press)
        await news_articles.enrich(data["sources"])
        press_groups.sync_enriched(data)
        data["analysis_input"] = news_articles.analysis_input(movie["title"], data["sources"])
        data["extraction_version"] = 1
        snapshot = now()
        result = {
            "movieId": movie_id,
            "snapshotId": uuid.uuid4().hex,
            "generatedAt": snapshot.isoformat(),
            "nextRefreshAt": (snapshot + INTERVAL).isoformat(),
            "data": data,
            "analysis": None,
        }
        # Một snapshot mới vô hiệu hoá phân tích của snapshot cũ.
        await press_reviews.save(result)
        await cache.delete(PREFIX + movie_id + ":retry")
        return result
    except Exception:  # lỗi tìm tin giữ snapshot cũ để vẫn xem được
        if old:
            return old
        raise
    finally:
        await cache.eval(UNLOCK, 1, lock, owner)


async def tick() -> None:
    if not settings.serpapi_api_key:
        return
    cache = get_redis_client()
    if not await cache.set(PREFIX + "schedule_scan:hybrid_search:v2", "1", nx=True, ex=3600):
        return
    with d1_strict():
        movies = await d1_query("SELECT id, released_at FROM movies WHERE enabled = 1 AND released_at IS NOT NULL")
    for movie in movies or []:
        if not released(movie.get("released_at")):
            continue
        try:
            await refresh_news(movie["id"])
        except Exception:  # noqa: BLE001 - một phim lỗi không chặn phim còn lại
            logger.warning("post_release_news_refresh_failed", movie_id=movie["id"])


_task: asyncio.Task | None = None


def start_tick() -> None:
    global _task

    async def run() -> None:
        try:
            await tick()
        except Exception:  # noqa: BLE001
            logger.warning("post_release_news_tick_failed")

    if _task is None or _task.done():
        _task = asyncio.create_task(run())


def stop() -> None:
    if _task is not None:
        _task.cancel()
