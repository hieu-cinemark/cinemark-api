"""Quy mô hashtag TikTok qua Omar `search_hashtags`: tìm hashtag liên quan cho người vận hành (tab Omar) và chụp tổng video /
lượt xem mỗi ngày để Social Topic vẽ biểu đồ tăng trưởng (bảng tiktok_hashtag_stats).

Một lần gọi `search_hashtags` trả cả họ hashtag có tên chứa từ khoá; tổng của hashtag đang theo dõi lấy ở mục có tên KHỚP
CHÍNH XÁC (không phân biệt hoa thường), các mục còn lại là biến thể để gợi ý."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from app.clients.omar import OmarStop
from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1 import hashtag_stats
from app.repositories.d1.keywords import get_enabled_keywords
from app.services.accounts.config_db.omar import get_omar_api_settings
from app.services.omar.omar_budget import Pacer
from app.services.omar.omar_fetch import search_hashtags
from app.services.omar.omar_pause import get_pause, log_stop

logger = get_logger(__name__)

SEARCH_TIMEOUT_SECONDS = 20.0
# Chụp một lần mỗi ngày từ giờ này (giờ Việt Nam) - sau lượt cào đêm để số liệu của ngày mới đã ổn định.
SNAPSHOT_FROM_HOUR = 6
_DAY_KEY = "omar_hashtag_stats:day"


def clean_name(value: str) -> str:
    """Tên hashtag người dùng gõ -> dạng TikTok dùng: bỏ "#" và khoảng trắng."""
    return value.strip().lstrip("#").replace(" ", "")


def split_matches(query: str, results: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """(mục khớp chính xác tên, các biến thể còn lại xếp theo số video giảm dần)."""
    wanted = clean_name(query).casefold()
    exact = next((item for item in results if item["name"].casefold() == wanted), None)
    related = sorted((item for item in results if item is not exact), key=lambda item: -item["video_count"])
    return exact, related


async def _api_key() -> str:
    row = await get_omar_api_settings()
    return ((row or {}).get("api_key") or "").strip()


async def search(query: str) -> dict[str, Any] | None:
    """Tìm hashtag liên quan cho tab Omar: {query, exact, related}; None khi chưa có key Omar.

    `tracked` đánh dấu hashtag đang là từ khoá TikTok của một phim (để giao diện không gợi ý thêm lại)."""
    api_key = await _api_key()
    if not api_key:
        return None
    name = clean_name(query)
    async with httpx.AsyncClient(timeout=httpx.Timeout(SEARCH_TIMEOUT_SECONDS)) as client:
        results = await search_hashtags(client, Pacer(settings.omar_requests_per_minute), api_key, name)
    tracked = {str(k["keyword"]).lstrip("#").casefold() for k in await get_enabled_keywords(platform="tiktok")}
    exact, related = split_matches(name, results)
    mark = lambda item: {**item, "tracked": item["name"].casefold() in tracked}
    return {"query": name, "exact": mark(exact) if exact else None, "related": [mark(item) for item in related]}


async def snapshot_all(now: datetime | None = None) -> dict[str, int]:
    """Chụp tổng video / lượt xem của mọi hashtag TikTok đang theo dõi (mỗi hashtag một request). Hashtag không có mục khớp
    chính xác thì bỏ qua; dừng cả lượt khi Omar báo hết hạn mức hoặc key sai."""
    from app.services.crawl.scheduler.core import TIMEZONE

    api_key = await _api_key()
    stats = {"hashtags": 0, "saved": 0, "missing": 0, "failed": 0}
    if not api_key:
        return stats
    moment = (now or datetime.now(tz=UTC)).astimezone(TIMEZONE)
    day, captured_at = moment.date().isoformat(), datetime.now(tz=UTC).isoformat()
    keywords = [k for k in await get_enabled_keywords(platform="tiktok") if str(k["keyword"]).startswith("#")]
    stats["hashtags"] = len(keywords)
    pacer = Pacer(settings.omar_requests_per_minute)
    async with httpx.AsyncClient(timeout=httpx.Timeout(SEARCH_TIMEOUT_SECONDS)) as client:
        for kw in keywords:
            name = clean_name(str(kw["keyword"]))
            try:
                results = await search_hashtags(client, pacer, api_key, name)
            except OmarStop as exc:
                log_stop(exc)
                break
            except Exception as exc:  # noqa: BLE001 - một hashtag lỗi không chặn các hashtag còn lại
                stats["failed"] += 1
                logger.warning("omar_hashtag_stats_failed", hashtag=kw["keyword"], error=exc)
                continue
            exact, _ = split_matches(name, results)
            if exact is None:
                stats["missing"] += 1
                continue
            await hashtag_stats.upsert_snapshot(
                day=day,
                keyword_id=kw["id"],
                movie_id=kw["movie_id"],
                hashtag=exact["name"],
                hashtag_id=exact["hashtag_id"],
                video_count=exact["video_count"],
                view_count=exact["view_count"],
                captured_at=captured_at,
            )
            stats["saved"] += 1
    logger.info("omar_hashtag_stats_finished", **stats)
    return stats


async def snapshot_due(now: datetime) -> bool:
    """Đến giờ chụp của hôm nay và chưa chụp: dùng khoá Redis theo ngày nên restart không chụp lặp. Đang tạm dừng vì hạn mức
    hoặc key thì bỏ qua (không gọi Omar thêm)."""
    if now.hour < SNAPSHOT_FROM_HOUR:
        return False
    redis = get_redis_client()
    day = now.date().isoformat()
    if await redis.get(_DAY_KEY) == day:
        return False
    if await get_pause():
        return False
    await redis.set(_DAY_KEY, day, ex=2 * 24 * 3600)
    return True
