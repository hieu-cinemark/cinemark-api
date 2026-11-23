"""Tác vụ public (không tài khoản) chạy liên tục khi hàng đợi trống - phần YouTube (16/11/2026). Bật/tắt: proxy_settings
.youtube_continuous (dashboard › Settings › Proxy).

Trước đó làn YouTube của spider-hub chỉ có việc ở các khung giờ cào trong ngày (crawl_slots) và lượt bình luận mỗi giờ; hết thì
các làn ngồi không. Mỗi nhịp scheduler: khi hai hàng đợi Redis của spider-hub (crawl_youtube:search / :comments) và job
đang chạy đều trống, xếp tìm kiếm cho settings.youtube_continuous_batch phim chạy lâu nhất - mỗi phim cách lần xếp trước
ít nhất settings.youtube_continuous_min_gap_hours giờ (bài đã thấy không được gửi lại nên lặp lại chỉ tốn request).

Page Facebook và kênh Threads không đăng nhập đã nạp liên tục trong source_pages.fill_public_pages_queue /
threads_channels.fill_queue (min_gap).
"""

from __future__ import annotations

from datetime import UTC, datetime

from app.clients import crawl_queues as queues
from app.clients.kafka import publish_crawl_request
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.keywords import get_enabled_keywords
from app.services.accounts.config_db.proxy_settings import crawl_setting

logger = get_logger(__name__)

# Khoá của spider-hub (social_crawler/consumer/jobs/youtube.py), cùng prefix Redis.
YOUTUBE_QUEUE_KEYS = queues.YOUTUBE_QUEUE_KEYS
# Hash {keyword_id: thời điểm (epoch) xếp gần nhất bởi nhịp này}.
LAST_KEY = f"{REDIS_KEY_PREFIX}youtube_continuous:last"


async def youtube_idle_tick(now: datetime | None = None) -> int:
    """Xếp tìm kiếm YouTube khi làn rảnh. Trả số phim đã xếp."""
    if not await crawl_setting("youtube_continuous"):
        return 0
    redis = get_redis_client()
    for key in YOUTUBE_QUEUE_KEYS:
        if await redis.llen(key):
            return 0
    from app.services.crawl.scheduler.crawl_slots import ensure_youtube_keywords

    await ensure_youtube_keywords()
    keywords = await get_enabled_keywords(platform="youtube")
    if not keywords:
        return 0
    now_ts = (now or datetime.now(tz=UTC)).timestamp()
    last = {
        (k.decode() if isinstance(k, bytes) else k): float(v) for k, v in (await redis.hgetall(LAST_KEY) or {}).items()
    }
    min_gap = settings.youtube_continuous_min_gap_hours * 3600
    ready = sorted(
        (kw for kw in keywords if now_ts - last.get(kw["id"], 0.0) >= min_gap),
        key=lambda kw: last.get(kw["id"], 0.0),
    )
    queued = 0
    for kw in ready[: settings.youtube_continuous_batch]:
        if await publish_crawl_request(platform="youtube", keyword=kw["keyword"], keyword_id=kw["id"]):
            await redis.hset(LAST_KEY, kw["id"], str(now_ts))
            queued += 1
    if queued:
        logger.info("youtube_continuous_queued", queued=queued, ready=len(ready), keywords=len(keywords))
    return queued
