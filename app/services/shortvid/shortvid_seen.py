"""ShortVid: dấu video đã thấy (chống cào lặp) và đếm hashtag đi kèm.

Tách từ shortvid_tiktok.py (17/11/2026); shortvid_tiktok.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.store import SHORTVID_COOCCUR_KEY_TMPL

logger = get_logger(__name__)


_SEEN_PREFIX = f"{REDIS_KEY_PREFIX}shortvid:seen:"
# Set video TikTok đã gửi lên raw_posts của spider-hub (constants/tiktok.py SEEN_POSTS_KEY, cùng prefix) - dùng chung để
# spider không gửi lại video ShortVid đã gửi và ngược lại.
_SPIDER_SEEN_VIDEOS_KEY = f"{REDIS_KEY_PREFIX}tiktok:seen_video_ids"


async def _claim_unseen(video_ids: list[str], brand_new: set[str] | None = None) -> set[str]:
    """Video cần gửi lên raw_posts: video MỚI (chưa có trong set đã gửi dùng chung với spider) hoặc video cũ đã quá
    settings.shortvid_republish_hours giờ chưa gửi lại (cập nhật tương tác). Đánh dấu luôn những video đó. Với gói lớn (~1 triệu
    video/ngày, phần lớn trùng) gửi lại mọi video làm ingest/D1 quá tải. Redis lỗi thì coi như mới hết (thà ghi trùng còn
    hơn mất bài)."""
    if not video_ids:
        return set()
    ttl = int(settings.shortvid_republish_hours * 3600)
    try:
        pipe = get_redis_client().pipeline()
        for video_id in video_ids:
            pipe.sadd(_SPIDER_SEEN_VIDEOS_KEY, video_id)
            if ttl > 0:
                pipe.set(f"{_SEEN_PREFIX}{video_id}", "1", nx=True, ex=ttl)
        results = await pipe.execute()
    except Exception as exc:  # noqa: BLE001
        logger.warning("shortvid_seen_check_failed", error=str(exc)[:200])
        return set(video_ids)
    step = 2 if ttl > 0 else 1
    out = set()
    for i, video_id in enumerate(video_ids):
        is_new = bool(results[i * step])
        refresh_due = bool(results[i * step + 1]) if ttl > 0 else True
        if is_new or refresh_due:
            out.add(video_id)
        if is_new and brand_new is not None:
            brand_new.add(video_id)  # lần đầu thấy (chưa có trong tập chống trùng dùng chung với spider)
    return out


async def _forget_seen(video_id: str) -> None:
    """Gửi Kafka lỗi: bỏ đánh dấu để lần sau gửi lại."""
    try:
        pipe = get_redis_client().pipeline()
        pipe.srem(_SPIDER_SEEN_VIDEOS_KEY, video_id)
        pipe.delete(f"{_SEEN_PREFIX}{video_id}")
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001
        logger.warning("shortvid_seen_forget_failed", error=str(exc)[:200])


# BFS: hashtag đi kèm trong video ShortVid lấy về, đếm theo từ khoá (hash tag -> số video), giữ 14 ngày. d1.related_hashtags_for_keywords
# gộp với hashtag liên quan của spider - cùng một nguồn cho chip duyệt trên dashboard và cho AI duyệt (hashtag_discovery).
_COOCCUR_TTL_SECONDS = 14 * 24 * 3600


async def _count_cooccur(keyword: str, keyword_id: str, videos: list[dict[str, Any]]) -> None:
    """Cộng số video có mỗi hashtag đi kèm (khác chính hashtag đang cào). Giữ nguyên dấu, chỉ hạ chữ thường: TikTok không
    phân biệt hoa thường nhưng #trạibuônngười và #traibuonnguoi là hai hashtag khác nhau."""
    own = keyword.lstrip("#").strip().lower()
    counts: dict[str, int] = {}
    for video in videos:
        for tag in {str(t).strip().lower() for t in video.get("hashtags") or [] if str(t).strip()}:
            if tag != own and 3 <= len(tag) <= 60:
                counts[tag] = counts.get(tag, 0) + 1
    if not counts:
        return
    key = SHORTVID_COOCCUR_KEY_TMPL.format(keyword_id=keyword_id)
    try:
        pipe = get_redis_client().pipeline()
        for tag, n in counts.items():
            pipe.hincrby(key, tag, n)
        pipe.expire(key, _COOCCUR_TTL_SECONDS)
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001 - thiếu số đếm chỉ làm BFS chậm hơn
        logger.warning("shortvid_cooccur_failed", error=str(exc)[:200])
