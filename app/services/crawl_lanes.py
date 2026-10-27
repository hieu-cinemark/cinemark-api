"""Lịch crawl Facebook theo hai phần hạn mức (27/10/2026) - thay lịch "lượt đầu quét sâu, lượt sau 'gần đây' sâu".

Đo 25-27/10/2026: lượt 05:30 (quét 4/60 ngày) dùng hết hạn mức request của tài khoản (spider-hub
services/search_budget.py) cho phần lớn là bài cũ đã thấy - 765 bài mới nhưng chỉ 3 bài dưới 6 giờ tuổi; lượt "gần đây"
12:30 không còn request nào. Giờ hạn mức mỗi tài khoản chia hai phần cố định (spider-hub FACEBOOK_FRESH_SHARE):

- "fresh" (bài mới): mỗi giờ chạy của lịch Facebook (crawl_schedules) là một lượt "Bài viết gần đây" nông
  (settings.facebook_fresh_pages trang) - chỉ phim đang chiếu (mọi lượt) và sắp chiếu (lượt chẵn). Phim cũ không cần.
- "deep" (đào bài cũ): mỗi giờ trong settings.facebook_deep_hours xếp settings.facebook_deep_jobs_per_hour job; spider-hub
  (services/deep_cursor.py) đào tiếp từ con trỏ của từ khoá lùi về ngày chiếu - 12 tháng. Ưu tiên phim đang chiếu, rồi
  sắp chiếu, rồi phim cũ; trong cùng nhóm, từ khoá lâu chưa được xếp nhất trước.
Trần request mỗi giờ của tài khoản (spider-hub FACEBOOK_HOURLY_REQUEST_CAP) rải cả hai phần ra cả ngày.
"""

from __future__ import annotations

import json
import time
import zlib
from datetime import date, datetime
from typing import Any

from app.clients.kafka import publish_crawl_request
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.d1 import d1_query

logger = get_logger(__name__)

# Phim "đang chiếu": công chiếu trong chừng này ngày gần đây (giống ACTIVE_AFTER_DAYS của query_discovery).
SHOWING_DAYS = 45
STAGE_ORDER = {"showing": 0, "upcoming": 1, "older": 2, "unknown": 3}
# Cùng format với spider-hub services/deep_cursor.py (_KEY, RESWEEP_EVERY_DAYS) - đổi bên đó thì sửa cả ở đây.
DEEP_STATE_KEY = f"{REDIS_KEY_PREFIX}fb_deep:{{keyword_id}}"
RESWEEP_EVERY_DAYS = 7
DEEP_LAST_QUEUED_KEY = f"{REDIS_KEY_PREFIX}fb_deep:last_queued"
# Từ khoá vừa xếp job đào thì chờ chừng này mới xếp lại (job cũ có thể còn trong hàng đợi).
DEEP_REQUEUE_AFTER_SECONDS = 3 * 3600
_DEEP_FIRED_KEY = f"{REDIS_KEY_PREFIX}scheduler:deep_fired:{{hour}}"
# Nhịp đào chạy từ phút này mỗi giờ - lệch khỏi lượt bài mới (đầu giờ) để hai lượt không xếp hàng cùng lúc.
DEEP_TICK_MINUTE = 20


def stage_of(released_at: str | None, today: date) -> str:
    try:
        released = date.fromisoformat(str(released_at or "")[:10])
    except ValueError:
        return "unknown"
    if released > today:
        return "upcoming"
    return "showing" if (today - released).days <= SHOWING_DAYS else "older"


async def _facebook_keywords() -> list[dict[str, Any]]:
    rows = await d1_query(
        """
        SELECT k.id, k.movie_id, k.platform, k.keyword, m.released_at
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.platform = 'facebook' AND k.enabled = 1 AND m.enabled = 1
        """,
        [],
    )
    return rows or []


async def fresh_keywords(slot_index: int, today: date) -> list[dict[str, Any]]:
    """Từ khoá cho lượt bài mới thứ `slot_index` trong ngày: phim đang chiếu mọi lượt, phim sắp chiếu lượt chẵn."""
    out = []
    for keyword in await _facebook_keywords():
        stage = stage_of(keyword.get("released_at"), today)
        if stage == "showing" or (stage == "upcoming" and slot_index % 2 == 0):
            out.append(keyword)
    return out


def _resweep_due(state: dict[str, Any], keyword_id: str, today: date) -> bool:
    """Giống deep_cursor.resweep_due của spider-hub."""
    last = state.get("resweep")
    if last:
        return (today - date.fromisoformat(last)).days >= RESWEEP_EVERY_DAYS
    return (today.toordinal() + zlib.crc32(keyword_id.encode())) % RESWEEP_EVERY_DAYS == 0


def _in_deep_hours(now: datetime) -> bool:
    start, _, end = settings.facebook_deep_hours.partition("-")
    try:
        return int(start) <= now.hour <= int(end)
    except ValueError:
        return False


async def run_deep_tick(now: datetime) -> int:
    """Xếp job đào bài cũ của giờ này (mỗi giờ một lần); trả số job đã xếp."""
    if not settings.facebook_crawl_lanes or settings.facebook_deep_jobs_per_hour <= 0:
        return 0
    if now.minute < DEEP_TICK_MINUTE or not _in_deep_hours(now):
        return 0
    redis = get_redis_client()
    hour_key = _DEEP_FIRED_KEY.format(hour=now.strftime("%Y-%m-%dT%H"))
    if not await redis.set(hour_key, "1", nx=True, ex=2 * 3600):
        return 0

    today = now.date()
    keywords = await _facebook_keywords()
    if not keywords:
        return 0
    raw_states = await redis.mget([DEEP_STATE_KEY.format(keyword_id=k["id"]) for k in keywords])
    last_queued = await redis.hgetall(DEEP_LAST_QUEUED_KEY)
    cutoff = time.time() - DEEP_REQUEUE_AFTER_SECONDS

    candidates = []
    for keyword, raw in zip(keywords, raw_states, strict=True):
        keyword_id = str(keyword["id"])
        queued_at = float(last_queued.get(keyword_id) or 0)
        if queued_at > cutoff:
            continue
        try:
            state = json.loads(raw) if raw else {}
        except ValueError:
            state = {}
        if state.get("done") and not _resweep_due(state, keyword_id, today):
            continue
        stage = stage_of(keyword.get("released_at"), today)
        candidates.append((STAGE_ORDER[stage], queued_at, keyword))
    candidates.sort(key=lambda item: (item[0], item[1]))

    queued = 0
    for _, _, keyword in candidates[: settings.facebook_deep_jobs_per_hour]:
        ok = await publish_crawl_request(
            platform="facebook",
            keyword=keyword["keyword"],
            keyword_id=keyword["id"],
            lane="deep",
            released_at=str(keyword.get("released_at") or "")[:10] or None,
        )
        if ok:
            queued += 1
            await redis.hset(DEEP_LAST_QUEUED_KEY, str(keyword["id"]), str(time.time()))
    logger.info("deep_crawl_queued", queued=queued, candidates=len(candidates), keywords=len(keywords))
    return queued


def fresh_pages(planned: int | None) -> int:
    """Số trang của lượt bài mới: settings.facebook_fresh_pages, nhỏ hơn nếu keyword_yield đã giảm trang từ khoá kém."""
    return min(planned or settings.facebook_fresh_pages, settings.facebook_fresh_pages)

