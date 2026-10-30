"""Lịch crawl Facebook theo hai phần hạn mức (27/10/2026) - thay lịch "lượt đầu quét sâu, lượt sau 'gần đây' sâu".

Đo 25-27/10/2026: lượt 05:30 (quét 4/60 ngày) dùng hết hạn mức request của tài khoản (spider-hub
services/search_budget.py) cho phần lớn là bài cũ đã thấy - 765 bài mới nhưng chỉ 3 bài dưới 6 giờ tuổi; lượt "gần đây"
12:30 không còn request nào. Giờ hạn mức mỗi tài khoản chia hai phần cố định (spider-hub FACEBOOK_FRESH_SHARE):

- "fresh": mỗi giờ chạy của lịch Facebook (crawl_schedules) là một lượt tìm kiếm thường (không lọc "gần đây", 28/10/2026)
  (tới 100 trang, dừng sau 10 trang liên tiếp không có bài mới - 28/10/2026) - chỉ phim đang chiếu (mọi lượt) và sắp
  chiếu (lượt chẵn). Phim cũ không cần.
- "deep" (đào bài cũ): mỗi giờ trong settings.facebook_deep_hours xếp số job theo số tài khoản dùng được (deep_jobs_this_hour); spider-hub
  (services/deep_cursor.py) đào tiếp từ con trỏ của từ khoá lùi về ngày chiếu - 12 tháng. Ưu tiên phim ra rạp tháng
  này / tháng sau (30/10/2026), rồi đang chiếu, sắp chiếu, phim cũ; trong cùng nhóm, từ khoá lâu chưa được xếp nhất trước.
Trần request mỗi giờ của tài khoản (spider-hub FACEBOOK_HOURLY_REQUEST_CAP) rải cả hai phần ra cả ngày.
"""

from __future__ import annotations

import json
import math
import time
import zlib
from datetime import date, datetime
from typing import Any

from app.clients.kafka import publish_crawl_request
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.store import d1_query
from app.services.accounts import platform_config_db

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


# Số trang tối đa mỗi job Facebook theo giai đoạn phim (28/10/2026, người vận hành chọn): phim chưa chiếu ít bài hơn
# hẳn nên 50 trang là đủ; đã chiếu (hoặc chưa có ngày chiếu) 100. Spider vẫn dừng sớm sau 10 trang liên tiếp không có bài
# mới (spider-hub FACEBOOK_NO_NEW_PAGES).
UPCOMING_MAX_PAGES = 50
RELEASED_MAX_PAGES = 100


def focus_window(today: date) -> tuple[date, date]:
    """[ngày 1 tháng này, ngày cuối tháng sau] - phim ra rạp trong khoảng này được ưu tiên (30/10/2026, người vận hành
    chọn): lượt bài mới cào chúng ở MỌI lượt và xếp trước, lượt đào xếp chúng đầu hàng."""
    start = today.replace(day=1)
    after_next = date(start.year + (start.month + 1) // 12, (start.month + 1) % 12 + 1, 1)
    return start, date.fromordinal(after_next.toordinal() - 1)


def is_focus(released_at: str | None, today: date) -> bool:
    try:
        released = date.fromisoformat(str(released_at or "")[:10])
    except ValueError:
        return False
    start, end = focus_window(today)
    return start <= released <= end


def priority_of(released_at: str | None, today: date) -> int:
    """Thứ tự xếp hàng: phim tháng này / tháng sau trước, rồi đang chiếu, sắp chiếu xa hơn, phim cũ, chưa có ngày."""
    return -1 if is_focus(released_at, today) else STAGE_ORDER[stage_of(released_at, today)]


def facebook_max_pages(released_at: str | None, today: date) -> int:
    return UPCOMING_MAX_PAGES if stage_of(released_at, today) == "upcoming" else RELEASED_MAX_PAGES


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
    """Từ khoá cho lượt bài mới thứ `slot_index` trong ngày: phim ra rạp tháng này / tháng sau (is_focus) mọi lượt, phim
    đang chiếu khác và phim sắp chiếu xa hơn lượt chẵn (30/10/2026; trước đó phim đang chiếu mọi lượt). Phim tháng này /
    tháng sau xếp đầu - chỗ gọi giữ thứ tự đó sau plan_crawl (sort_by_priority)."""
    out = []
    for keyword in await _facebook_keywords():
        released_at = keyword.get("released_at")
        stage = stage_of(released_at, today)
        if is_focus(released_at, today) or (stage in ("showing", "upcoming") and slot_index % 2 == 0):
            out.append(keyword)
    return sort_by_priority(out, today)


def sort_by_priority(keywords: list[dict[str, Any]], today: date) -> list[dict[str, Any]]:
    """Sắp xếp ổn định theo priority_of - trong cùng nhóm giữ nguyên thứ tự cũ (vd. theo hiệu quả từ khoá)."""
    return sorted(keywords, key=lambda keyword: priority_of(keyword.get("released_at"), today))


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
    if not settings.facebook_crawl_lanes or settings.facebook_deep_jobs_per_account_hour <= 0:
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
        candidates.append((priority_of(keyword.get("released_at"), today), queued_at, keyword))
    candidates.sort(key=lambda item: (item[0], item[1]))

    jobs = await deep_jobs_this_hour()
    queued = 0
    for _, _, keyword in candidates[:jobs]:
        ok = await publish_crawl_request(
            platform="facebook",
            keyword=keyword["keyword"],
            keyword_id=keyword["id"],
            max_pages=facebook_max_pages(keyword.get("released_at"), today),
            lane="deep",
            released_at=str(keyword.get("released_at") or "")[:10] or None,
        )
        if ok:
            queued += 1
            await redis.hset(DEEP_LAST_QUEUED_KEY, str(keyword["id"]), str(time.time()))
    logger.info("deep_crawl_queued", queued=queued, jobs=jobs, candidates=len(candidates), keywords=len(keywords))
    return queued


async def deep_jobs_this_hour() -> int:
    """Số job đào của giờ này theo số tài khoản FB dùng được - thêm tài khoản là đào nhanh hơn, không cần sửa cấu hình."""
    try:
        accounts = await platform_config_db.count_crawl_ready_accounts("facebook")
    except Exception as exc:  # noqa: BLE001 - không đếm được (DB chập chờn) thì vẫn xếp mức tối thiểu
        logger.warning("deep_account_count_failed", error=str(exc)[:200])
        accounts = 1
    jobs = math.ceil(max(accounts, 1) * settings.facebook_deep_jobs_per_account_hour)
    return max(1, min(jobs, settings.facebook_deep_jobs_max_per_hour))
