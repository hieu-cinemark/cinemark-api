"""Lượt cào bài theo lịch (bảng `crawl_schedules`, sửa trên dashboard): mỗi nền tảng tối đa vài giờ chạy mỗi ngày.

Mỗi giờ chạy có cờ riêng trong Redis (sống qua restart), kèm bản trong bộ nhớ phòng khi Redis trục trặc. "Đang giao" tách
khỏi "đã giao xong": cờ fired chỉ đặt khi Kafka đã nhận đủ; lease chặn chạy chồng và sau một lần hỏng thì giữ vai trò khoảng
nghỉ trước lần thử lại. Số lần thử có giới hạn tự nhiên vì slot hết hạn sau CATCH_UP_MINUTES (is_due).
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from functools import partial

from app.clients.d1 import d1_strict
from app.clients.kafka import publish_crawl_request, publish_nurture_request
from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.keywords import get_enabled_keywords
from app.schemas.settings import parse_run_times
from app.services.accounts.config_db.schedules import list_crawl_schedules, mark_crawl_schedule_triggered
from app.services.crawl import crawl_lanes
from app.services.crawl.keyword_yield import plan_crawl as plan_keyword_crawl
from app.services.crawl.scheduler.core import KEY_PREFIX, TIMEZONE, is_due, spawn

logger = get_logger(__name__)

NURTURE_PLATFORMS = {"threads", "tiktok"}
# Lượt sau trong ngày (không phải giờ đầu tiên của lịch) với FB/Threads chỉ lấy bài mới nhất - lượt đầu đã đào sâu theo độ
# liên quan; kết quả kiểu đó lặp lại giữa các lượt, còn xếp theo thời gian thì bắt được bài mới đăng trong ngày.
RECENT_LATER_SLOT_PLATFORMS = {"facebook", "threads"}

CRAWL_FIRED_PREFIX = f"{KEY_PREFIX}crawl_fired:"
CRAWL_LEASE_PREFIX = f"{KEY_PREFIX}crawl_lease:"
CRAWL_PROGRESS_PREFIX = f"{KEY_PREFIX}crawl_progress:"
_FIRED_TTL_SECONDS = 2 * 24 * 3600
_LEASE_SECONDS = 600
RETRY_BACKOFF_SECONDS = 120
crawl_fired: set[str] = set()
crawl_running: set[str] = set()


class SlotProgress:
    """Những gì một lượt theo lịch đã giao được cho Kafka (từng từ khoá, nurture, trang nguồn) - sống qua lần thử lại và qua
    restart, để lần sau chỉ gửi phần còn thiếu. Redis lỗi thì vẫn nhớ trong tiến trình."""

    def __init__(self, key: str) -> None:
        self.key = key
        self._done: set[str] = set()

    async def load(self) -> None:
        try:
            self._done |= set(await get_redis_client().smembers(f"{CRAWL_PROGRESS_PREFIX}{self.key}"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("scheduler_slot_progress_load_failed", error=exc)

    def __contains__(self, item: str) -> bool:
        return item in self._done

    def __len__(self) -> int:
        return len(self._done)

    async def add(self, item: str) -> None:
        self._done.add(item)
        try:
            client = get_redis_client()
            await client.sadd(f"{CRAWL_PROGRESS_PREFIX}{self.key}", item)
            await client.expire(f"{CRAWL_PROGRESS_PREFIX}{self.key}", _FIRED_TTL_SECONDS)
            # Còn đang giao được thì gia hạn lease: lượt dài (D1 chậm) không bị tick khác chạy chồng.
            await client.expire(f"{CRAWL_LEASE_PREFIX}{self.key}", _LEASE_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.warning("scheduler_slot_progress_save_failed", error=exc)

    def run_id(self, keyword_id: str) -> str:
        """Cùng slot + từ khoá thì cùng run_id: crash giữa lúc publish và lúc ghi tiến độ làm job gửi hai lần, spider-hub
        bỏ bản sau khi bản đầu đã chạy xong."""
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cinemark:crawl_slot:{self.key}:{keyword_id}"))


async def _deliver_once(progress: SlotProgress, item: str, publish: Callable[[], Awaitable[bool]]) -> bool:
    if item in progress:
        return True
    ok = await publish()
    if ok:
        await progress.add(item)
    return ok


async def _nurture(progress: SlotProgress, platform: str, when: str) -> bool:
    # Cùng topic Kafka và consumer theo nền tảng với crawl: publish trước thì chạy xong rồi mới tới các lượt từ khoá, publish
    # sau cùng thì chạy sau khi chúng xong hết.
    if platform not in NURTURE_PLATFORMS:
        logger.info("scheduled_nurture_skipped_platform", platform=platform, when=when)
        return True
    ok = await _deliver_once(progress, f"nurture_{when}", lambda: publish_nurture_request(platform))
    logger.info("scheduled_nurture_queued", platform=platform, when=when, ok=ok)
    return ok


async def trigger_platform(
    platform: str,
    *,
    progress: SlotProgress,
    nurture_before: bool = False,
    nurture_after: bool = False,
    recent: bool = False,
    slot_index: int = 0,
) -> bool:
    """True khi mọi thứ của lượt đã được Kafka nhận; False thì run_crawl_slot cho thử lại phần còn thiếu."""
    complete = True
    if nurture_before:
        complete = await _nurture(progress, platform, "before")

    # Facebook theo hai phần hạn mức (crawl_lanes): mỗi giờ chạy là một lượt bài mới nông; bài cũ do nhịp đào mỗi giờ lo.
    fresh_lane = platform == "facebook" and settings.facebook_crawl_lanes
    today = datetime.now(TIMEZONE).date()
    if platform == "youtube":
        await ensure_youtube_keywords()
    keywords = (
        await crawl_lanes.fresh_keywords(slot_index, today)
        if fresh_lane
        else await get_enabled_keywords(platform=platform)
    )
    if not keywords:
        logger.info("scheduled_crawl_no_keywords", platform=platform)
    else:
        published = 0
        # Facebook/Threads: tắt từ khoá toàn ra rác, giảm trang từ khoá kém (keyword_yield).
        keywords, low_pages = await plan_keyword_crawl(platform, keywords)
        if fresh_lane:
            # plan_crawl xếp theo hiệu quả từ khoá - giữ phim tháng này / tháng sau lên đầu (sắp xếp ổn định).
            keywords = crawl_lanes.sort_by_priority(keywords, today)
        if platform == "facebook":
            # 28/10/2026 (người vận hành chọn): mọi job Facebook tới max_pages của spider và chỉ dừng sớm khi nhiều trang
            # liên tiếp không có bài mới - không giảm trang cho từ khoá kém nữa.
            low_pages = {}
        for keyword in keywords:
            if platform == "threads" and keyword["keyword"].strip().startswith("#"):
                # publish_crawl_request luôn từ chối (Threads không cào hashtag) - không phải một lần giao hỏng cần thử lại.
                continue
            common = {
                "platform": platform,
                "keyword": keyword["keyword"],
                "keyword_id": keyword["id"],
                "run_id": progress.run_id(keyword["id"]),
            }
            if fresh_lane:
                max_pages = crawl_lanes.facebook_max_pages(keyword.get("released_at"), today)
                publish = partial(publish_crawl_request, **common, max_pages=max_pages, lane="fresh")
            else:
                publish = partial(
                    publish_crawl_request, **common, max_pages=low_pages.get(keyword["id"]), recent_post=recent
                )
            if await _deliver_once(progress, f"keyword:{keyword['id']}", publish):
                published += 1
            else:
                complete = False
        logger.info(
            "scheduled_crawl_triggered",
            platform=platform,
            requested=len(keywords),
            published=published,
            recent=recent,
            lane="fresh" if fresh_lane else None,
            slot_index=slot_index,
            complete=complete,
            telegram=True,
        )

    if nurture_after:
        complete = await _nurture(progress, platform, "after") and complete
    return complete


async def ensure_youtube_keywords() -> int:
    """YouTube (13/11/2026): mỗi phim đang theo dõi chưa có từ khoá YouTube nào được tạo một từ khoá = tên phim. Từ khoá
    người vận hành đã tắt thì không tạo lại (phim đó vẫn có dòng). Trả số từ khoá mới."""
    from sqlalchemy import select

    from app.repositories.d1.keywords import get_or_create_keyword
    from app.repositories.d1.sql import d1_exec
    from app.repositories.d1.tables import keywords as keywords_table
    from app.repositories.d1.tables import movies as movies_table

    has_youtube = select(keywords_table.c.movie_id).where(keywords_table.c.platform == "youtube")
    rows = await d1_exec(
        select(movies_table.c.id, movies_table.c.title).where(
            movies_table.c.enabled == 1, movies_table.c.id.not_in(has_youtube)
        ),
        quiet=True,
    )
    created = 0
    for row in rows or []:
        if row.get("title") and await get_or_create_keyword(row["id"], "youtube", str(row["title"]).strip()):
            created += 1
    if created:
        logger.info("youtube_keywords_created", created=created)
    return created


def slot_key(platform: str, today: str, slot: str) -> str:
    return f"{platform}:{today}:{slot}"


async def claim_crawl_slot(platform: str, today: str, slot: str) -> bool:
    key = slot_key(platform, today, slot)
    if key in crawl_fired or key in crawl_running:
        return False
    try:
        client = get_redis_client()
        if await client.exists(f"{CRAWL_FIRED_PREFIX}{key}"):
            return False
        if not await client.set(f"{CRAWL_LEASE_PREFIX}{key}", "1", nx=True, ex=_LEASE_SECONDS):
            return False
    except Exception as exc:  # noqa: BLE001 - không có Redis thì các tập trong bộ nhớ vẫn chặn lặp trong tiến trình này
        logger.warning("scheduler_crawl_slot_redis_failed", error=exc)
    crawl_running.add(key)
    return True


async def _finish_crawl_slot(key: str, *, complete: bool) -> None:
    crawl_running.discard(key)
    if complete:
        if len(crawl_fired) > 500:
            crawl_fired.clear()
        crawl_fired.add(key)
    try:
        client = get_redis_client()
        if complete:
            await client.set(f"{CRAWL_FIRED_PREFIX}{key}", "1", ex=_FIRED_TTL_SECONDS)
            await client.delete(f"{CRAWL_LEASE_PREFIX}{key}", f"{CRAWL_PROGRESS_PREFIX}{key}")
        else:
            await client.set(f"{CRAWL_LEASE_PREFIX}{key}", "1", ex=RETRY_BACKOFF_SECONDS)
    except Exception as exc:  # noqa: BLE001
        logger.warning("scheduler_crawl_slot_redis_failed", error=exc)


async def run_crawl_slot(platform: str, today: str, slot: str, **kwargs) -> None:
    """Giao một lượt theo lịch cho Kafka. d1_strict: D1 tạm lỗi không được giống "không có từ khoá nào" - lượt đó sẽ được coi là
    đã xong trong khi chưa gửi gì."""
    key = slot_key(platform, today, slot)
    progress = SlotProgress(key)
    await progress.load()
    complete = False
    try:
        with d1_strict():
            complete = await trigger_platform(platform, progress=progress, **kwargs)
    except Exception as exc:  # noqa: BLE001 - slot còn hạn thì lần sau thử lại phần còn thiếu
        logger.warning("scheduled_crawl_attempt_failed", platform=platform, slot=slot, error=exc)
    await _finish_crawl_slot(key, complete=complete)
    if complete:
        try:
            await mark_crawl_schedule_triggered(platform, today)
        except Exception as exc:  # noqa: BLE001 - chỉ để hiển thị, cờ fired mới là thứ chống chạy lặp
            logger.warning("scheduled_crawl_mark_failed", platform=platform, error=exc)
    elif not is_due(slot, datetime.now(TIMEZONE) + timedelta(seconds=RETRY_BACKOFF_SECONDS)):
        logger.error("scheduled_crawl_incomplete", platform=platform, slot=slot, delivered=len(progress), telegram=True)


async def crawl_tick() -> None:
    now = datetime.now(TIMEZONE)
    today = now.date().isoformat()
    for sched in await list_crawl_schedules():
        if not sched["enabled"]:
            continue
        slots = parse_run_times(sched["run_time"])
        due = [slot for slot in slots if is_due(slot, now)]
        if not due:
            continue
        platform, slot = sched["platform"], due[0]
        if not await claim_crawl_slot(platform, today, slot):
            continue
        logger.info("scheduled_crawl_firing", platform=platform, run_time=slot, fired_at=now.strftime("%H:%M"))
        # Không await tại chỗ - một lần publish Kafka hoặc query D1 chậm cho một nền tảng không được làm trễ việc kiểm tra
        # (hoặc bắn) lịch của mọi nền tảng khác trong cùng lượt.
        spawn(
            run_crawl_slot(
                platform,
                today,
                slot,
                nurture_before=bool(sched.get("nurture_before")),
                nurture_after=bool(sched.get("nurture_after")),
                recent=platform in RECENT_LATER_SLOT_PLATFORMS and len(slots) > 1 and slot != slots[0],
                slot_index=slots.index(slot),
            )
        )
