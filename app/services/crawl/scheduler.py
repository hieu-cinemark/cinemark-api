"""Bộ lập lịch crawl hằng ngày chạy trong tiến trình - bản thay thế của dashboard cho cả
scripts/trigger_scheduled_crawl.sh chạy bằng crontab hệ điều hành của cinemark-api
(nhịp cố định "mỗi 6 giờ", chỉ đổi được bằng cách sửa crontab trên máy) lẫn
Cloudflare Cron Triggers của cinemark-scraper cho Threads/TikTok (wrangler.toml, phải
deploy lại Worker mới đổi được). Từ giờ, đây là nguồn sự thật duy nhất về lúc nào
crawl của một nền tảng chạy - sửa được từ thẻ "Crawl schedule" trên dashboard (xem
CRUD crawl_schedules trong app/services/accounts/platform_config_db.py), không cần deploy
crontab/Worker để đổi.

Một asyncio task sống suốt đời tiến trình (bắt đầu/dừng từ hook startup/shutdown của
app/main.py), thức dậy mỗi _POLL_INTERVAL_SECONDS để so run_time ("HH:MM", giờ cố
định Asia/Ho_Chi_Minh - xem comment của cột đó trong scripts/dev_db_schema.sql của
spider-hub) của mọi dòng crawl_schedules đang bật với phút hiện tại. last_triggered_date
là cờ chống chạy lặp: không có nó, một vòng lặp kiểm tra mỗi 30s sẽ bắn cùng một lượt
theo lịch có khi cả chục lần trong một phút mà run_time khớp.

Dùng lại đúng các lời gọi get_enabled_keywords + publish_crawl_request mà route POST
/<platform>/run trong app/api/routes/platform_scraper.py dùng cho "mọi từ khoá đang
bật" - một lượt chạy theo lịch và một lần bấm nút "chạy tất cả" là cùng một thao tác,
chỉ khác cách kích hoạt.

_comments_tick bên dưới chạy crawl comment MỖI GIỜ cho các nền tảng có comment_crawl_schedules đang bật (từ
2026-10-08; trước đó là một lượt mỗi ngày, top 100 theo tương tác của từng từ khoá, mỗi bài chỉ một lần): theo dõi
bài nóng của từng phim mỗi giờ (top_n của lịch = số bài nóng mỗi phim), và thêm lượt mẫu phân tầng ở run_time và
run_time + 12 giờ - xem app/services/crawl/comment_planner.py."""

from __future__ import annotations

import asyncio
import time
import zlib
from datetime import datetime
from zoneinfo import ZoneInfo

from app.clients.kafka import (
    publish_cookie_check_request,
    publish_crawl_request,
    publish_nurture_request,
)
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.store import get_enabled_keywords
from app.schemas.settings import parse_run_times
from app.services.accounts import platform_config_db as db
from app.services.accounts.auto_login import resolve_auto_login_settings, run_auto_login_tick
from app.services.crawl import channel_profiles, crawl_lanes, priority_channels, tracked_posts
from app.services.crawl.comment_planner import run_comment_round
from app.services.crawl.keyword_yield import plan_crawl as plan_keyword_crawl
from app.services.crawl.platforms import COMMENT_CRAWL_PLATFORMS
from app.services.crawl.source_pages import queue_enabled_sources as queue_source_pages
from app.services.crawl.source_pages import review_suggestions as review_source_pages
from app.services.discovery.hashtag_discovery import run_discovery
from app.services.discovery.query_discovery import run_query_discovery
from app.services.omar import omar_comments, omar_trending
from app.services.omar.omar_tiktok import claim_round as claim_omar_round
from app.services.omar.omar_tiktok import clear_pause as clear_omar_pause
from app.services.omar.omar_tiktok import get_pause as get_omar_pause
from app.services.omar.omar_tiktok import is_running as omar_is_running
from app.services.omar.omar_tiktok import pause_over as omar_pause_over
from app.services.omar.omar_tiktok import run_omar_round_tracked
from app.services.ops import ops_alerts
from app.services.relevance.cleanup import resolve_cleanup_settings, run_purge
from app.services.stats import comment_rollup
from app.services.stats.stats_summary import reconcile_recent_days

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
_POLL_INTERVAL_SECONDS = 30
# Một lượt không bao giờ được làm treo vòng lặp: ngày 2026-09-30 một socket Postgres mở
# từ mạng trước đã làm treo lượt đó (và mọi lịch sau nó) suốt một đêm trong khi API vẫn
# trả lời /health.
_TICK_TIMEOUT_SECONDS = 25
# Một lượt bị lỡ vì vòng lặp bị kẹt hoặc máy đang ngủ vẫn được bắn nếu vòng lặp hồi
# phục trong khoảng thời gian này sau run_time của nó.
_CATCH_UP_MINUTES = 30


def _is_due(run_time: str, now: datetime) -> bool:
    try:
        hour, minute = (int(part) for part in run_time.split(":", 1))
    except ValueError:
        return False
    late = (now.hour * 60 + now.minute) - (hour * 60 + minute)
    return 0 <= late <= _CATCH_UP_MINUTES


_task: asyncio.Task[None] | None = None
# Giữ tham chiếu mạnh tới các task bắn-rồi-quên - event loop chỉ giữ tham chiếu yếu,
# nên task không ai tham chiếu có thể bị garbage-collect giữa chừng.
_background_tasks: set[asyncio.Task] = set()


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


NURTURE_PLATFORMS = {"facebook", "threads", "tiktok"}


# Lượt cào sau trong ngày (không phải giờ đầu tiên của lịch) với FB/Threads chỉ lấy bài mới nhất ("Bài viết gần đây" /
# tab "Gần đây") - lượt đầu đã đào sâu theo độ liên quan; kết quả kiểu đó lặp lại giữa các lượt (2026-10-16), còn xếp
# theo thời gian thì ra tập bài khác và bắt được bài mới đăng trong ngày.
RECENT_LATER_SLOT_PLATFORMS = {"facebook", "threads"}


async def _trigger_platform(
    platform: str,
    *,
    nurture_before: bool = False,
    nurture_after: bool = False,
    recent: bool = False,
    slot_index: int = 0,
) -> None:
    # Cùng topic Kafka và consumer theo nền tảng với crawl, nên nurture publish trước thì
    # thực sự chạy xong rồi mới tới các lượt crawl từ khoá phía sau; publish sau cùng thì
    # nó chạy sau khi chúng xong hết.
    if nurture_before and platform in NURTURE_PLATFORMS:
        ok = await publish_nurture_request(platform)
        logger.info("scheduled_nurture_queued", platform=platform, when="before", ok=ok)
    elif nurture_before:
        logger.info("scheduled_nurture_skipped_platform", platform=platform, when="before")

    # Facebook theo hai phần hạn mức (app/services/crawl/crawl_lanes.py): mỗi giờ chạy là một lượt bài mới nông; bài cũ do
    # nhịp đào mỗi giờ (crawl_lanes.run_deep_tick) lo.
    fresh_lane = platform == "facebook" and settings.facebook_crawl_lanes
    today = datetime.now(TIMEZONE).date()
    if fresh_lane:
        keywords = await crawl_lanes.fresh_keywords(slot_index, today)
    else:
        keywords = await get_enabled_keywords(platform=platform)
    if not keywords:
        logger.info("scheduled_crawl_no_keywords", platform=platform)
    else:
        published = 0
        # Facebook/Threads: tắt từ khoá toàn ra rác, giảm trang từ khoá kém (app/services/crawl/keyword_yield.py).
        keywords, low_pages = await plan_keyword_crawl(platform, keywords)
        if fresh_lane:
            # plan_crawl xếp theo hiệu quả từ khoá - giữ phim tháng này / tháng sau lên đầu (sắp xếp ổn định).
            keywords = crawl_lanes.sort_by_priority(keywords, today)
        if platform == "facebook":
            # 28/10/2026 (người vận hành chọn): mọi job Facebook tới FACEBOOK max_pages của spider (100) và chỉ dừng sớm khi
            # FACEBOOK_NO_NEW_PAGES (10) trang liên tiếp không có bài mới - không giảm trang cho từ khoá kém nữa.
            low_pages = {}
        for keyword in keywords:
            if fresh_lane:
                ok = await publish_crawl_request(
                    platform=platform,
                    keyword=keyword["keyword"],
                    keyword_id=keyword["id"],
                    max_pages=crawl_lanes.facebook_max_pages(keyword.get("released_at"), today),
                    lane="fresh",
                )
            else:
                ok = await publish_crawl_request(
                    platform=platform,
                    keyword=keyword["keyword"],
                    keyword_id=keyword["id"],
                    max_pages=low_pages.get(keyword["id"]),
                    recent_post=recent,
                )
            if ok:
                published += 1
        # Trang nguồn: mỗi ngày một lượt (lượt đầu) khi chạy theo phần - 6 lượt bài mới mà lượt nào cũng xếp lại bảng
        # tin mọi trang nguồn là gấp 6 lần request page_feed.
        if platform == "facebook" and (not fresh_lane or slot_index == 0):
            # Cào theo nguồn: bảng tin các trang nguồn đang bật (app/services/crawl/source_pages.py).
            try:
                await queue_source_pages()
            except Exception as exc:  # noqa: BLE001 - lỗi trang nguồn không chặn lượt tìm kiếm
                logger.warning("source_pages_queue_failed", error=str(exc)[:200])
        logger.info(
            "scheduled_crawl_triggered",
            platform=platform,
            requested=len(keywords),
            published=published,
            recent=recent,
            lane="fresh" if fresh_lane else None,
            slot_index=slot_index,
            telegram=True,
        )

    if nurture_after and platform in NURTURE_PLATFORMS:
        ok = await publish_nurture_request(platform)
        logger.info("scheduled_nurture_queued", platform=platform, when="after", ok=ok)
    elif nurture_after:
        logger.info("scheduled_nurture_skipped_platform", platform=platform, when="after")


# Từ 2026-10-09 một nền tảng có thể chạy tối đa 3 lần mỗi ngày (run_time = "HH:MM,HH:MM,HH:MM"). last_triggered_date chỉ
# chống chạy lặp theo NGÀY nên không đủ: mỗi giờ chạy có cờ riêng trong Redis (sống qua restart), kèm bản trong bộ nhớ
# phòng khi Redis trục trặc.
_CRAWL_FIRED_PREFIX = f"{REDIS_KEY_PREFIX}scheduler:crawl_fired:"
_crawl_fired: set[str] = set()


async def _claim_crawl_slot(platform: str, today: str, slot: str) -> bool:
    key = f"{platform}:{today}:{slot}"
    if key in _crawl_fired:
        return False
    _crawl_fired.add(key)
    if len(_crawl_fired) > 500:
        _crawl_fired.clear()
        _crawl_fired.add(key)
    try:
        return bool(await get_redis_client().set(f"{_CRAWL_FIRED_PREFIX}{key}", "1", nx=True, ex=2 * 24 * 3600))
    except Exception as exc:  # noqa: BLE001 - không có Redis thì cờ trong bộ nhớ vẫn chặn lặp trong tiến trình này
        logger.warning("scheduler_crawl_slot_redis_failed", error=str(exc))
        return True


async def _tick() -> None:
    now = datetime.now(TIMEZONE)
    current_hm = now.strftime("%H:%M")
    today = now.date().isoformat()

    schedules = await db.list_crawl_schedules()
    for sched in schedules:
        if not sched["enabled"]:
            continue
        due = [slot for slot in parse_run_times(sched["run_time"]) if _is_due(slot, now)]
        if not due:
            continue
        platform = sched["platform"]
        if not await _claim_crawl_slot(platform, today, due[0]):
            continue
        await db.mark_crawl_schedule_triggered(platform, today)
        logger.info("scheduled_crawl_firing", platform=platform, run_time=due[0], fired_at=current_hm)
        # Không await tại chỗ - một lần publish Kafka hoặc query D1 chậm cho một nền tảng không
        # được làm trễ việc kiểm tra (hoặc bắn) lịch của mọi nền tảng khác trong cùng lượt.
        slots = parse_run_times(sched["run_time"])
        _spawn(
            _trigger_platform(
                platform,
                nurture_before=bool(sched.get("nurture_before")),
                nurture_after=bool(sched.get("nurture_after")),
                recent=platform in RECENT_LATER_SLOT_PLATFORMS and len(slots) > 1 and due[0] != slots[0],
                slot_index=slots.index(due[0]) if due[0] in slots else 0,
            )
        )


# Lượt comment mỗi giờ chạy từ phút này (cho lượt crawl bài đầu giờ kịp đổ bài mới về) - và giờ đã chạy của mỗi nền
# tảng, để vòng 30 giây không bắn lặp. Mất khi khởi động lại tiến trình: tệ nhất chạy thêm một lượt, các bài vừa xếp
# hàng đã được đánh dấu comments_crawled_at nên không bị quét trùng.
_COMMENT_ROUND_MINUTE = 5
_comment_round_done: dict[str, str] = {}
# Số bài nóng mỗi phim = top_n của lịch, kẹp trong khoảng này. top_n trước 2026-10-08 nghĩa là "top N bài mỗi từ
# khoá" (mặc định 100) - giá trị lớn hơn mức trần đó coi là cấu hình cũ và dùng _HOT_PER_MOVIE_DEFAULT.
_HOT_PER_MOVIE_RANGE = (3, 30)
_HOT_PER_MOVIE_DEFAULT = 15


def _sample_hours(run_time: str) -> set[int]:
    """Giờ chạy lượt mẫu phân tầng: giờ của run_time và 12 tiếng sau đó."""
    try:
        hour = int(run_time.split(":")[0]) % 24
    except ValueError, AttributeError:
        hour = 8
    return {hour, (hour + 12) % 24}


async def _comments_tick() -> None:
    now = datetime.now(TIMEZONE)
    if now.minute < _COMMENT_ROUND_MINUTE:
        return
    hour_key = now.strftime("%Y-%m-%dT%H")
    schedules = await db.list_comment_crawl_schedules()
    for sched in schedules:
        platform = sched["platform"]
        if not sched["enabled"] or platform not in COMMENT_CRAWL_PLATFORMS:
            continue
        if _comment_round_done.get(platform) == hour_key:
            continue
        if platform == "facebook" and not await _facebook_crawl_enabled():
            continue
        _comment_round_done[platform] = hour_key
        low, high = _HOT_PER_MOVIE_RANGE
        top_n = int(sched.get("top_n") or _HOT_PER_MOVIE_DEFAULT)
        hot_per_movie = max(top_n, low) if top_n <= high else _HOT_PER_MOVIE_DEFAULT
        with_sample = now.hour in _sample_hours(sched.get("run_time") or "08:00")
        logger.info(
            "scheduled_comments_firing",
            platform=platform,
            hour=hour_key,
            hot_per_movie=hot_per_movie,
            with_sample=with_sample,
        )
        # Không await tại chỗ - một lượt chọn bài chậm cho một nền tảng không được làm trễ lịch của nền tảng khác.
        _spawn(run_comment_round(platform, hot_per_movie=hot_per_movie, with_sample=with_sample))


_PURGE_KEY = "cinemark_api:cleanup:irrelevant_posts:last_run"
_purge_running = False


async def _run_purge(triggered_by: str = "schedule") -> None:
    """Một lượt dọn (cleanup.run_purge ghi dòng lịch sử và giữ khoá liên tiến trình). Cờ
    `_purge_running` trong tiến trình là thứ mà chỉ báo "running" của dashboard đọc."""
    global _purge_running
    try:
        await run_purge(triggered_by=triggered_by)
    except Exception as exc:
        logger.error("irrelevant_purge_failed", error=str(exc), telegram=True)
    finally:
        _purge_running = False


async def _cleanup_tick() -> None:
    global _purge_running
    if _purge_running:
        return
    # Tham số lưu trên dashboard được ưu tiên hơn mặc định từ env có sẵn trong object
    # Settings; dòng cleanup_settings chưa đặt/rỗng thì quay về các giá trị env đó qua
    # resolve_cleanup_settings().
    cfg = await resolve_cleanup_settings()
    if not cfg["enabled"]:
        return
    now = datetime.now(TIMEZONE)
    if not _is_due(cfg["run_time"], now):
        return
    today = now.date().isoformat()
    redis = get_redis_client()
    if await redis.get(_PURGE_KEY) == today:
        return
    await redis.set(_PURGE_KEY, today, ex=3 * 24 * 60 * 60)
    _purge_running = True
    logger.info("irrelevant_purge_firing", run_time=cfg["run_time"])
    _spawn(_run_purge(triggered_by="schedule"))


async def run_purge_now() -> bool:
    """Kích hoạt tay từ dashboard. Trả về False nếu đang có lượt chạy (dashboard nên báo
    điều đó cho người dùng thay vì quay vòng chờ kết quả mãi) - lượt theo lịch cũng làm
    cùng phép kiểm tra _purge_running ở trên."""
    global _purge_running
    if _purge_running:
        return False
    _purge_running = True
    _spawn(_run_purge(triggered_by="manual"))
    return True


def purge_in_progress() -> bool:
    return _purge_running


# --- Lượt của bộ lập lịch auto-login ---
# Nhịp khác với crawl/cleanup/comments: mặc định mỗi giờ thay vì mỗi ngày. Thay vì ở
# mỗi lần kiểm tra 30s lại xem đã đủ một giờ chưa (buộc vòng lặp của scheduler.py phải
# theo dõi mốc thời gian), ta chạy một lượt bên trong:
#   * Đọc interval_seconds do người vận hành cấu hình trong auto_login_settings mỗi lần
#     thức dậy, nên đổi chu kỳ trên dashboard có hiệu lực ở lần thức tiếp theo (không
#     cần restart API).
#   * So một mốc "lượt gần nhất lúc" trong bộ nhớ với thời gian hiện tại.
# Quay về mặc định 3600s nếu dòng setting rỗng/thiếu (khớp với giá trị dự phòng từ env
# mà auto_login/scheduler.py của spider-hub dùng từ ngày đầu). Lỗi bên trong
# run_auto_login_tick được service bắt + log + lưu vào auto_login_run_history; vòng lặp
# này chỉ ngủ rồi lên lịch lại.
_auto_login_last_tick_at: float | None = None


async def _auto_login_tick() -> None:
    global _auto_login_last_tick_at
    try:
        cfg = await resolve_auto_login_settings()
    except Exception:
        # Bảng settings chưa tồn tại / Supabase trục trặc / v.v. - log rồi bỏ qua khung 30s
        # này, lượt sau sẽ thử lại.
        logger.error("scheduler_auto_login_settings_read_failed")
        return
    if not cfg.enabled:
        # Không cần ngủ - người vận hành muốn tắt auto-login, ta chỉ ngừng kiểm tra. Bật lại
        # công tắc thì dòng đó được đọc lại ở lần kiểm tra 30s kế tiếp, nên lật công tắc trên
        # dashboard có hiệu lực trong vòng 30s.
        _auto_login_last_tick_at = None
        return
    interval = int(cfg.interval_seconds or 3600)
    now = datetime.now(TIMEZONE).timestamp()
    if _auto_login_last_tick_at is None:
        # Lấy mốc từ lượt chạy đã ghi gần nhất thay vì bắn ngay - API restart mỗi lần lưu code
        # khi chạy `uvicorn --reload`, và nếu không thì mỗi lần restart sẽ publish một loạt
        # đăng nhập thật mới.
        history = await db.list_auto_login_run_history(limit=1)
        if history:
            _auto_login_last_tick_at = history[0]["started_at"].timestamp()
    if _auto_login_last_tick_at is not None and (now - _auto_login_last_tick_at) < interval:
        return
    _auto_login_last_tick_at = now
    logger.info(
        "scheduler_auto_login_firing",
        interval_seconds=interval,
        platforms=list(cfg.platforms),
        dry_run=cfg.dry_run,
    )
    # Bắn-rồi-quên - publish Kafka chậm theo từng tài khoản (mỗi tài khoản chết một lần, có
    # thể tới vài trăm mỗi nền tảng) không được chặn các lượt khác của vòng lặp scheduler
    # (crawl/comments/cleanup). Cờ _in_flight của service ngăn lượt theo lịch thứ hai đua
    # với lượt đầu dù ở đây không await.
    _spawn(run_auto_login_tick(triggered_by="schedule", force=True))


# Mốc lần xếp cookie_check gần nhất nằm trong Redis, không trong biến của tiến trình: `uvicorn --reload` khởi động lại
# tiến trình mỗi lần sửa code, và mốc trong bộ nhớ sẽ khiến mỗi lần restart lại xếp thêm một lượt kiểm tra.
_COOKIE_CHECK_LAST_KEY = f"{REDIS_KEY_PREFIX}scheduler:cookie_check:last_published_at"


async def _cookie_check_tick() -> None:
    """Mỗi cookie_check_interval_hours xếp một lượt kiểm tra cookie Facebook (xem publish_cookie_check_request).
    stale_hours nhỏ hơn chu kỳ 1 giờ để tài khoản kiểm tra ở lượt trước (lệch vài phút) vẫn được kiểm tra lại."""
    cfg = await resolve_auto_login_settings()
    if not cfg.cookie_check_enabled or "facebook" not in cfg.platforms:
        return
    interval_seconds = cfg.cookie_check_interval_hours * 3600
    client = get_redis_client()
    last = await client.get(_COOKIE_CHECK_LAST_KEY)
    if last is not None and time.time() - float(last) < interval_seconds:
        return
    await client.set(_COOKIE_CHECK_LAST_KEY, str(time.time()), ex=interval_seconds * 2)
    stale_hours = max(1, cfg.cookie_check_interval_hours - 1)
    ok = await publish_cookie_check_request("facebook", stale_hours=stale_hours)
    if not ok:
        await client.delete(_COOKIE_CHECK_LAST_KEY)  # Kafka lỗi: thử lại ở vòng 30s kế tiếp
    logger.info(
        "scheduler_cookie_check_published",
        ok=ok,
        interval_hours=cfg.cookie_check_interval_hours,
        stale_hours=stale_hours,
    )


async def _facebook_crawl_enabled() -> bool:
    """Công tắc lịch cào Facebook (crawl_schedules) - 30/10/2026: tắt nó là ngừng MỌI việc dùng tài khoản / proxy Facebook
    do scheduler xếp (nhịp đào mỗi giờ, lượt bình luận, bài booking theo dõi), không chỉ các lượt cào theo giờ. Trước đó
    người vận hành tắt lịch để cho tài khoản nghỉ nhưng nhịp đào + bình luận vẫn chạy. Nuôi tài khoản không bị ảnh hưởng."""
    return any(sched["platform"] == "facebook" and sched["enabled"] for sched in await db.list_crawl_schedules())


def _jittered(slot: str, day: str, jitter_minutes: int) -> str:
    """ "HH:MM" cộng 0..jitter_minutes phút, cố định theo (ngày, mốc) - mọi tiến trình / lượt 30 giây ra cùng một giờ."""
    if jitter_minutes <= 0:
        return slot
    hour, minute = (int(part) for part in slot.split(":", 1))
    total = min(hour * 60 + minute + zlib.crc32(f"{day}:{slot}".encode()) % (jitter_minutes + 1), 23 * 60 + 59)
    return f"{total // 60:02d}:{total % 60:02d}"


_NURTURE_FIRED_KEY = f"{REDIS_KEY_PREFIX}scheduler:nurture:{{platform}}:{{day}}:{{slot}}"


async def _nurture_tick() -> None:
    """Xếp một lượt nuôi Facebook ở mỗi mốc settings.facebook_nurture_times, kể cả khi lịch cào đang tắt. Trùng với cờ
    nuôi của lịch cào cũng không sao: spider-hub bỏ qua tài khoản đã nuôi trong ngày / nửa ngày."""
    now = datetime.now(TIMEZONE)
    day = now.date().isoformat()
    due = [
        slot
        for slot in parse_run_times(settings.facebook_nurture_times or "")
        if _is_due(_jittered(slot, day, settings.facebook_nurture_jitter_minutes), now)
    ]
    if not due:
        return
    key = _NURTURE_FIRED_KEY.format(platform="facebook", day=day, slot=due[0])
    if not await get_redis_client().set(key, "1", nx=True, ex=2 * 86400):
        return
    ok = await publish_nurture_request("facebook")
    logger.info("scheduled_nurture_queued", platform="facebook", when="nurture_schedule", slot=due[0], ok=ok)


# AI tìm thêm hashtag TikTok mỗi ngày một lần ở settings.hashtag_discovery_time (xem app/services/discovery/hashtag_discovery.py).
_HASHTAG_DISCOVERY_KEY = f"{REDIS_KEY_PREFIX}scheduler:hashtag_discovery:last_run"


async def _discovery_enabled() -> bool:
    """Công tắc "AI tự thêm nguồn cào" trên dashboard (bảng discovery_settings). Tắt -> không chạy hashtag_discovery,
    query_discovery, duyệt trang nguồn gợi ý, kể cả bước BFS sau lượt Omar: chỉ cào từ khoá / trang đang có. Đọc lỗi thì
    coi như bật (hành vi trước khi có công tắc)."""
    try:
        return bool((await db.get_discovery_settings()).get("enabled", True))
    except Exception as exc:  # noqa: BLE001
        logger.warning("discovery_settings_read_failed", error=str(exc)[:200])
        return True


async def _run_hashtag_discovery() -> None:
    # Hashtag TikTok rồi câu tìm kiếm Facebook/Threads (app/services/discovery/query_discovery.py); lỗi phần này không chặn phần kia.
    # Rồi AI duyệt trang nguồn Facebook gợi ý (app/services/crawl/source_pages.py) - tự thêm trang của phim / trang chung.
    for name, job in (
        ("hashtag_discovery", run_discovery),
        ("query_discovery", run_query_discovery),
        ("source_page_review", review_source_pages),
    ):
        try:
            await job()
        except Exception as exc:  # noqa: BLE001 - một lượt lỗi chỉ để mai chạy lại
            logger.error(f"{name}_failed", error=str(exc)[:300])


async def _hashtag_tick() -> None:
    run_time = (settings.hashtag_discovery_time or "").strip()
    now = datetime.now(TIMEZONE)
    if not run_time or not _is_due(run_time, now):
        return
    today = now.date().isoformat()
    redis = get_redis_client()
    if await redis.get(_HASHTAG_DISCOVERY_KEY) == today:
        return
    await redis.set(_HASHTAG_DISCOVERY_KEY, today, ex=3 * 24 * 3600)
    if not await _discovery_enabled():
        logger.info("hashtag_discovery_skipped_disabled", run_time=run_time)
        return
    logger.info("hashtag_discovery_firing", run_time=run_time)
    _spawn(_run_hashtag_discovery())


# --- Omar API: hashtag TikTok mỗi giờ (app/services/omar/omar_tiktok.py) ---------------------------------------------
_OMAR_ROUND_KEY = f"{REDIS_KEY_PREFIX}scheduler:omar_round:last_hour"


_BFS_DISCOVERY_KEY = f"{REDIS_KEY_PREFIX}scheduler:hashtag_discovery:after_omar_at"


async def _run_omar_round(trigger: str) -> None:
    try:
        await run_omar_round_tracked(trigger)
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để giờ sau chạy lại
        logger.error("omar_round_failed", error=str(exc)[:300])
    # BFS: Kira duyệt hashtag đi kèm mà lượt Omar vừa đếm (app/services/discovery/hashtag_discovery.py), không đợi tới sáng hôm sau.
    try:
        redis = get_redis_client()
        last = float(await redis.get(_BFS_DISCOVERY_KEY) or 0)
        if time.time() - last >= settings.hashtag_discovery_after_omar_hours * 3600 and await _discovery_enabled():
            await redis.set(_BFS_DISCOVERY_KEY, str(time.time()), ex=7 * 24 * 3600)
            logger.info("hashtag_discovery_after_omar")
            await run_discovery()
    except Exception as exc:  # noqa: BLE001
        logger.error("hashtag_discovery_after_omar_failed", error=str(exc)[:300])


async def _enqueue_weekly_comments(week: str) -> None:
    # Chạy nền: xếp hàng bình luận tuần mất lâu hơn một tick (một truy vấn D1 mỗi hashtag) - chạy trong tick thì bị cắt
    # sau 25 giây và chặn luôn việc mở lượt Omar (lỗi 2026-10-12).
    try:
        await omar_comments.enqueue_weekly_job(week=week)
    except Exception as exc:  # noqa: BLE001 - khoá hết hạn sau 15 phút, tick sau thử lại
        logger.error("omar_comments_enqueue_failed", error=str(exc)[:300])


def trigger_omar_now() -> bool:
    """Nút "Chạy ngay" của tab Omar (app/api/routes/omar.py). False khi đang có lượt chạy."""
    if not claim_omar_round("manual"):
        return False
    _spawn(_run_omar_round("manual"))
    return True


async def _omar_tick() -> None:
    """Mỗi giờ một lượt, từ phút settings.omar_round_minute. Lượt trước chưa xong (chế độ chạy liên tục: một lượt có thể
    kéo dài nhiều giờ tới khi hết hạn mức) thì không mở lượt chồng lên; xong thì lượt kế bắt đầu ở lần kiểm tra sau. Khoá
    theo giờ trong Redis để restart API không chạy lặp. Tới mốc tuần thì xếp hàng bình luận (app/services/omar/omar_comments.py)
    - lượt kế tiếp xử lý hàng đợi trước phần hashtag."""
    now = datetime.now(TIMEZONE)
    try:
        if await omar_comments.weekly_enqueue_due(now):
            _spawn(_enqueue_weekly_comments(omar_comments.iso_week(now)))
    except Exception as exc:  # noqa: BLE001 - tick sau thử lại
        logger.error("omar_comments_enqueue_failed", error=str(exc)[:300])
    if omar_is_running():
        return
    # Đang dừng vì hết hạn mức / key sai (omar_tiktok.set_pause): không gọi Omar cho tới lúc hết dừng, rồi mở lượt NGAY
    # (không đợi phút omar_round_minute hay giờ kế tiếp) để cào tiếp từ con trỏ đã lưu.
    resume = False
    pause = await get_omar_pause()
    if pause:
        if not await omar_pause_over(pause):
            return
        await clear_omar_pause()
        resume = True
        logger.info("omar_resuming_after_pause", reason=pause.get("reason"), paused_until=pause.get("until"))
    if not resume and now.minute < settings.omar_round_minute:
        return
    hour_key = now.strftime("%Y-%m-%dT%H")
    redis = get_redis_client()
    if not resume and await redis.get(_OMAR_ROUND_KEY) == hour_key:
        return
    if not claim_omar_round("schedule"):
        return
    await redis.set(_OMAR_ROUND_KEY, hour_key, ex=2 * 3600)
    logger.info("omar_round_firing", hour=hour_key)
    _spawn(_run_omar_round("schedule"))


# --- Cảnh báo vận hành (app/services/ops/ops_alerts.py) - mỗi settings.ops_alerts_interval_minutes phút ---------------
# Khoá Redis thay cho mốc trong bộ nhớ để restart API (uvicorn --reload) không kiểm tra lại ngay; chạy nền vì /usage của
# Omar và D1 có thể chậm.
_OPS_ALERTS_KEY = f"{REDIS_KEY_PREFIX}scheduler:ops_alerts:last_run"


async def _run_ops_alerts() -> None:
    try:
        await ops_alerts.run_checks()
    except Exception as exc:  # noqa: BLE001 - lượt sau thử lại
        logger.error("ops_alerts_failed", error=str(exc)[:300])


async def _ops_alerts_tick() -> None:
    if not settings.ops_alerts_enabled:
        return
    # Trừ một nhịp kiểm tra để lượt kế tiếp không trễ thêm 30 giây mỗi lần.
    ttl = max(60, settings.ops_alerts_interval_minutes * 60 - _POLL_INTERVAL_SECONDS)
    if not await get_redis_client().set(_OPS_ALERTS_KEY, "1", nx=True, ex=ttl):
        return
    _spawn(_run_ops_alerts())


# --- Omar: "video đang nổi" mỗi settings.omar_trending_interval_hours giờ (app/services/omar/omar_trending.py) -----------
_OMAR_TRENDING_KEY = f"{REDIS_KEY_PREFIX}scheduler:omar_trending:last_run"


async def _run_omar_trending() -> None:
    try:
        await omar_trending.run_refresh_tracked()
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để lượt sau chạy lại
        logger.error("omar_trending_failed", error=str(exc)[:300])


async def _omar_trending_tick() -> None:
    """Độc lập với lượt Omar chính (nhịp gọi riêng). Khoá Redis sống một chu kỳ (trừ 2 phút cho lệch tick) nên restart API
    không chạy lặp; việc chạy ở nền vì một lượt mất hàng chục phút."""
    if not settings.omar_trending_enabled or omar_trending.is_running():
        return
    # Hết hạn mức thì làm mới video đang lên cũng chỉ ra 429 - chờ cùng lúc với lượt chính.
    if await get_omar_pause():
        return
    ttl = max(60, int(settings.omar_trending_interval_hours * 3600) - 120)
    if not await get_redis_client().set(_OMAR_TRENDING_KEY, datetime.now(TIMEZONE).isoformat(), nx=True, ex=ttl):
        return
    logger.info("omar_trending_firing")
    _spawn(_run_omar_trending())


# Cấu hình AI cho Worker cinemark (app/services/ops/ai_runtime_sync.py): chép sang D1 mỗi AI_RUNTIME_SYNC_SECONDS - lưu ở
# dashboard thì đã chép ngay, vòng này bắt cả thay đổi sửa thẳng trên Supabase.
AI_RUNTIME_SYNC_SECONDS = 600


# Report Social Topic theo ngày hẹn (app/services/reports/report_schedule.py): kiểm tra mỗi chừng này giây.
REPORT_SCHEDULE_CHECK_SECONDS = 600
_report_schedule_running = False


async def _run_report_schedule() -> None:
    global _report_schedule_running
    from app.services.reports.report_schedule import run_due_reports

    try:
        await run_due_reports()
    except Exception as exc:  # noqa: BLE001 - lượt sau thử lại
        logger.error("scheduler_report_schedule_failed", error=str(exc)[:300])
    finally:
        _report_schedule_running = False


async def _report_schedule_tick() -> None:
    global _report_schedule_running
    key = f"{REDIS_KEY_PREFIX}scheduler:report_schedule"
    if _report_schedule_running:
        return
    if await get_redis_client().set(key, datetime.now(TIMEZONE).isoformat(), nx=True, ex=REPORT_SCHEDULE_CHECK_SECONDS):
        _report_schedule_running = True
        _spawn(_run_report_schedule())


async def _ai_runtime_tick() -> None:
    from app.services.ops.ai_runtime_sync import sync_ai_runtime

    key = f"{REDIS_KEY_PREFIX}scheduler:ai_runtime_sync"
    if await get_redis_client().set(key, datetime.now(TIMEZONE).isoformat(), nx=True, ex=AI_RUNTIME_SYNC_SECONDS):
        _spawn(sync_ai_runtime())


# Bài booking đang theo dõi (app/services/crawl/tracked_posts.py): cào lại mỗi ngày một lần, từ TRACKED_POSTS_HOUR giờ sáng.
TRACKED_POSTS_HOUR = 6


async def _run_tracked_posts() -> None:
    try:
        await tracked_posts.run_daily(include_facebook=await _facebook_crawl_enabled())
    except Exception as exc:  # noqa: BLE001 - lượt lỗi: hôm sau chạy lại (hoặc restart trong ngày chạy lại)
        logger.error("tracked_posts_failed", error=str(exc)[:300])


async def _tracked_posts_tick() -> None:
    now = datetime.now(TIMEZONE)
    if now.hour < TRACKED_POSTS_HOUR or tracked_posts.is_running():
        return
    key = f"{REDIS_KEY_PREFIX}scheduler:tracked_posts:{now.date().isoformat()}"
    if not await get_redis_client().set(key, now.isoformat(), nx=True, ex=2 * 86400):
        return
    logger.info("tracked_posts_firing")
    _spawn(_run_tracked_posts())


_CHANNEL_PROFILES_KEY = f"{REDIS_KEY_PREFIX}scheduler:channel_profiles"
_threads_collecting = False


async def _collect_threads_profiles() -> None:
    global _threads_collecting
    try:
        await channel_profiles.collect_threads_results()
    except Exception as exc:  # noqa: BLE001 - kết quả còn trong Redis, vòng sau gom lại
        logger.error("channel_profiles_threads_collect_failed", error=str(exc))
    finally:
        _threads_collecting = False


async def _channel_profiles_tick() -> None:
    """Follower kênh (app/services/crawl/channel_profiles.py) mỗi settings.channel_profiles_interval_hours giờ. Khoá Redis
    sống một chu kỳ nên restart API không chạy lặp; chạy nền vì một lượt mất vài phút.

    - Facebook / Threads: không tự crawl (spider-hub lo); kết quả spider-hub ghi vào Redis (nếu có) vẫn được gom mỗi vòng.
    - TikTok (Omar): Omar hết hạn mức thì chờ."""
    global _threads_collecting
    # Kênh ưu tiên (danh sách booking chưa có bài): crawl ngay, mỗi vòng - app/services/crawl/priority_channels.py.
    if not priority_channels.is_running():
        _spawn(priority_channels.process_pending())
    if settings.channel_profiles_interval_hours <= 0:
        return
    redis = get_redis_client()
    if not _threads_collecting and await redis.hlen(channel_profiles.THREADS_RESULTS_KEY):
        _threads_collecting = True
        _spawn(_collect_threads_profiles())
    ttl = max(60, int(settings.channel_profiles_interval_hours * 3600) - 120)
    if channel_profiles.is_running() or await get_omar_pause():
        return
    if not await redis.set(_CHANNEL_PROFILES_KEY, datetime.now(TIMEZONE).isoformat(), nx=True, ex=ttl):
        return
    _spawn(channel_profiles.refresh_followers())


# Đối soát bảng tổng hợp số liệu (stats_summary.reconcile_recent_days) mỗi 10 phút: từ 30/10/2026 đây là nguồn DUY NHẤT
# của bảng tổng hợp (ingest không cộng từng bài nữa - xem docstring stats_summary). Lượt thường chỉ tính hôm nay; lượt
# _STATS_RECONCILE_FULL_SLOT mỗi giờ tính cả hôm qua (bài về trễ sau nửa đêm).
_STATS_RECONCILE_EVERY_MINUTES = 10
_STATS_RECONCILE_FULL_SLOT = 4  # phút :40-:49
_STATS_RECONCILE_KEY = f"{REDIS_KEY_PREFIX}scheduler:stats_reconcile:{{slot}}"


async def _stats_reconcile_tick() -> None:
    now = datetime.now(TIMEZONE)
    slot = now.minute // _STATS_RECONCILE_EVERY_MINUTES
    key = _STATS_RECONCILE_KEY.format(slot=f"{now:%Y-%m-%dT%H}:{slot}")
    if not await get_redis_client().set(key, "1", nx=True, ex=2 * 3600):
        return
    # Mỗi câu tổng hợp có thể tới vài chục giây khi D1 bận - chạy nền, không giữ vòng lặp lịch.
    _spawn(reconcile_recent_days(2 if slot == _STATS_RECONCILE_FULL_SLOT else 1))
    # Bảng tổng hợp bình luận theo phim x ngày (cinemark đọc cho Social Topic / Social Buzz) - cùng nhịp 10 phút; lượt đêm
    # tính lại NIGHTLY_DAYS ngày (app/services/stats/comment_rollup.py).
    if now.hour == comment_rollup.NIGHTLY_HOUR and slot == 3:
        _spawn(comment_rollup.refresh_nightly())
    else:
        _spawn(comment_rollup.refresh_recent())


async def _loop() -> None:
    while True:
        try:
            await asyncio.wait_for(_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            # Một lượt lỗi (D1/Kafka/DB trục trặc) không được giết vòng lặp - lượt theo lịch kế
            # tiếp, có thể cho nền tảng khác, vẫn cần có cơ hội chạy.
            logger.error("scheduler_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_cleanup_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_cleanup_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            logger.error("scheduler_cleanup_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_comments_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_comments_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            logger.error("scheduler_comments_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_auto_login_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_auto_login_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            # Đọc auto_login_settings lỗi hoặc lỗi ném ra từ create_task không được giết vòng lặp.
            logger.error("scheduler_auto_login_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_cookie_check_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_cookie_check_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS, telegram=True)
        except Exception as exc:
            logger.error("scheduler_cookie_check_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_hashtag_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_hashtag_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001 - như các lượt khác: không được giết vòng lặp
            logger.error("scheduler_hashtag_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_omar_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_omar_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_omar_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_omar_trending_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_omar_trending_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_omar_trending_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_channel_profiles_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_channel_profiles_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_channel_profiles_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_ops_alerts_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_ops_alerts_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_ops_alerts_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_tracked_posts_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_tracked_posts_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_tracked_posts_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_ai_runtime_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_ai_runtime_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_report_schedule_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_report_schedule_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_stats_reconcile_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_stats_reconcile_tick_failed", error=str(exc))
        try:
            await asyncio.wait_for(_nurture_tick(), timeout=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_nurture_tick_failed", error=str(exc))
        try:
            if await _facebook_crawl_enabled():
                await asyncio.wait_for(crawl_lanes.run_deep_tick(datetime.now(TIMEZONE)), timeout=_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error("scheduler_deep_tick_timeout", timeout_seconds=_TICK_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error("scheduler_deep_tick_failed", error=str(exc))
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)


def start() -> None:
    global _task
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info("scheduler_started")


def stop() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        _task = None
