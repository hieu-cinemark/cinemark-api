"""Bộ lập lịch crawl hằng ngày chạy trong tiến trình - bản thay thế của dashboard cho cả
scripts/trigger_scheduled_crawl.sh chạy bằng crontab hệ điều hành của cinemark-api
(nhịp cố định "mỗi 6 giờ", chỉ đổi được bằng cách sửa crontab trên máy) lẫn
Cloudflare Cron Triggers của cinemark-scraper cho Threads/TikTok (wrangler.toml, phải
deploy lại Worker mới đổi được). Từ giờ, đây là nguồn sự thật duy nhất về lúc nào
crawl của một nền tảng chạy - sửa được từ thẻ "Crawl schedule" trên dashboard (xem
CRUD crawl_schedules trong app/services/platform_config_db.py), không cần deploy
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

_comments_tick bên dưới cùng dạng cho comment_crawl_schedules - một giờ hằng ngày
thứ hai, độc lập, theo từng nền tảng, quét các bài tương tác cao của mọi từ khoá đang
bật để tìm bài còn thiếu comment (xem list_posts_needing_comments trong
app/services/d1.py) và xếp hàng một lượt crawl comment cho mỗi bài, đúng cách chọn mà
scripts/trigger_recent_keyword_comments.py vẫn làm bằng tay."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from app.clients.kafka import (
    publish_comments_crawl_request,
    publish_cookie_check_request,
    publish_crawl_request,
    publish_nurture_request,
)
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.services import platform_config_db as db
from app.services.auto_login import resolve_auto_login_settings, run_auto_login_tick
from app.services.cleanup import resolve_cleanup_settings, run_purge
from app.services.d1 import get_enabled_keywords, list_posts_needing_comments
from app.services.platforms import COMMENT_CRAWL_PLATFORMS

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


async def _trigger_platform(platform: str, *, nurture_before: bool = False, nurture_after: bool = False) -> None:
    # Cùng topic Kafka và consumer theo nền tảng với crawl, nên nurture publish trước thì
    # thực sự chạy xong rồi mới tới các lượt crawl từ khoá phía sau; publish sau cùng thì
    # nó chạy sau khi chúng xong hết.
    if nurture_before and platform in NURTURE_PLATFORMS:
        ok = await publish_nurture_request(platform)
        logger.info("scheduled_nurture_queued", platform=platform, when="before", ok=ok)
    elif nurture_before:
        logger.info("scheduled_nurture_skipped_platform", platform=platform, when="before")

    keywords = await get_enabled_keywords(platform=platform)
    if not keywords:
        logger.info("scheduled_crawl_no_keywords", platform=platform)
    else:
        published = 0
        for keyword in keywords:
            ok = await publish_crawl_request(platform=platform, keyword=keyword["keyword"], keyword_id=keyword["id"])
            if ok:
                published += 1
        logger.info(
            "scheduled_crawl_triggered",
            platform=platform,
            requested=len(keywords),
            published=published,
            telegram=True,
        )

    if nurture_after and platform in NURTURE_PLATFORMS:
        ok = await publish_nurture_request(platform)
        logger.info("scheduled_nurture_queued", platform=platform, when="after", ok=ok)
    elif nurture_after:
        logger.info("scheduled_nurture_skipped_platform", platform=platform, when="after")


async def _tick() -> None:
    now = datetime.now(TIMEZONE)
    current_hm = now.strftime("%H:%M")
    today = now.date().isoformat()

    schedules = await db.list_crawl_schedules()
    for sched in schedules:
        if not sched["enabled"]:
            continue
        if not _is_due(sched["run_time"], now):
            continue
        last = sched["last_triggered_date"]
        # dict_row của psycopg trả về object date thật, không phải chuỗi - so sánh với cùng
        # dạng thay vì chuỗi ISO.
        if last is not None and last.isoformat() == today:
            continue
        platform = sched["platform"]
        await db.mark_crawl_schedule_triggered(platform, today)
        logger.info("scheduled_crawl_firing", platform=platform, run_time=sched["run_time"], fired_at=current_hm)
        # Không await tại chỗ - một lần publish Kafka hoặc query D1 chậm cho một nền tảng không
        # được làm trễ việc kiểm tra (hoặc bắn) lịch của mọi nền tảng khác trong cùng lượt.
        _spawn(
            _trigger_platform(
                platform,
                nurture_before=bool(sched.get("nurture_before")),
                nurture_after=bool(sched.get("nurture_after")),
            )
        )


async def _trigger_comments_platform(platform: str, *, top_n: int) -> None:
    """Với mỗi từ khoá đang bật trên `platform`, xếp hàng một lượt crawl comment cho các bài
    top `top_n` theo tương tác của từ khoá đó mà vẫn chưa có comment nào được lưu - đúng
    cách chọn mà scripts/trigger_recent_keyword_comments.py vẫn làm bằng tay, chỉ là chạy
    theo lịch thay vì bằng tay."""
    keywords = await get_enabled_keywords(platform=platform)
    if not keywords:
        logger.info("scheduled_comments_no_keywords", platform=platform)
        return
    published = 0
    for keyword in keywords:
        posts = await list_posts_needing_comments(platform=platform, keyword_id=keyword["id"], top_n=top_n)
        for post in posts:
            if not post.get("url"):
                continue
            ok = await publish_comments_crawl_request(
                platform=platform, post_external_id=post["external_id"], post_url=post["url"], bypass_drain=False
            )
            if ok:
                published += 1
    logger.info(
        "scheduled_comments_triggered", platform=platform, keywords=len(keywords), published=published, telegram=True
    )


async def _comments_tick() -> None:
    now = datetime.now(TIMEZONE)
    current_hm = now.strftime("%H:%M")
    today = now.date().isoformat()

    schedules = await db.list_comment_crawl_schedules()
    for sched in schedules:
        if not sched["enabled"]:
            continue
        if sched["platform"] not in COMMENT_CRAWL_PLATFORMS:
            continue
        if not _is_due(sched["run_time"], now):
            continue
        last = sched["last_triggered_date"]
        if last is not None and last.isoformat() == today:
            continue
        platform = sched["platform"]
        await db.mark_comment_crawl_schedule_triggered(platform, today)
        logger.info("scheduled_comments_firing", platform=platform, run_time=sched["run_time"], fired_at=current_hm)
        # Không await tại chỗ - cùng lý do như _tick bên dưới: một lượt quét chậm cho một nền
        # tảng không được làm trễ việc kiểm tra lịch (bài hay comment) của mọi nền tảng khác
        # trong cùng lượt.
        _spawn(_trigger_comments_platform(platform, top_n=int(sched.get("top_n") or 100)))


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
