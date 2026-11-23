"""Cảnh báo vận hành qua Telegram - scheduler (app/services/crawl/scheduler/) gọi run_checks() mỗi
settings.ops_alerts_interval_minutes phút:

  1. Omar: đã dùng >= ops_omar_quota_alert_pct % hạn mức ngày; gói hết hạn trong ops_omar_expiry_alert_days ngày (và lần
     nữa khi còn <= 1 ngày, hoặc đã hết hạn).
  2. Omar: tỉ lệ request lỗi của lượt đang chạy / vừa xong trong 1 giờ qua > ops_omar_error_rate_alert (đủ
     ops_omar_error_min_attempts lần gọi).
  3. Nguồn cạn: không có bài TikTok / comment mới (MAX(scraped_at) trên D1) trong ops_post/comment_drought_hours giờ.
  4. Ví AI: nhà cung cấp AI (app/ai/providers/client.py) trả 402 hoặc lỗi hết số dư - đường gọi AI ghi mốc vào Redis qua
     note_ai_wallet_error, ở đây chỉ đọc mốc.

Mỗi cảnh báo chống lặp bằng một khoá Redis SET NX có TTL: lặp lại sau ops_alert_repeat_hours, riêng hạn mức / hạn gói
Omar / chi phí Jev thì khoá theo ngày (mỗi ngày một lần). Gửi thẳng send_telegram_message: field telegram=True của log
trong service này chỉ là một field, không có gì đọc nó để gửi Telegram.

Module này chỉ import nhẹ ở đầu (Redis, Telegram, config) vì app/ai/providers/client.py import nó; phần nặng
(Omar, D1) import trong hàm."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.clients.telegram import send_telegram_message
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
_ALERT_PREFIX = f"{REDIS_KEY_PREFIX}ops_alert:"
_WALLET_PREFIX = f"{REDIS_KEY_PREFIX}ops:ai_wallet_error:"
_DAY_TTL_SECONDS = 26 * 3600
_REDIS_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class Alert:
    key: str  # khoá chống lặp (không kèm prefix)
    ttl: int  # giây: trong khoảng này không gửi lại cùng khoá
    text: str


def _repeat_ttl() -> int:
    return int(settings.ops_alert_repeat_hours * 3600)


def vn_date(now: datetime) -> str:
    return now.astimezone(TIMEZONE).date().isoformat()


def parse_time(value: Any) -> datetime | None:
    """ISO (có/không múi giờ, 'Z', dấu cách thay 'T') hoặc epoch giây/mili giây -> datetime UTC. Không đọc được -> None."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, int | float) or (isinstance(value, str) and value.replace(".", "", 1).isdigit()):
            seconds = float(value)
            if seconds > 1e12:
                seconds /= 1000
            return datetime.fromtimestamp(seconds, tz=UTC)
        parsed = datetime.fromisoformat(str(value).strip())
    except ValueError, OverflowError, OSError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


# --- Hàm quyết định (thuần, không I/O - tests/test_ops_alerts.py) ---------------------------------------------------


def omar_quota_alerts(usage: dict[str, Any] | None, now: datetime) -> list[Alert]:
    """Sinh cảnh báo Telegram từ `usage` của Omar: dùng quá `ops_omar_quota_alert_pct`% hạn mức ngày khi còn lâu mới reset
    (chạy liên tục dùng hết hạn mức là bình thường) và gói sắp / đã hết hạn. Mỗi cảnh báo có khoá chống lặp theo ngày."""
    if not usage:
        return []
    alerts: list[Alert] = []
    today = vn_date(now)
    day = usage.get("day") or {}
    try:
        limit = int(day.get("limit") or 0)
        if day.get("used") is not None:
            used = int(day["used"])
        elif day.get("remaining") is not None:
            used = limit - int(day["remaining"])
        else:
            used = 0
    except TypeError, ValueError:
        limit, used = 0, 0
    try:
        resets_in = float(day.get("resets_in") or 0)
    except TypeError, ValueError:
        resets_in = 0.0
    # Chế độ chạy liên tục cố ý dùng hết hạn mức mỗi ngày - chỉ báo khi CẠN SỚM (còn hơn ops_omar_quota_early_hours giờ
    # mới reset), tức hạn mức không đủ cho cả ngày.
    early = resets_in >= settings.ops_omar_quota_early_hours * 3600
    if limit > 0 and early and used * 100 >= settings.ops_omar_quota_alert_pct * limit:
        alerts.append(
            Alert(
                f"omar_quota:{today}",
                _DAY_TTL_SECONDS,
                f"⚠️ Omar: đã dùng {used:,}/{limit:,} request hôm nay ({used * 100 // limit}%) mà còn "
                f"{resets_in / 3600:.0f} giờ nữa mới reset - hạn mức không đủ cho cả ngày, cân nhắc nâng gói hoặc tăng tổng request mỗi ngày ở tab Omar.",
            )
        )
    expires = parse_time(usage.get("expires_at"))
    if expires is not None:
        days_left = (expires - now).total_seconds() / 86400
        when = expires.astimezone(TIMEZONE).strftime("%H:%M %d/%m/%Y")
        if days_left <= 0:
            tier, text = "expired", f"🚨 Gói Omar ({usage.get('plan') or '?'}) ĐÃ HẾT HẠN lúc {when}. Gia hạn ngay."
        elif days_left <= 1:
            tier, text = "1d", f"🚨 Gói Omar ({usage.get('plan') or '?'}) hết hạn trong vòng 1 ngày ({when})."
        elif days_left <= settings.ops_omar_expiry_alert_days:
            tier = f"{settings.ops_omar_expiry_alert_days}d"
            text = f"⚠️ Gói Omar ({usage.get('plan') or '?'}) hết hạn sau {int(days_left)} ngày ({when})."
        else:
            tier, text = None, ""
        if tier:
            alerts.append(Alert(f"omar_expiry:{tier}:{today}", _DAY_TTL_SECONDS, text))
    return alerts


def omar_error_alert(status: dict[str, Any] | None, now: datetime) -> Alert | None:
    """Lượt Omar đang chạy, hoặc kết thúc trong 1 giờ qua, có tỉ lệ request lỗi cao."""
    if not status:
        return None
    if not status.get("running"):
        finished = parse_time(status.get("finished_at"))
        if finished is None or (now - finished).total_seconds() > 3600:
            return None
    stats = status.get("stats") or {}
    ok, failed = int(stats.get("requests") or 0), int(stats.get("failed") or 0)
    attempts = ok + failed
    if attempts < settings.ops_omar_error_min_attempts or failed / attempts <= settings.ops_omar_error_rate_alert:
        return None
    return Alert(
        "omar_error_rate",
        _repeat_ttl(),
        f"⚠️ Omar: {failed}/{attempts} request lỗi ({failed * 100 // attempts}%) trong lượt "
        f"{'đang chạy' if status.get('running') else 'vừa xong'} (bắt đầu {status.get('started_at') or '?'}, "
        f"đã gửi {int(stats.get('published') or 0)} video). Kiểm tra key/gói Omar hoặc API Omar.",
    )


def drought_alert(kind: str, label: str, last: Any, now: datetime, hours: float) -> Alert | None:
    """kind: khoá ngắn ('posts_tiktok'); label: mô tả tiếng Việt. last: MAX(scraped_at) - None khi bảng rỗng."""
    last_at = parse_time(last)
    if last_at is not None and (now - last_at).total_seconds() < hours * 3600:
        return None
    since = last_at.astimezone(TIMEZONE).strftime("%H:%M %d/%m") if last_at else "chưa từng"
    return Alert(
        f"drought:{kind}",
        _repeat_ttl(),
        f"⚠️ Không có {label} mới trong {hours:g} giờ qua (lần cuối: {since}). Kiểm tra crawler/Omar/ingest consumer.",
    )


def wallet_alert(provider: str, last_error_at: float | None, now_ts: float, detail: str = "") -> Alert | None:
    if last_error_at is None or now_ts - last_error_at > settings.ops_ai_wallet_window_hours * 3600:
        return None
    ago = max(0, int((now_ts - last_error_at) // 60))
    return Alert(
        f"ai_wallet:{provider}",
        _repeat_ttl(),
        f"🚨 AI {provider}: hết số dư / bị từ chối thanh toán ({ago} phút trước){f' - {detail}' if detail else ''}. "
        "Nạp tiền để tiếp tục phân loại.",
    )


_WALLET_CODES = ("vnd_balance_exhausted", "insufficient_quota", "insufficient_balance")


def is_ai_wallet_error(exc: BaseException) -> bool:
    """openai.APIStatusError 402, hoặc mã lỗi hết số dư (OpenAI trả insufficient_quota kèm 429)."""
    if getattr(exc, "status_code", None) == 402:
        return True
    blob = f"{getattr(exc, 'code', '') or ''} {exc}".lower()
    return any(code in blob for code in _WALLET_CODES)


# --- Ghi mốc từ đường gọi AI (không bao giờ raise, không làm chậm lời gọi quá _REDIS_TIMEOUT_SECONDS) ---------------


async def _record_wallet_error(provider: str, detail: str) -> None:
    try:
        await asyncio.wait_for(
            get_redis_client().set(f"{_WALLET_PREFIX}{provider}", f"{time.time()}|{detail[:150]}", ex=24 * 3600),
            timeout=_REDIS_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001 - mất mốc chỉ làm thiếu một cảnh báo
        logger.warning("ops_wallet_error_record_failed", provider=provider, error=exc)


async def note_ai_wallet_error(provider: str, exc: BaseException) -> None:
    """Gọi trong except của app.ai.providers.client.call_ai."""
    if settings.ops_alerts_enabled and is_ai_wallet_error(exc):
        await _record_wallet_error(provider, f"{getattr(exc, 'status_code', '')} {exc}".strip())


# --- Lượt kiểm tra (scheduler) -----------------------------------------------------------------------------------


async def _fire(alert: Alert) -> bool:
    """Gửi nếu chưa gửi cùng khoá trong TTL. True khi đã gửi."""
    if not await get_redis_client().set(f"{_ALERT_PREFIX}{alert.key}", "1", nx=True, ex=max(60, alert.ttl)):
        return False
    logger.warning("ops_alert_sent", alert=alert.key, text=alert.text)
    await send_telegram_message(alert.text)
    return True


async def _omar_alerts(now: datetime) -> list[Alert]:
    from app.services.accounts.config_db.omar import get_omar_api_settings
    from app.services.omar import omar_tiktok

    alerts: list[Alert] = []
    error = omar_error_alert(omar_tiktok.round_status(), now)
    if error:
        alerts.append(error)
    api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
    if api_key:
        alerts += omar_quota_alerts(await omar_tiktok.fetch_usage(api_key), now)
    return alerts


async def _drought_alerts(now: datetime) -> list[Alert]:
    from sqlalchemy import func, select

    from app.repositories.d1.sql import d1_exec
    from app.repositories.d1.tables import comments, posts

    alerts: list[Alert] = []
    checks = (
        (
            "posts_tiktok",
            "bài TikTok",
            select(func.max(posts.c.scraped_at).label("last")).where(posts.c.platform == "tiktok"),
            "post",
        ),
        ("comments", "comment", select(func.max(comments.c.scraped_at).label("last")), "comment"),
    )
    for kind, label, query, which in checks:
        rows = await d1_exec(query)
        if rows is None:  # D1 chưa cấu hình / lỗi: không kết luận được
            continue
        hours = settings.ops_post_drought_hours if which == "post" else settings.ops_comment_drought_hours
        alert = drought_alert(kind, label, (rows[0] if rows else {}).get("last"), now, hours)
        if alert:
            alerts.append(alert)
    return alerts


async def _wallet_providers(redis: Any) -> list[str]:
    """Các nhà cung cấp AI đã ghi mốc ví hết tiền (khoá ops:wallet:<nhà cung cấp>)."""
    keys = [k async for k in redis.scan_iter(match=f"{_WALLET_PREFIX}*")]
    return sorted(str(k).removeprefix(_WALLET_PREFIX) for k in keys)


async def _ai_alerts(now: datetime) -> list[Alert]:
    redis = get_redis_client()
    alerts: list[Alert] = []
    for provider in await _wallet_providers(redis):
        raw = await redis.get(f"{_WALLET_PREFIX}{provider}")
        if not raw:
            continue
        ts, _, detail = str(raw).partition("|")
        try:
            alert = wallet_alert(provider, float(ts), now.timestamp(), detail)
        except ValueError:
            continue  # mục cảnh báo hỏng: bỏ qua
        if alert:
            alerts.append(alert)
    return alerts


async def run_checks() -> int:
    """Chạy mọi nhóm kiểm tra (nhóm này lỗi không chặn nhóm khác). Trả về số cảnh báo đã gửi."""
    now = datetime.now(tz=UTC)
    sent = 0
    for name, check in (("omar", _omar_alerts), ("drought", _drought_alerts), ("ai", _ai_alerts)):
        try:
            for alert in await check(now):
                sent += int(await _fire(alert))
        except Exception as exc:  # noqa: BLE001 - lượt sau thử lại
            logger.error("ops_alert_check_failed", check=name, error=exc)
    return sent
