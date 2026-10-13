"""Cảnh báo vận hành qua Telegram - scheduler (app/services/scheduler.py) gọi run_checks() mỗi
settings.ops_alerts_interval_minutes phút:

  1. Omar: đã dùng >= ops_omar_quota_alert_pct % hạn mức ngày; gói hết hạn trong ops_omar_expiry_alert_days ngày (và lần
     nữa khi còn <= 1 ngày, hoặc đã hết hạn).
  2. Omar: tỉ lệ request lỗi của lượt đang chạy / vừa xong trong 1 giờ qua > ops_omar_error_rate_alert (đủ
     ops_omar_error_min_attempts lần gọi).
  3. Nguồn cạn: không có bài TikTok / comment mới (MAX(scraped_at) trên D1) trong ops_post/comment_drought_hours giờ.
  4. Ví AI: Kira (app/ai/client.py) / Jev (app/ai/jev.py) trả 402 hoặc lỗi hết số dư - đường gọi AI ghi mốc vào Redis qua
     note_ai_wallet_error / note_jev_response, ở đây chỉ đọc mốc. Chi phí Jev hôm nay (note_jev_tokens cộng dồn token đầu
     vào theo ngày giờ VN) vượt ops_jev_daily_usd_alert.

Mỗi cảnh báo chống lặp bằng một khoá Redis SET NX có TTL: lặp lại sau ops_alert_repeat_hours, riêng hạn mức / hạn gói
Omar / chi phí Jev thì khoá theo ngày (mỗi ngày một lần). Gửi thẳng send_telegram_message: field telegram=True của log
trong service này chỉ là một field, không có gì đọc nó để gửi Telegram.

Module này chỉ import nhẹ ở đầu (Redis, Telegram, config) vì app/ai/client.py và app/ai/jev.py import nó; phần nặng
(Omar, D1, Supabase) import trong hàm."""

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
_JEV_TOKENS_PREFIX = f"{REDIS_KEY_PREFIX}ops:jev_input_tokens:"
_DAY_TTL_SECONDS = 26 * 3600
_REDIS_TIMEOUT_SECONDS = 2.0
# Token Jev cộng dồn trong tiến trình rồi mới ghi Redis (Jev chạy hàng chục request song song - không ghi mỗi request).
_JEV_FLUSH_SECONDS = 30.0


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
                f"{resets_in / 3600:.0f} giờ nữa mới reset - hạn mức không đủ cho cả ngày, cân nhắc nâng gói.",
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
            tier = None
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


def jev_spend_alert(input_tokens: int, usd_per_m_input: float, now: datetime) -> Alert | None:
    usd = input_tokens / 1e6 * usd_per_m_input
    if usd <= settings.ops_jev_daily_usd_alert:
        return None
    return Alert(
        f"jev_spend:{vn_date(now)}",
        _DAY_TTL_SECONDS,
        f"⚠️ Jev hôm nay đã tốn khoảng ${usd:.2f} ({input_tokens:,} token đầu vào), vượt ngưỡng "
        f"${settings.ops_jev_daily_usd_alert:g}.",
    )


_KIRA_WALLET_CODES = ("vnd_balance_exhausted", "insufficient_quota", "insufficient_balance")
_JEV_WALLET_WORDS = ("balance", "credit", "quota", "insufficient", "payment")


def is_kira_wallet_error(exc: BaseException) -> bool:
    """openai.APIStatusError 402, hoặc mã lỗi hết số dư (OpenAI trả insufficient_quota kèm 429)."""
    if getattr(exc, "status_code", None) == 402:
        return True
    blob = f"{getattr(exc, 'code', '') or ''} {exc}".lower()
    return any(code in blob for code in _KIRA_WALLET_CODES)


def is_jev_wallet_error(status_code: int, body: str) -> bool:
    if status_code in (402, 403):
        return True
    return status_code >= 400 and any(word in (body or "").lower() for word in _JEV_WALLET_WORDS)


# --- Ghi mốc từ đường gọi AI (không bao giờ raise, không làm chậm lời gọi quá _REDIS_TIMEOUT_SECONDS) ---------------


async def _record_wallet_error(provider: str, detail: str) -> None:
    try:
        await asyncio.wait_for(
            get_redis_client().set(f"{_WALLET_PREFIX}{provider}", f"{time.time()}|{detail[:150]}", ex=24 * 3600),
            timeout=_REDIS_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001 - mất mốc chỉ làm thiếu một cảnh báo
        logger.warning("ops_wallet_error_record_failed", provider=provider, error=str(exc)[:200])


async def note_ai_wallet_error(provider: str, exc: BaseException) -> None:
    """Gọi trong except của app/ai/client.call_ai."""
    if settings.ops_alerts_enabled and is_kira_wallet_error(exc):
        await _record_wallet_error(provider, f"{getattr(exc, 'status_code', '')} {exc}".strip())


async def note_jev_response(status_code: int, body: str) -> None:
    """Gọi ngay sau mỗi request Jev (body chỉ cần khi status >= 400)."""
    if settings.ops_alerts_enabled and status_code >= 400 and is_jev_wallet_error(status_code, body):
        await _record_wallet_error("jev", f"HTTP {status_code} {(body or '')[:120]}")


_jev_pending_tokens = 0
_jev_last_flush = 0.0


async def note_jev_tokens(input_tokens: int) -> None:
    """Cộng token đầu vào của một request Jev vào tổng ngày (giờ VN) trong Redis - ghi dồn mỗi _JEV_FLUSH_SECONDS."""
    global _jev_pending_tokens, _jev_last_flush
    if not settings.ops_alerts_enabled:
        return
    _jev_pending_tokens += max(0, int(input_tokens or 0))
    if not _jev_pending_tokens or time.monotonic() - _jev_last_flush < _JEV_FLUSH_SECONDS:
        return
    tokens, _jev_pending_tokens = _jev_pending_tokens, 0
    _jev_last_flush = time.monotonic()
    key = f"{_JEV_TOKENS_PREFIX}{vn_date(datetime.now(tz=UTC))}"
    try:
        pipe = get_redis_client().pipeline()
        pipe.incrby(key, tokens)
        pipe.expire(key, 3 * 24 * 3600)
        await asyncio.wait_for(pipe.execute(), timeout=_REDIS_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - để lần ghi sau cộng lại
        _jev_pending_tokens += tokens
        logger.warning("ops_jev_tokens_record_failed", error=str(exc)[:200])


# --- Lượt kiểm tra (scheduler) -----------------------------------------------------------------------------------


async def _fire(alert: Alert) -> bool:
    """Gửi nếu chưa gửi cùng khoá trong TTL. True khi đã gửi."""
    if not await get_redis_client().set(f"{_ALERT_PREFIX}{alert.key}", "1", nx=True, ex=max(60, alert.ttl)):
        return False
    logger.warning("ops_alert_sent", alert=alert.key, text=alert.text)
    await send_telegram_message(alert.text)
    return True


async def _omar_alerts(now: datetime) -> list[Alert]:
    from app.services import omar_tiktok
    from app.services.platform_config_db import get_omar_api_settings

    alerts: list[Alert] = []
    error = omar_error_alert(omar_tiktok.round_status(), now)
    if error:
        alerts.append(error)
    api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
    if api_key:
        alerts += omar_quota_alerts(await omar_tiktok.fetch_usage(api_key), now)
    return alerts


async def _drought_alerts(now: datetime) -> list[Alert]:
    from app.services.d1 import d1_query

    alerts: list[Alert] = []
    checks = (
        ("posts_tiktok", "bài TikTok", "SELECT MAX(scraped_at) AS last FROM posts WHERE platform = 'tiktok'", "post"),
        ("comments", "comment", "SELECT MAX(scraped_at) AS last FROM comments", "comment"),
    )
    for kind, label, sql, which in checks:
        rows = await d1_query(sql)
        if rows is None:  # D1 chưa cấu hình / lỗi: không kết luận được
            continue
        hours = settings.ops_post_drought_hours if which == "post" else settings.ops_comment_drought_hours
        alert = drought_alert(kind, label, (rows[0] if rows else {}).get("last"), now, hours)
        if alert:
            alerts.append(alert)
    return alerts


async def _ai_alerts(now: datetime) -> list[Alert]:
    from app.ai.jev import USD_PER_M_INPUT

    redis = get_redis_client()
    alerts: list[Alert] = []
    for provider in ("kira", "bee", "jev"):
        raw = await redis.get(f"{_WALLET_PREFIX}{provider}")
        if not raw:
            continue
        ts, _, detail = str(raw).partition("|")
        try:
            alert = wallet_alert(provider, float(ts), now.timestamp(), detail)
        except ValueError:
            continue
        if alert:
            alerts.append(alert)
    tokens = int(await redis.get(f"{_JEV_TOKENS_PREFIX}{vn_date(now)}") or 0)
    spend = jev_spend_alert(tokens, USD_PER_M_INPUT, now)
    if spend:
        alerts.append(spend)
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
            logger.error("ops_alert_check_failed", check=name, error=str(exc)[:300])
    return sent
