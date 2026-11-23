"""Omar: trạng thái tạm dừng chờ hạn mức/key mới và kết quả lượt gần nhất.

Tách từ omar_tiktok.py (17/11/2026); omar_tiktok.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.clients.omar import (
    OmarStop,
)
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.accounts.config_db.omar import get_omar_api_settings

logger = get_logger(__name__)


# Tạm dừng khi hết hạn mức: lượt kết thúc vì dùng hết hạn mức ngày (hoặc key sai) thì ghi lại tới khi nào
# chạy được - scheduler không gọi Omar nữa (kể cả /usage) cho tới lúc đó rồi mở lượt NGAY khi hạn mức reset, không đợi
# mốc giờ kế tiếp. Con trỏ hashtag / hàng đợi bình luận / tìm kiếm vốn lưu sau mỗi trang nên lượt sau đi tiếp đúng chỗ.
_PAUSE_KEY = f"{REDIS_KEY_PREFIX}omar:paused"
_LAST_ROUND_KEY = f"{REDIS_KEY_PREFIX}omar:last_round"
# Key sai/hết hạn: thử lại khi đổi key, hoặc sau chừng này giờ nếu vẫn key cũ.
_KEY_PAUSE_HOURS = 6
# Hạn mức reset xong chừng này giây mới mở lượt - tránh gọi đúng lúc phía Omar chưa kịp reset.
_RESUME_SLACK_SECONDS = 60


def key_fingerprint(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()[:12] if api_key else ""


def _log_stop(exc: OmarStop) -> None:
    quota = str(exc).startswith("daily_limit")
    # Hết hạn mức ngày là chuyện bình thường của chế độ chạy liên tục - chỉ báo Telegram khi key có vấn đề.
    if quota:
        logger.info("omar_round_stopped", error=exc)
    else:
        logger.error("omar_round_stopped", error=exc, telegram=True)


async def get_pause() -> dict[str, Any] | None:
    try:
        raw = await get_redis_client().get(_PAUSE_KEY)
        return json.loads(raw) if raw else None
    except Exception as exc:  # noqa: BLE001 - không đọc được thì coi như không dừng (lượt tự kiểm tra hạn mức)
        logger.warning("omar_pause_read_failed", error=exc)
        return None


async def set_pause(reason: str, until: datetime, *, api_key: str = "") -> None:
    entry = {
        "reason": reason,  # "quota" (hết hạn mức ngày) | "key" (key sai/hết hạn)
        "until": until.astimezone(UTC).isoformat(),
        "at": datetime.now(tz=UTC).isoformat(),
        "key": key_fingerprint(api_key),
    }
    ttl = max(60, int((until - datetime.now(tz=UTC)).total_seconds()) + 7 * 24 * 3600)
    await get_redis_client().set(_PAUSE_KEY, json.dumps(entry), ex=ttl)
    logger.info("omar_paused", **entry)


async def clear_pause() -> None:
    await get_redis_client().delete(_PAUSE_KEY)


async def pause_over(pause: dict[str, Any]) -> bool:
    """Hết dừng chưa: tới giờ `until`, hoặc (dừng vì key) key trong D1 đã đổi."""
    try:
        until = datetime.fromisoformat(pause["until"])
    except KeyError, TypeError, ValueError:
        return True
    if datetime.now(tz=UTC) >= until:
        return True
    if pause.get("reason") == "key":
        row = await get_omar_api_settings()
        current = key_fingerprint(((row or {}).get("api_key") or "").strip())
        return bool(current) and current != pause.get("key")
    return False


async def _pause_after_round(stats: dict[str, Any], api_key: str, reset_at: datetime | None, exhausted: bool) -> None:
    """Ghi lại khi nào lượt kế tiếp chạy được, theo lý do lượt vừa dừng."""
    from app.services.omar.omar_tiktok import fetch_usage  # vòng import: omar_tiktok dùng module này

    stopped = str(stats.get("stopped") or "")
    now = datetime.now(tz=UTC)
    if stopped.startswith(("http_401", "http_403")):
        await set_pause("key", now + timedelta(hours=_KEY_PAUSE_HOURS), api_key=api_key)
        return
    if not (stopped.startswith("daily_limit") or exhausted):
        return
    # Đọc lại /usage (không tính vào hạn mức) để có giờ reset chính xác; không đọc được thì dùng giờ reset lúc mở lượt.
    usage = await fetch_usage(api_key)
    day = (usage or {}).get("day") or {}
    if day.get("resets_in") is not None:
        reset_at = now + timedelta(seconds=int(day["resets_in"]))
        if int(day.get("remaining") or 0) > settings.omar_daily_reserve and not stopped.startswith("daily_limit"):
            return  # vẫn còn hạn mức (vd. gói vừa được nâng) - lượt sau chạy bình thường
    await set_pause("quota", (reset_at or now + timedelta(hours=1)) + timedelta(seconds=_RESUME_SLACK_SECONDS))


async def last_round() -> dict[str, Any] | None:
    try:
        raw = await get_redis_client().get(_LAST_ROUND_KEY)
        return json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        return None


# Trạng thái lượt đang/vừa chạy - cho tab Omar trên dashboard (app/api/routes/omar.py) và scheduler (không mở lượt chồng).
_round: dict[str, Any] = {"running": False, "trigger": None, "started_at": None, "finished_at": None, "stats": {}}
