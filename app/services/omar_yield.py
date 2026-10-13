"""Hiệu quả từng nguồn cào Omar (từ 2026-10-13): mỗi ngày (giờ VN) đếm cho từng nguồn - hashtag, search (tìm theo từ
khoá), channel (cào theo kênh), comments (bình luận tuần) - số request, số video/bình luận gửi lên pipeline và số MỚI THẬT
(chưa từng thấy - lần đầu vào tập chống trùng dùng chung với spider). Số liệu cho tab Omar và để chia ngân sách lượt Omar
theo hiệu quả (adaptive_share): nguồn mang về nhiều bài mới trên mỗi request hơn hashtag thì được phần lớn hơn.

Redis hash omar:yield:<YYYY-MM-DD>, trường "<nguồn>:requests" / "<nguồn>:sent" / "<nguồn>:new", giữ 30 ngày."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

SOURCES = ("hashtag", "search", "channel", "comments")
_VN = ZoneInfo("Asia/Ho_Chi_Minh")
_TTL = 30 * 24 * 3600
# Cần ít nhất chừng này request trong cửa sổ đo mới chỉnh ngân sách - ít hơn thì số liệu chưa đáng tin.
MIN_REQUESTS = 200


def _key(day: str) -> str:
    return f"{REDIS_KEY_PREFIX}omar:yield:{day}"


def _today() -> str:
    return datetime.now(_VN).date().isoformat()


async def add(source: str, *, requests: int = 0, sent: int = 0, new: int = 0) -> None:
    """Cộng dồn số liệu của ngày hôm nay. Lỗi Redis bỏ qua - đo hiệu quả không được làm hỏng lượt cào."""
    if not (requests or sent or new):
        return
    key = _key(_today())
    try:
        pipe = get_redis_client().pipeline()
        for field, value in (("requests", requests), ("sent", sent), ("new", new)):
            if value:
                pipe.hincrby(key, f"{source}:{field}", value)
        pipe.expire(key, _TTL)
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001
        logger.warning("omar_yield_record_failed", error=str(exc)[:200])


def _empty() -> dict[str, int]:
    return {"requests": 0, "sent": 0, "new": 0}


async def daily(days: int = 7) -> list[dict[str, Any]]:
    """[{day, sources: {nguồn: {requests, sent, new}}}] cho `days` ngày gần nhất, mới nhất trước."""
    redis = get_redis_client()
    start = datetime.now(_VN).date()
    out = []
    for offset in range(days):
        day = (start - timedelta(days=offset)).isoformat()
        raw = await redis.hgetall(_key(day)) or {}
        sources = {source: _empty() for source in SOURCES}
        for field, value in raw.items():
            source, _, metric = str(field).partition(":")
            if source in sources and metric in sources[source]:
                sources[source][metric] = int(value or 0)
        out.append({"day": day, "sources": sources})
    return out


def totals(days: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Cộng các ngày, kèm số mới trên 100 request."""
    out = {source: _empty() for source in SOURCES}
    for day in days:
        for source, values in day["sources"].items():
            for metric, value in values.items():
                out[source][metric] += value
    for values in out.values():
        values["new_per_100"] = round(values["new"] * 100 / values["requests"], 2) if values["requests"] else None
    return out


def share_for(source: str, base: float, measured: dict[str, dict[str, Any]]) -> float:
    """Phần ngân sách của `source` (search/channel): base nhân tỷ lệ hiệu quả của nó so với hashtag, kẹp trong
    [settings.omar_adaptive_min_share, settings.omar_adaptive_max_share]. Chưa đủ số liệu thì giữ base."""
    mine, ref = measured.get(source) or {}, measured.get("hashtag") or {}
    if (mine.get("requests") or 0) < MIN_REQUESTS or (ref.get("requests") or 0) < MIN_REQUESTS:
        return base
    mine_rate = mine["new"] / mine["requests"]
    ref_rate = ref["new"] / ref["requests"]
    ratio = 2.5 if ref_rate <= 0 else min(2.5, max(0.25, mine_rate / ref_rate))
    return round(min(settings.omar_adaptive_max_share, max(settings.omar_adaptive_min_share, base * ratio)), 3)


async def adaptive_shares() -> dict[str, float]:
    """{"search": phần, "channel": phần} theo hiệu quả settings.omar_adaptive_days ngày gần nhất (tắt thì trả base)."""
    base = {"search": settings.omar_search_budget_share, "channel": settings.omar_channels_budget_share}
    if not settings.omar_adaptive_budget:
        return base
    try:
        measured = totals(await daily(settings.omar_adaptive_days))
    except Exception as exc:  # noqa: BLE001
        logger.warning("omar_yield_read_failed", error=str(exc)[:200])
        return base
    return {source: share_for(source, share, measured) for source, share in base.items()}
