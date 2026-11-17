"""Dấu hiệu A: số bình luận chê của một giờ cao bất thường so với cùng giờ đó trong 14 ngày trước.

Bất thường khi đủ cả ba (docs mục 2.4 A): chê >= ratio x trung vị cùng giờ-trong-ngày 14 ngày trước VÀ >= min_count; tỷ lệ
chê >= share_ratio x tỷ lệ chê thường (chỉ tổng tăng - bài viral - không tính). Trong ±6 giờ quanh một mốc sự kiện (premiere,
khởi chiếu...) thì vẫn ghi nhưng không "mạnh".
"""

from __future__ import annotations

from datetime import datetime, timedelta
from statistics import median
from typing import Any

from app.services.attack.signals import Signal

BASELINE_DAYS = 14
EVENT_WINDOW = timedelta(hours=6)
MIN_COUNT = 30
RATIO = 3.0
SHARE_RATIO = 1.5


def find_spikes(
    hours: list[dict[str, Any]],
    events: list[str],
    *,
    min_count: int = MIN_COUNT,
    ratio: float = RATIO,
    share_ratio: float = SHARE_RATIO,
) -> list[dict[str, Any]]:
    """hours: [{"hour": "YYYY-MM-DDTHH" giờ VN, "total", "negative"}] liên tục từng giờ (giờ trống = 0), cũ -> mới.
    events: các mốc 'YYYY-MM-DDTHH' giờ VN."""
    marks = [datetime.fromisoformat(e) for e in events]
    spikes = []
    for i in range(BASELINE_DAYS * 24, len(hours)):
        now = hours[i]
        past = [hours[i - 24 * k] for k in range(1, BASELINE_DAYS + 1)]  # cùng giờ-trong-ngày
        base = median(h["negative"] for h in past)
        base_share = sum(h["negative"] for h in past) / max(1, sum(h["total"] for h in past))
        share = now["negative"] / max(1, now["total"])
        if now["negative"] < min_count or now["negative"] < ratio * max(base, 1):
            continue
        if share < share_ratio * base_share:
            continue
        at = datetime.fromisoformat(now["hour"])
        near = any(abs(at - mark) <= EVENT_WINDOW for mark in marks)
        spikes.append(
            {
                **now,
                "baseline": base,
                "multiple": round(now["negative"] / max(base, 1), 1),
                "share": round(share, 3),
                "base_share": round(base_share, 3),
                "near_event": near,
                "strong": not near,
            }
        )
    return spikes


def spike_signal(hours: list[dict[str, Any]], events: list[str], *, since_hour: str | None = None) -> Signal:
    """Dấu hiệu A cho cửa sổ đang xét: chỉ các đợt tăng từ `since_hour` (giờ VN) trở đi. window = (giờ đầu, giờ cuối + 1) VN."""
    spikes = [s for s in find_spikes(hours, events) if not since_hour or s["hour"] >= since_hour]
    if not spikes:
        return Signal(key="spike", fired=False, strong=False, score=0.0)
    top = max(spikes, key=lambda s: s["multiple"])
    last = datetime.fromisoformat(max(s["hour"] for s in spikes)) + timedelta(hours=1)
    return Signal(
        key="spike",
        fired=True,
        strong=any(s["strong"] for s in spikes),
        score=top["multiple"],
        window=(min(s["hour"] for s in spikes), last.strftime("%Y-%m-%dT%H")),
        detail={"spikes": spikes[-24:], "series": hours[-72:]},
    )
