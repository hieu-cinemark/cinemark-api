"""Quy tắc ra trạng thái (docs mục 2.5) - hàm thuần, cùng dữ liệu luôn ra cùng kết quả.

| insufficient | < MIN_COMMENTS_7D bình luận trong 7 ngày                                    |
| clear        | không dấu hiệu nào strong (dấu hiệu chỉ fired vẫn ghi lý do để người đọc biết) |
| suspicious   | có >= 1 dấu hiệu strong                                                     |
| coordinated  | >= 2 dấu hiệu strong có window chồng nhau (vd đợt tăng A chứa cụm B)         |

Ngưỡng của từng dấu hiệu ở spikes.py / similar.py - chưa thử trên dữ liệu cũ (docs mục 2.9), sẽ chỉnh sau backtest.
Độ tin cậy dựa trên độ phủ dữ liệu, KHÔNG phải xác suất bị tấn công.
"""

from __future__ import annotations

from typing import Any

from app.services.attack.signals import Signal, overlaps

MIN_COMMENTS_7D = 200

STATUS_TEXT = {
    "insufficient": "Không đủ dữ liệu",
    "clear": "Không phát hiện bất thường",
    "suspicious": "Có dấu hiệu nghi vấn",
    "coordinated": "Có dấu hiệu phối hợp mạnh",
}
DISCLAIMER = "Kết quả mang tính khuyến cáo, dựa trên dữ liệu thu được, không đầy đủ 100%."


def decide(signals: list[Signal], coverage: dict[str, Any]) -> tuple[str, str, list[str]]:
    """(status, confidence, reasons). coverage: {"comments_7d", "platforms"}."""
    comments = int(coverage.get("comments_7d") or 0)
    platforms = int(coverage.get("platforms") or 0)
    confidence = "high" if comments >= 2000 and platforms >= 2 else "medium" if comments >= 500 else "low"
    if comments < MIN_COMMENTS_7D:
        return "insufficient", "low", [f"Chỉ có {comments} bình luận trong 7 ngày (cần ít nhất {MIN_COMMENTS_7D})."]
    fired = [s for s in signals if s.fired]
    strong = [s for s in fired if s.strong]
    reasons = [_reason(s) for s in fired]
    if not strong:
        return "clear", confidence, reasons
    for i, a in enumerate(strong):
        if any(overlaps(a.window, b.window) for b in strong[i + 1 :]):
            return "coordinated", confidence, reasons
    return "suspicious", confidence, reasons


def _reason(signal: Signal) -> str:
    if signal.key == "spike":
        return f"Bình luận chê tăng gấp {signal.score:g} lần mức thường của cùng khung giờ" + (
            "" if signal.strong else " (trùng thời điểm sự kiện của phim)"
        )
    if signal.key == "similar":
        top = (signal.detail.get("clusters") or [{}])[0]
        return (
            f"{top.get('size', 0)} bình luận chê có nội dung gần như giống nhau từ {int(signal.score)} tài khoản, "
            f"{round((top.get('burst') or 0) * 100)}% đăng trong 2 giờ"
        )
    return signal.key
