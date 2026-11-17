"""Kiểu dữ liệu chung của các dấu hiệu."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# Bằng chứng tối đa mỗi dấu hiệu (lưu vào attack_signals.signals_json, hiện trên trang).
MAX_EVIDENCE = 10


@dataclass
class Signal:
    key: str  # "spike" | "similar"
    fired: bool  # vượt ngưỡng
    strong: bool  # đủ mạnh để tự nâng trạng thái
    score: float  # số đo chính (bội số so với mức thường, số tài khoản...)
    window: tuple[str, str] | None = None  # khoảng thời gian (ISO UTC) - đối chiếu dấu hiệu trùng nhau
    evidence: list[dict[str, Any]] = field(default_factory=list)
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["window"] = list(self.window) if self.window else None
        out["evidence"] = self.evidence[:MAX_EVIDENCE]
        return out


def overlaps(a: tuple[str, str] | None, b: tuple[str, str] | None) -> bool:
    return bool(a and b and a[0] < b[1] and b[0] < a[1])


def mask_name(name: str | None) -> str:
    """Che bớt tên tài khoản khi hiện bằng chứng: 'Nguyễn Văn An' -> 'Ng*** V* A*' (từ >= 4 chữ giữ 2 chữ đầu, từ ngắn
    hơn giữ 1)."""
    parts = (name or "").split()
    return " ".join(p[:2] + "*" * min(3, len(p) - 2) if len(p) >= 4 else p[:1] + "*" for p in parts) or "Ẩn danh"
