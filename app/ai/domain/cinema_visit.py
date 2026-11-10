"""Nhận diện câu nói rõ về việc đi rạp (khía cạnh `di_rap`), bằng luật, không gọi AI.

Phạm vi hẹp có chủ đích: "xem rồi", giá vé hay lịch chiếu đơn thuần không tính là bàn chuyện đi rạp.
"""

import re
import unicodedata

_VISIT = re.compile(
    r"\b(?:di|toi|den)\s+rap\b"
    r"|\bdi\s+(?:xem|coi)\s+phim\b"
    r"|\b(?:minh|toi|tao|tui|em|anh|chi|ban|nay|mai|qua|vua|moi|bi|dinh|muon|di)\s+(?:se\s+)?ra\s+rap\b"
    r"|\bra\s+rap\s+(?:xem|coi)\b"
    r"|\bcheck[\s-]?in\s+(?:(?:o|tai)\s+)?rap\b"
)
_REFUSAL = re.compile(r"\b(?:khong|chang|cha)\s+muon\s+(?:di\s+rap|ra\s+rap|di\s+(?:xem|coi)\s+phim)\b")
_EAGER = re.compile(r"\b(?:hao huc|mong cho|rat muon)\s+(?:duoc\s+)?(?:di\s+rap|ra\s+rap|di\s+(?:xem|coi)\s+phim)\b")
_NEGATED_EAGER = re.compile(r"\b(?:khong|chang|cha|chua)\s+(?:con\s+)?(?:hao huc|mong cho|rat muon)\b")


def cinema_visit_aspect(message: str) -> str | None:
    folded = "".join(c for c in unicodedata.normalize("NFD", message.lower()) if not unicodedata.combining(c))
    folded = re.sub(r"\s+", " ", folded.replace("đ", "d")).strip()
    if not _VISIT.search(folded):
        return None
    # Không coi xem online/ở nhà là đi rạp chỉ vì có chữ "đi xem phim".
    if "rap" not in folded and re.search(r"\b(?:o nha|tai nha|online|netflix|youtube)\b", folded):
        return None
    if _REFUSAL.search(folded):
        return "di_rap:-"
    if _EAGER.search(folded) and not _NEGATED_EAGER.search(folded):
        return "di_rap:+"
    return "di_rap:0"
