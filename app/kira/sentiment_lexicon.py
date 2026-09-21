"""Lexicon-based 3-way sentiment for movie comments (positive/negative/neutral).

Matches labels in app/kira/sentiment.py and the Social Topic dashboard.
Used for bulk backfill when Kira is unavailable or for fast heuristic
labeling - see scripts/label_comment_sentiment_lexicon.py and
app/kira/sentiment_lexicon.md.
"""

from __future__ import annotations

import re

from app.services.d1 import MIN_CONTENT_LENGTH

VALID_SENTIMENTS = frozenset({"positive", "negative", "neutral"})

# Longer / more specific phrases first so "không xem" beats bare "xem".
_POSITIVE = (
    "ủng hộ phim",
    "ủng hộ",
    "phải xem",
    "đi xem liền",
    "muốn ra rạp",
    "mong chờ",
    "chờ phim",
    "book vé",
    "mua vé",
    "đỉnh cao",
    "đỉnh thật",
    "đỉnh vcl",
    "đỉnh vc",
    "hay quá trời",
    "hay vl",
    "hay vcl",
    "xuất sắc",
    "mãn nhãn",
    "cảm động",
    "diễn đạt",
    "đóng hay",
    "diễn sâu",
    "hoá thân",
    "hóa thân",
    "nhạc phim đã",
    "ost hay",
    "quay đẹp",
    "hình ảnh đẹp",
    "10 điểm",
    "9/10",
    "đã quá",
    "đã mắt",
    "đã tai",
    "đã đời",
    "tâm đắc",
    "đỉnh",
    "tuyệt",
    "xuất sắc",
    "cuốn",
    "chất",
    "xịn",
    "ưng",
    "mê",
    "thích",
    "yêu",
    "hay",
    "best",
    "love",
    "goat",
    "fire",
    "hóng",
    "chill",
)

_NEGATIVE = (
    "không đi xem",
    "chắc không đi",
    "không xem đâu",
    "không xem",
    "hết muốn xem",
    "xem không nổi",
    "bỏ giữa chừng",
    "tắt ngang",
    "phí tiền",
    "phí thời gian",
    "kịch bản lỏng",
    "logic kém",
    "thất vọng nặng",
    "thất vọng",
    "quảng cáo lố",
    "fake trailer",
    "overhyped",
    "thổi quá",
    "diễn đơ",
    "diễn gỗ",
    "diễn tệ",
    "casting sai",
    "cgi xấu",
    "hiệu ứng rẻ",
    "hết cứu",
    "bay màu",
    "dài dòng",
    "lê thê",
    "nhàm",
    "nhạt",
    "lạc đề",
    "sáo",
    "dở",
    "tệ hại",
    "tệ",
    "chán",
    "kém",
    "flop",
    "fail",
    "trash",
    "garbage",
    "nản",
    "ngán",
    "ói",
    "toang",
    "sập",
    "lừa",
    "gạt",
)

_NEUTRAL_QUESTION = (
    "rạp nào",
    "suất mấy",
    "giá vé",
    "bao nhiêu tiền",
    "bao nhiêu",
    "ngày nào chiếu",
    "chiếu chưa",
    "ở đâu xem",
    "link đâu",
    "ai đóng",
    "tên phim",
    "bao giờ ra",
    "bao giờ chiếu",
    "suất chiếu",
)

# Sarcasm / negation overrides: if these fire, prefer negative even with
# positive lexicon hits (unless a stronger "không hay sao" style flip).
_SARCASM_NEG = (
    "flop chắc",
    "chắc flop",
    "flop rồi",
    "hay quá =))",
    "đỉnh =))",
    "ủng hộ =))",
)

_POSITIVE_FLIP = (
    "không hay sao",
    "chán phải không? không",
)


def _count_hits(text: str, phrases: tuple[str, ...]) -> int:
    return sum(1 for p in phrases if p in text)


def classify_sentiment_lexicon(message: str | None) -> str | None:
    """Returns positive/negative/neutral, or None if message too short."""
    if not message or len(message.strip()) < MIN_CONTENT_LENGTH:
        return None

    raw = message.strip()
    text = raw.lower()
    # Normalize elongated vowels a bit: "hayyy" -> "hayy" still matches "hay"
    text_compact = re.sub(r"(.)\1{2,}", r"\1\1", text)

    if any(p in text_compact for p in _POSITIVE_FLIP):
        return "positive"
    if any(p in text_compact for p in _SARCASM_NEG):
        return "negative"

    pos = _count_hits(text_compact, _POSITIVE)
    neg = _count_hits(text_compact, _NEGATIVE)
    neu_q = _count_hits(text_compact, _NEUTRAL_QUESTION)

    # Question-only info seeking with no opinion words.
    if neu_q and pos == 0 and neg == 0:
        return "neutral"

    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    if pos == neg and pos > 0:
        # Tie with signals both ways — lean on trailing clause / "nhưng".
        if "nhưng" in text_compact or " nhưng " in f" {text_compact} ":
            # After "nhưng" usually carries the stance.
            after = text_compact.split("nhưng", 1)[-1]
            pos_a = _count_hits(after, _POSITIVE)
            neg_a = _count_hits(after, _NEGATIVE)
            if neg_a > pos_a:
                return "negative"
            if pos_a > neg_a:
                return "positive"
        return "neutral"

    # Emoji-only lean (weak).
    if any(e in raw for e in ("❤️", "😍", "🔥")) and neg == 0:
        return "positive"
    if "😂" in raw and pos == 0 and ("dở" in text_compact or "tệ" in text_compact or "flop" in text_compact):
        return "negative"

    return "neutral"
