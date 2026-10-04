"""Phân loại cảm xúc 3 nhóm dựa trên từ điển cho comment phim (positive/negative/neutral).

Dùng cùng nhãn với app/ai/tasks/sentiment.py và dashboard Social Topic. Dùng để
backfill hàng loạt khi Kira không dùng được hoặc để gắn nhãn nhanh theo heuristic -
xem scripts/label_comment_sentiment_lexicon.py và app/services/sentiment_lexicon.md.
"""

from __future__ import annotations

import re

from app.services.d1 import MIN_CONTENT_LENGTH

VALID_SENTIMENTS = frozenset({"positive", "negative", "neutral"})

# Cụm dài / cụ thể hơn đặt trước để "không xem" thắng "xem" trơ trọi.
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

# Ghi đè cho mỉa mai / phủ định: nếu khớp những cụm này thì ưu tiên negative kể cả khi
# có từ tích cực (trừ khi có kiểu đảo nghĩa mạnh hơn như "không hay sao").
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
    """Trả về positive/negative/neutral, hoặc None nếu message quá ngắn."""
    if not message or len(message.strip()) < MIN_CONTENT_LENGTH:
        return None

    raw = message.strip()
    text = raw.lower()
    # Chuẩn hoá bớt nguyên âm kéo dài: "hayyy" -> "hayy" vẫn khớp "hay"
    text_compact = re.sub(r"(.)\1{2,}", r"\1\1", text)

    if any(p in text_compact for p in _POSITIVE_FLIP):
        return "positive"
    if any(p in text_compact for p in _SARCASM_NEG):
        return "negative"

    pos = _count_hits(text_compact, _POSITIVE)
    neg = _count_hits(text_compact, _NEGATIVE)
    neu_q = _count_hits(text_compact, _NEUTRAL_QUESTION)

    # Chỉ hỏi thông tin, không có từ nào thể hiện ý kiến.
    if neu_q and pos == 0 and neg == 0:
        return "neutral"

    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    if pos == neg and pos > 0:
        # Hoà nhau khi có tín hiệu cả hai phía — dựa vào vế cuối / "nhưng".
        if "nhưng" in text_compact or " nhưng " in f" {text_compact} ":
            # Phần sau "nhưng" thường mang quan điểm.
            after = text_compact.split("nhưng", 1)[-1]
            pos_a = _count_hits(after, _POSITIVE)
            neg_a = _count_hits(after, _NEGATIVE)
            if neg_a > pos_a:
                return "negative"
            if pos_a > neg_a:
                return "positive"
        return "neutral"

    # Nghiêng theo emoji khi chỉ có emoji (yếu).
    if any(e in raw for e in ("❤️", "😍", "🔥")) and neg == 0:
        return "positive"
    if "😂" in raw and pos == 0 and ("dở" in text_compact or "tệ" in text_compact or "flop" in text_compact):
        return "negative"

    return "neutral"
