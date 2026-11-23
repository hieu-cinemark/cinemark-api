"""Cửa sổ thời gian quanh ngày chiếu và các tên rút gọn của phim hợp lệ trong cửa sổ đó."""

from __future__ import annotations

import re
from datetime import date, timedelta

from app.services.relevance.film_phrases import _STRONG_FILM_PHRASES
from app.services.relevance.text_rules import _contains_phrase, _squash, normalize_title

# --- Quy tắc "quanh ngày công chiếu" (release_window, 17/10/2026) ---
# Bài nhắc TÊN LOẠT PHIM (tên bỏ số phần / phần trước ":" / tên trong ngoặc: "Út Lan", "Shaun the Sheep", "Tee Yod", "Lên
# Hương") kèm từ ngữ rạp chiếu, đăng trong khoảng quanh ngày công chiếu, là về phim đang chiếu - kể cả khi tên là cụm từ
# thường ngày hay trùng tên phần trước. Bài phần cũ trong danh sách hashtag phần lớn là video đăng từ năm trước nên ngày đăng
# tách được chúng. Đo trên bài ẩn chỉ dùng ngày đăng thì nâng 1.695 bài lẫn nhiều rác; ngày + tên loạt + từ rạp
# còn khoảng 190 bài, phần lớn đúng ("SHAUN THE SHEEP hôm nay ra mắt... suất sớm", "#lenhuong đi coi về").
RELEASE_WINDOW_BEFORE_DAYS = 7

RELEASE_WINDOW_AFTER_DAYS = 60

_RELEASE_CONTEXT_PHRASES = (
    *_STRONG_FILM_PHRASES,
    "cinetour",
    "tham do",
    "di xem",
    "di coi",
    "xem phim",
    "coi phim",
    "phim hay",
    "dang chieu",
    "ra mat",
    "suat som",
    "showcase",
    "review phim",
    "gala",
)


def franchise_names(title: str | None) -> list[str]:
    """Tên loạt phim từ tên phim (đã bỏ dấu, bỏ số phần): cả tên, phần trước ":" / "(", và tên trong ngoặc - mỗi tên từ
    hai chữ và năm ký tự viết liền trở lên."""
    raw = str(title or "")
    names: list[str] = []
    for part in [raw, re.split(r"[:(（]", raw, maxsplit=1)[0], *re.findall(r"[(（]([^)）]+)[)）]", raw)]:
        words = [word for word in normalize_title(part).split() if not word.isdigit()]
        name = " ".join(words)
        if len(words) >= 2 and len(_squash(name)) >= 5 and name not in names:
            names.append(name)
    return names


def _to_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def in_release_window(content: str | None, movie: dict | None, posted_at: object) -> bool:
    """Xem phần chú thích RELEASE_WINDOW_*: đăng trong [công chiếu - BEFORE, công chiếu + AFTER] ngày, nhắc tên loạt phim
    và có từ ngữ rạp chiếu."""
    movie = movie or {}
    released, posted = _to_date(movie.get("released_at")), _to_date(posted_at)
    if not released or not posted:
        return False
    if (
        not released - timedelta(days=RELEASE_WINDOW_BEFORE_DAYS)
        <= posted
        <= released + timedelta(days=RELEASE_WINDOW_AFTER_DAYS)
    ):
        return False
    text = normalize_title(content)
    if not text:
        return False
    squashed = _squash(text)
    if not any(
        _contains_phrase(text, name) or _squash(name) in squashed for name in franchise_names(movie.get("title"))
    ):
        return False
    return any(_contains_phrase(text, phrase) for phrase in _RELEASE_CONTEXT_PHRASES)


# --- Tên rút gọn quanh ngày công chiếu (short_title, 21/10/2026) ---
# Bài booking / page chung hay chỉ viết tên ngắn: "Phim Scotty đang chiếu", "#scotty" cho "SCOTTY: GIẢI CỨU HOÀNG THƯỢNG" -
# cổng mentions_keyword_or_title cần đủ tên nên loại (đo 21/10: 4 bài booking Scotty của Fan Xi Nê Ma bị loại như vậy).
# Nhận tên rút gọn (phần trước / sau ":" hoặc " - ", tên trong ngoặc) khi bài đăng trong khoảng quanh ngày công chiếu -
# quảng bá thường bắt đầu cả tháng trước ngày chiếu nên cửa sổ trước rộng hơn RELEASE_WINDOW_BEFORE_DAYS.
SHORT_TITLE_BEFORE_DAYS = 30

SHORT_TITLE_AFTER_DAYS = 60

# Tên một chữ phải dài từ chừng này ký tự ("scotty") - chữ ngắn ("mai", "nha") là từ thường ngày.
SHORT_TITLE_MIN_SINGLE_WORD = 6


def short_title_names(title: str | None) -> list[str]:
    """Các tên rút gọn (đã bỏ dấu) của phim có tên dạng "A: B" / "A - B" / "A (B)": A, B và tên trong ngoặc - hai chữ trở
    lên (từ 8 ký tự viết liền), hoặc một chữ từ SHORT_TITLE_MIN_SINGLE_WORD ký tự. Tên không có phần tách thì rỗng (đã có
    cổng tên đầy đủ)."""
    raw = str(title or "")
    pieces = [*re.split(r"\s*[:：]\s*|\s+[-–]\s+", raw), *re.findall(r"[(（]([^)）]+)[)）]", raw)]
    if len(pieces) < 2:
        return []
    names: list[str] = []
    for piece in pieces:
        words = [
            word for word in normalize_title(re.sub(r"[(（][^)）]*[)）]", "", piece)).split() if not word.isdigit()
        ]
        name = " ".join(words)
        long_enough = (len(words) >= 2 and len(_squash(name)) >= 8) or (
            len(words) == 1 and len(name) >= SHORT_TITLE_MIN_SINGLE_WORD
        )
        if long_enough and name not in names and name != normalize_title(raw):
            names.append(name)
    return names
