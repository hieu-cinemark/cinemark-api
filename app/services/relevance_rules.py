"""Các quy tắc độ liên quan có tính tất định, áp dụng lúc ingest, trước (và bất kể) lối
tắt chuỗi con theo từ khoá và bộ phân loại Kira.

Vì sao dùng quy tắc, không chỉ dùng model: đo ngày 2026-09-28, pipeline ingest nhận
mọi bài có nội dung chứa từ khoá mà không có model nào xem qua - nên video nước ngoài
dùng chung một hashtag không dấu (#memin: 17% số bài không phải tiếng Việt, phần lớn
là bài tiếng Tây Ban Nha về truyện tranh Mexico Memín) đi thẳng lên dashboard. Còn
PhoBERT (huấn luyện trên nhãn keyword_match) tự tin đánh dấu related cho các bài về
một phim *khác* cũng đang theo dõi (ví dụ "Lan Trinh trong Án Mạng Xém Hoàn Hảo" bị
gán cho Án Mạng Karaoke). Cả hai đều có tín hiệu rẻ, kiểm tra được:

  foreign_language_reason() - chính phần chữ của bài (đã bỏ hashtag, mention, URL và
      emoji) rõ ràng không phải tiếng Việt, hoặc textLanguage của TikTok nói vậy. Cố
      ý thận trọng: text ngắn/mơ hồ trả về None (không phán quyết) thay vì đoán.
  mentions_other_film() - bài hoàn toàn không nhắc tới phim mục tiêu nhưng lại nêu
      tên một phim khác đang theo dõi.

Mọi phim đang theo dõi đều là phim Việt; trên thực tế bài liên quan tới một phim đều
viết bằng tiếng Việt (có dấu hoặc không dấu). Bài bị loại được lưu trữ qua topic
Kafka ingest_decisions (-> lake writer, dưới bronze/entity=decisions/), nên một lần
loại nhầm ở đây vẫn phát lại được từ các file NDJSON trong lake thay vì từ một bảng
D1 giờ đã bị xoá.
"""

from __future__ import annotations

import re
import unicodedata

_STRIP_RE = re.compile(r"(?:#|@)\S+|https?://\S+|www\.\S+")
_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)

# Các chữ cái chỉ có trong tiếng Việt trong số các ngôn ngữ chữ Latinh mà pipeline này
# gặp (sau NFC). Có bất kỳ chữ nào là đủ kết luận.
_VI_CHARS = frozenset("ăâđêôơưàáảãạằắẳẵặầấẩẫậèéẻẽẹềếểễệìíỉĩịòóỏõọồốổỗộờớởỡợùúủũụừứửữựỳýỷỹỵ")
# Các từ tiếng Việt không dấu đồng thời không phải từ thông dụng trong tiếng Anh, Tây
# Ban Nha, Indonesia hay Tagalog - bằng chứng cho tiếng Việt không dấu.
_VI_WORDS = frozenset(
    [
        "khong",
        "ko",
        "hok",
        "cua",
        "nhung",
        "nguoi",
        "duoc",
        "voi",
        "nhieu",
        "rat",
        "minh",
        "roi",
        "nha",
        "nhe",
        "oi",
        "qua",
        "xem",
        "phim",
        "hay",
        "dep",
        "vui",
        "buon",
        "lam",
        "gi",
        "sao",
        "nay",
        "thi",
        "cung",
        "dang",
        "chua",
        "luon",
        "tui",
        "toi",
        "mng",
        "moi",
        "ve",
        "rap",
        "coi",
        "thay",
        "biet",
    ]
)
# Các hư từ thường gặp của những ngôn ngữ nước ngoài thực sự xuất hiện dưới các hashtag
# bị trùng (tiếng Anh, Tây Ban Nha/Bồ Đào Nha, Indonesia/Mã Lai, Tagalog). Có từ hai từ
# trở lên, không có tín hiệu tiếng Việt nào, nghĩa là text nước ngoài.
_FOREIGN_WORDS = frozenset(
    [
        "the",
        "and",
        "is",
        "are",
        "you",
        "your",
        "to",
        "of",
        "in",
        "for",
        "this",
        "that",
        "with",
        "my",
        "it",
        "on",
        "was",
        "not",
        "have",
        "what",
        "just",
        "we",
        "me",
        "el",
        "los",
        "las",
        "que",
        "con",
        "por",
        "para",
        "una",
        "del",
        "es",
        "lo",
        "mi",
        "se",
        "como",
        "pero",
        "muy",
        "mas",
        "eso",
        "esta",
        "nos",
        "le",
        "yang",
        "dan",
        "ini",
        "itu",
        "aku",
        "kamu",
        "tidak",
        "ada",
        "untuk",
        "dengan",
        "ang",
        "ng",
        "mga",
        "sa",
        "ko",
        "ako",
        "hindi",
        "naman",
    ]
)
_MIN_LETTERS = 12
# Từ vựng phim/điện ảnh: một caption tiếng nước ngoài dùng các từ này thường là fan
# hoặc nhà phát hành Việt viết bằng tiếng Anh về phim (đã thấy thực tế: "One more time
# for cinetourrr ... #nghihesonghihuu"), nên quy tắc hư từ nhường lại. Phán quyết theo
# bảng chữ viết và theo ngôn ngữ của nền tảng vẫn áp dụng.
_FILM_CONTEXT_WORDS = frozenset(
    [
        "cinema",
        "cinemas",
        "movie",
        "movies",
        "film",
        "films",
        "trailer",
        "teaser",
        "premiere",
        "cinetour",
        "theater",
        "theatre",
        "boxoffice",
        "showtime",
        "showtimes",
        "ticket",
        "tickets",
        "screening",
        "review",
    ]
)


def _body(text: str | None) -> str:
    # NFKC đưa Unicode trang trí ("𝐞𝐛𝐞́", chữ full-width) về chữ thường + dấu kết hợp, rồi
    # NFC ghép lại dấu tiếng Việt để _VI_CHARS khớp được.
    folded = unicodedata.normalize("NFC", unicodedata.normalize("NFKC", text or ""))
    return _STRIP_RE.sub(" ", folded)


def _is_latin(ch: str) -> bool:
    return "LATIN" in unicodedata.name(ch, "")


def foreign_language_reason(text: str | None, text_language: str | None = None) -> str | None:
    """Một chuỗi lý do ngắn khi bài rõ ràng không phải tiếng Việt, ngược lại là None (tiếng
    Việt, hoặc không đủ chữ để kết luận)."""
    body = _body(text)
    lowered = body.lower()
    has_vi_chars = any(ch in _VI_CHARS for ch in lowered)
    if has_vi_chars:
        return None

    lang = (text_language or "").strip().lower()
    if lang and lang not in ("vi", "un", "und"):
        return f"platform_language={lang}"

    letters = [ch for ch in body if ch.isalpha()]
    if len(letters) < _MIN_LETTERS:
        return None
    non_latin = sum(1 for ch in letters if not _is_latin(ch))
    if non_latin / len(letters) >= 0.5:
        return "non_latin_script"

    words = [unicodedata.normalize("NFD", w).encode("ascii", "ignore").decode() for w in _WORD_RE.findall(lowered)]
    if sum(1 for w in words if w in _VI_WORDS):
        return None
    foreign_hits = sum(1 for w in words if w in _FOREIGN_WORDS)
    # "cinetourrr" -> "cinetour": gộp các chữ bị kéo dài trước khi tra.
    film_context = any(re.sub(r"(.)\1{2,}", r"\1", w) in _FILM_CONTEXT_WORDS for w in words)
    if len(words) >= 3 and foreign_hits >= 2 and not film_context:
        return "foreign_function_words"
    return None


def normalize_title(value: str | None) -> str:
    """Dạng không phân biệt dấu/hoa thường/dấu câu ("Án Mạng Karaoke" -> "an mang
    karaoke"), với đ chuyển thành d."""
    stripped = "".join(
        ch for ch in unicodedata.normalize("NFD", (value or "").lower()) if unicodedata.category(ch) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", " ", stripped.replace("đ", "d")).strip()


def _contains_phrase(haystack: str, phrase: str) -> bool:
    return bool(phrase) and f" {phrase} " in f" {haystack} "


def mentions_other_film(
    content: str | None, movie_title: str | None, keyword: str | None, other_titles: list[str]
) -> str | None:
    """Phim đang theo dõi khác mà bài này nêu tên, khi bài hoàn toàn không nhắc tới phim mục
    tiêu (tên phim hoặc từ khoá, không phân biệt dấu, tính cả dạng hashtag). Ngược lại là
    None. Bỏ qua tên ngắn hoặc lồng nhau: tên một-hai chữ như "Anh Hùng"/"Loạn Thế" là
    cụm từ thường ngày, còn "Út Lan" với "Út Lan 2" sẽ khớp lẫn nhau."""
    text = normalize_title(content)
    squashed = text.replace(" ", "")
    own = normalize_title(movie_title)
    kw = normalize_title((keyword or "").lstrip("#"))
    if _contains_phrase(text, own) or (own and own.replace(" ", "") in squashed):
        return None
    if kw and (_contains_phrase(text, kw) or kw.replace(" ", "") in squashed):
        return None
    for title in other_titles:
        other = normalize_title(title)
        if len(other.split()) < 3 or not own or other in own or own in other:
            continue
        if _contains_phrase(text, other) or other.replace(" ", "") in squashed:
            return title
    return None
