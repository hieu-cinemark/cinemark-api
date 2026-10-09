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
from collections.abc import Iterable

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


# Từ vựng điện ảnh *mạnh* (đã bỏ dấu, chữ thường): đi cùng tên phim đích thì gần như chắc chắn
# là bài về phim chiếu rạp. Cố ý KHÔNG có "phim" đứng một mình - bài review phim khác ("PHIM:
# LAN HƯƠNG NHƯ CỐ") hay tóm tắt phim truyền hình cũng có chữ đó, và đã lọt như vậy ngày
# 2026-10-05. Cũng không có "rap": bỏ dấu thì "rạp" trùng "rap" (nhạc).
_STRONG_FILM_PHRASES = (
    "dien anh",
    "man anh",
    "ra rap",
    "tai rap",
    "rap chieu",
    "phong chieu",
    "khoi chieu",
    "cong chieu",
    "suat chieu",
    "lich chieu",
    "chieu rap",
    "phong ve",
    "dat ve",
    "mua ve",
    "trailer",
    "teaser",
    "poster",
    "hau truong",
    "ban dung",
    "cung may",
    "khai may",
    "bam may",
    "dong may",
    "ngay quay",
    "doan phim",
    "ekip",
    "e kip",
    "du an",
    "lien hoan phim",
    "lhp",
    "dien vien",
    "vai dien",
    "dao dien",
    "nha san xuat",
    "nha phat hanh",
    "box office",
    "cgv",
    "lotte cinema",
    "galaxy cinema",
    "beta cinemas",
    "cinestar",
    "premiere",
    "showtime",
)
# Từ điện ảnh mạnh phải cách tên phim đích không quá chừng này từ: "Người Được Chọn dự kiến
# khởi chiếu" là về phim này, còn "...chưa phải người được chọn. Phim Madam T | Khởi chiếu
# 25.09" là bài của phim khác chỉ dùng cụm từ đó (gặp thật 2026-10-05).
_STRONG_PHRASE_WINDOW = 4
_QUOTE_PAIRS = (('"', '"'), ("“", "”"), ("'", "'"), ("‘", "’"), ("«", "»"))


def _squash(value: str) -> str:
    return value.replace(" ", "")


# Từ vựng âm nhạc (đã bỏ dấu): tên phim trùng tên bài hát rất hay gặp ("Người Được Chọn" của Ali
# Hoàng Dương). Bài có các từ này chỉ được giữ bằng tín hiệu phim trực tiếp, không bằng ngoặc kép.
_MUSIC_PHRASES = (
    "bai hat",
    "ca khuc",
    "nghe nhac",
    "am nhac",
    "giai dieu",
    "ca tu",
    "lyric",
    "lyrics",
    "stream",
    "karaoke",
    "beat",
    "cover",
    "album",
    "single",
    "mv",
    "outnow",
    "out now",
    "rap",
    "rapviet",
    "trinh dien",
    "trinh bay",
    "san khau",
)


def _hashtags(content: str | None) -> set[str]:
    """Các hashtag của bài ở dạng bỏ dấu, viết liền ("#TrạiBuônNgười" -> "traibuonnguoi")."""
    raw = unicodedata.normalize("NFC", content or "")
    return {_squash(normalize_title(tag)) for tag in re.findall(r"#(\w+)", raw)} - {""}


def _distinctive_title(title: str) -> bool:
    """Tên đủ đặc trưng để hashtag của nó là tín hiệu phim: từ ba chữ, hoặc viết liền dài từ 10 ký
    tự. Tên ngắn như "Anh Hùng"/"Loạn Thế" là cụm từ thường ngày, hashtag của chúng không nói lên gì."""
    return len(title.split()) >= 3 or len(_squash(title)) >= 10


def film_context_reason(
    content: str | None, movie: dict | None = None, *, allow_title_hashtag: bool = False, allow_names: bool = True
) -> str | None:
    """Lý do bài được coi là nói về ĐÚNG phim đích (movie["title"]) như một phim chiếu rạp, hoặc
    None. Theo thứ tự:
      - "cast"/"director": tên đạo diễn/diễn viên (từ hai chữ trở lên), kể cả dạng hashtag
        (#StevenNguyen) - bỏ qua khi `allow_names=False` (tin đời tư của diễn viên cũng nhắc tên);
      - "phim_title": "phim <tên>" / "điện ảnh <tên>", kể cả dạng hashtag (#PhimNguoiDuocChon);
      - "strong_near_title": một từ điện ảnh mạnh (khởi chiếu, ra rạp, trailer, phòng vé...) cách
        tên phim không quá _STRONG_PHRASE_WINDOW từ;
      - "quoted_title": tên phim trong ngoặc kép VÀ bài có chữ "phim"/"điện ảnh" VÀ không có từ
        vựng âm nhạc - ngoặc kép đánh dấu tên tác phẩm, nhưng tác phẩm đó có thể là bài hát;
      - "title_hashtag": chỉ khi `allow_title_hashtag` - hashtag đúng bằng tên phim (#traibuonnguoi)
        và tên đủ đặc trưng. Caption TikTok/Threads thường chỉ có vậy, còn nội dung phim nằm trong
        video. Bên gọi tắt cờ này cho phim tên là cụm từ thông dụng (settings.strict_relevance_movie_slugs).

    Dùng khi Kira không đưa ra phán quyết (tắt, vượt hạn mức ngày, lỗi) hoặc chỉ trả
    "uncertain". Trước 2026-10-05 lúc đó chỉ còn kiểm tra chuỗi con theo từ khoá, nên với tên
    phim là cụm từ thường ngày ("Người Được Chọn") mọi bài hát, bài tâm linh, bài tóm tắt phim
    khác dùng cụm từ đó đều lọt lên dashboard. Thà ẩn nhầm một bài thật (vẫn lưu, gán nhãn lại
    được) còn hơn để rác lên dashboard."""
    text = normalize_title(content)
    if not text:
        return None
    movie = movie or {}
    tags = _hashtags(content)
    for key in ("director", "cast") if allow_names else ():
        for name in re.split(r"[,;/|]", str(movie.get(key) or "")):
            folded = normalize_title(name)
            # Tên một chữ quá dễ trùng ("Hiếu", "Linh") - chỉ tin tên từ hai chữ trở lên.
            if len(folded.split()) >= 2 and (_contains_phrase(text, folded) or _squash(folded) in tags):
                return key

    title = normalize_title(movie.get("title"))
    if not title:
        return None
    squashed_text = _squash(text)
    if not (_contains_phrase(text, title) or _squash(title) in squashed_text):
        return None
    if any(_squash(prefix + title) in squashed_text for prefix in ("phim ", "dien anh ", "movie ", "film ")):
        return "phim_title"
    tokens = text.split()
    if _strong_phrase_near_title(tokens, title.split()):
        return "strong_near_title"
    has_film_word = any(token.startswith("phim") for token in tokens) or _contains_phrase(text, "dien anh")
    has_music = any(_contains_phrase(text, phrase) for phrase in _MUSIC_PHRASES)
    if has_film_word and not has_music and _quoted_title(content, title):
        return "quoted_title"
    if allow_title_hashtag and _distinctive_title(title) and _squash(title) in tags:
        return "title_hashtag"
    return None


def has_film_context(content: str | None, movie: dict | None = None) -> bool:
    """Xem film_context_reason."""
    return film_context_reason(content, movie) is not None


def _quoted_title(content: str | None, title: str) -> bool:
    raw = unicodedata.normalize("NFC", content or "")
    for left, right in _QUOTE_PAIRS:
        pattern = re.escape(left) + r"([^" + re.escape(right) + r"\n]{2,80})" + re.escape(right)
        if any(normalize_title(quoted) == title for quoted in re.findall(pattern, raw)):
            return True
    return False


def _phrase_spans(tokens: list[str], phrase: list[str]) -> list[tuple[int, int]]:
    """(vị trí bắt đầu, vị trí kết thúc + 1) của mọi lần `phrase` xuất hiện trong `tokens`."""
    size = len(phrase)
    spans = [(i, i + size) for i in range(len(tokens) - size + 1) if tokens[i : i + size] == phrase]
    if size > 1:
        # Dạng hashtag dính liền (#nguoiduocchon) là một token.
        joined = "".join(phrase)
        spans += [(i, i + 1) for i, token in enumerate(tokens) if joined in token]
    return spans


def _strong_phrase_near_title(tokens: list[str], title: list[str]) -> bool:
    title_spans = _phrase_spans(tokens, title)
    for phrase in _STRONG_FILM_PHRASES:
        for start, end in _phrase_spans(tokens, phrase.split()):
            for t_start, t_end in title_spans:
                gap = start - t_end if start >= t_end else t_start - end
                if gap <= _STRONG_PHRASE_WINDOW:
                    return True
    return False


def mentions_keyword_or_title(content: str | None, keywords: Iterable[str | None], title: str | None) -> bool:
    """Bài có chứa ĐẦY ĐỦ một trong các từ khoá của phim, hoặc đầy đủ tên phim không - so sau
    khi bỏ dấu/hoa thường/khoảng trắng/dấu câu, nên "#PhimNguoiDuocChon", "phim Người Được
    Chọn" và "PHIM NGƯỜI ĐƯỢC CHỌN" đều khớp từ khoá "#PhimNguoiDuocChon". Từ khoá nối bằng "+"
    cần đủ mọi phần (giống contains_keyword). `keywords` nên gồm mọi từ khoá đang bật của phim
    (mọi nền tảng): tên dài hay được viết theo từ khoá ngắn ("Thám Tử Kiên 2" cho "Thám Tử Kiên:
    Lời Nguyền Hoàng Kim").

    Cổng đầu tiên của ingest, trước Kira: ngày 2026-10-05 tìm kiếm Facebook trả về bài bàn về
    một phim truyền hình khác (không có chữ nào của "Người Được Chọn") và Kira vẫn gán
    "related" 0,65-0,72. Không chứa từ khoá lẫn tên phim thì loại luôn, không tốn lời gọi Kira."""
    text = _squash(normalize_title(content))
    if not text:
        return False
    for keyword in keywords:
        parts = [_squash(normalize_title(part)) for part in (keyword or "").split("+")]
        parts = [part for part in parts if part]
        if parts and all(part in text for part in parts):
            return True
    folded_title = _squash(normalize_title(title))
    return bool(folded_title) and folded_title in text


def resolve_relevance(
    label: str | None, content: str | None, movie: dict, *, has_keyword: bool, strict: bool
) -> tuple[bool | None, str | None, str | None]:
    """Nhãn Kira (related/uncertain hoặc None khi Kira không kết luận - "not_related" do bên gọi xử lý trước)
    -> (ai_relevant, relevance_label, context). Dùng chung cho ingest consumer và
    scripts/relabel_post_relevance.py.

    "uncertain" (caption chỉ có hashtag, nội dung phim nằm trong video) hoặc không kết luận: tín hiệu phim
    trong bài quyết định thay. Có tín hiệu -> bài lên dashboard ("uncertain" được nâng thành "related", lý do
    trả ra ở context); không có -> vẫn lưu nhưng ẩn (ai_relevant False, nhãn "uncertain"). Phim "chặt" (strict:
    tên trùng cụm từ thông dụng) chỉ nhận tín hiệu mạnh, và kể cả bài Kira gán "related" cũng phải có tín hiệu
    phim. ai_relevant None = chỉ kiểm tra từ khoá quyết định."""
    if label == "related":
        if strict and not has_film_context(content, movie):
            return False, "uncertain", None
        return True, label, None
    context = film_context_reason(content, movie, allow_title_hashtag=not strict, allow_names=not strict)
    if context:
        return True, "related" if label == "uncertain" else label, context
    if has_keyword or label == "uncertain":
        return False, label or "uncertain", None
    return None, label, None
