"""Luật độ liên quan tất định, áp dụng lúc ingest trước luật từ khoá và trước AI.

Luật bắt các lỗi rẻ mà model hay bỏ sót:

- `foreign_language_reason()`: phần chữ của bài (bỏ hashtag, mention, URL, emoji) rõ ràng không phải tiếng Việt. Thận
  trọng: chữ ngắn / mơ hồ thì không kết luận.
- `mentions_other_film()`: bài không nhắc phim đích nhưng nêu tên một phim khác đang theo dõi.
- Các luật quanh ngày công chiếu, tên rút gọn, gắn hashtag ké (xem từng hàm).

Bài bị loại vẫn được lưu qua topic Kafka `ingest_decisions` (lake), nên loại nhầm vẫn phát lại được.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from datetime import date, timedelta

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
_STRONG_FILM_PHRASES: tuple[str, ...] = (
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
    # 19/11/2026: sự kiện/báo chí quanh phim. Bài "Casting Call Cộng Trừ Nhân Chia", "Showcase Mẹ Mìn" bị ẩn
    # vì phim có tên mơ hồ (ambiguous/strict) không có tín hiệu nào qua được; các từ này chỉ tính khi sát tên phim (<= 4 từ). Không thêm "cinetour": #cinetour là hashtag chung, đứng cạnh hashtag tên phim sẽ làm yếu luật phim chặt.
    "casting call",
    "casting",
    "showcase",
    "hop bao",
    "ra mat phim",
    "buoi ra mat",
    "tham do",
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


# Số phần phim một chữ số ("Út Lan 2", #TeeYod4) - không tính năm (#TraiBuonNguoi2026).
_SEQUEL_DIGIT_RE = re.compile(r"(?<!\d)\d(?!\d)")


def _film_specific_keyword_hashtag(tags: set[str], movie: dict) -> bool:
    """Bài mang một hashtag trùng từ khoá đang bật của phim (movie["keywords"]) mà từ khoá đó chỉ trỏ tới ĐÚNG
    phim này. Đo trên bài đang bị ẩn #TraiBuonNguoiMovie, #holinhtrangsibianmovuadinh, #CCEE3,
    #TeeYod4, #ThamTuKien2 là tín hiệu gần như chắc chắn mà Jev vẫn trả "uncertain" (caption chỉ có hashtag).
    Bỏ qua từ khoá mơ hồ - chính chúng kéo về phần lớn bài ẩn đúng:
      - phim có số phần mà từ khoá không có số (#PhimUtLan, #teeyod, #ThamTuKien -> bài của phần trước);
      - từ khoá chỉ là tên loạt phim, phần trước dấu ":" của tên (#ShaunTheSheep);
      - từ khoá mở đầu bằng tên viết tắt mà không có số (#hhcccasting - viết tắt dễ trùng);
      - từ khoá ngắn chung chung (dưới 10 ký tự viết liền, không có số, không phải dạng phim…/…movie: #memin)."""
    raw_title = str(movie.get("title") or "")
    aliases = {_squash(normalize_title(str(k).lstrip("#"))) for k in movie.get("keywords") or ()} - {""}
    if not aliases & tags:
        return False
    sequel = bool(_SEQUEL_DIGIT_RE.search(normalize_title(raw_title))) or any(
        _SEQUEL_DIGIT_RE.search(a) for a in aliases
    )
    franchise = _squash(normalize_title(raw_title.split(":", 1)[0])) if ":" in raw_title else None
    acronym = title_acronym(raw_title)
    for alias in aliases & tags:
        numbered = bool(_SEQUEL_DIGIT_RE.search(alias))
        if sequel and not numbered:
            continue
        if franchise and alias in (franchise, "phim" + franchise, franchise + "movie"):
            continue
        if acronym and alias.startswith(acronym) and not numbered:
            continue
        if len(alias) >= 10 or numbered or alias.startswith("phim") or alias.endswith("movie"):
            return True
    return False


# 19/11/2026 (đo trên 628 bài rà soát tay, scripts/tools/eval_relevance_rules.py): hai tín hiệu đặc trưng mà luật cũ bỏ sót.
# - Tên viết tắt của phim (#amxhh, #NHSNH, CCEE3): chỉ tin khi từ viết tắt đủ dài (ACRONYM_MIN_LEN) - viết tắt ngắn dễ trùng.
#   Số phần phim là một "từ" của tên nên "Chị Chị Em Em 3" -> "ccee3" (khác "#CCEE" của hoạt hình Đám Cưới Mộng Mơ).
# - Hashtag chiến dịch của agency "<tên phim>by<agency>" (#congtrunhanchiabymoli, #ngayconconmebymoli): chỉ khi tên phim không có
#   phần phụ sau ":" (nếu có, phần đứng trước là tên loạt phim, dùng chung với phần cũ: #thamtukienbysunsee).
ACRONYM_MIN_LEN = 5
AGENCY_MIN_NAME_LEN = 8


def _agency_hashtag(tags: set[str], title: str | None) -> bool:
    raw = str(title or "")
    if re.search(r"[:(（]", raw):
        return False
    name = _squash(normalize_title(raw))
    return len(name) >= AGENCY_MIN_NAME_LEN and any(
        tag.startswith(name + "by") and len(tag) > len(name) + 3 for tag in tags
    )


def film_context_reason(
    content: str | None, movie: dict | None = None, *, allow_title_hashtag: bool = False, allow_names: bool = True
) -> str | None:
    """Lý do bài được coi là nói về ĐÚNG phim đích (movie["title"]) như một phim chiếu rạp, hoặc
    None. Theo thứ tự:
      - "cast"/"director": tên đạo diễn/diễn viên (từ hai chữ trở lên), kể cả dạng hashtag
        (#StevenNguyen) - bỏ qua khi `allow_names=False` (tin đời tư của diễn viên cũng nhắc tên);
      - "keyword_hashtag": chỉ khi `allow_title_hashtag` - hashtag trùng một từ khoá đang bật chỉ trỏ tới đúng
        phim này (#TraiBuonNguoiMovie, #CCEE3), xem _film_specific_keyword_hashtag;
      - "phim_title": "phim <tên>" / "điện ảnh <tên>", kể cả dạng hashtag (#PhimNguoiDuocChon);
      - "strong_near_title": một từ điện ảnh mạnh (khởi chiếu, ra rạp, trailer, phòng vé...) cách
        tên phim không quá _STRONG_PHRASE_WINDOW từ;
      - "quoted_title": tên phim trong ngoặc kép VÀ bài có chữ "phim"/"điện ảnh" VÀ không có từ
        vựng âm nhạc - ngoặc kép đánh dấu tên tác phẩm, nhưng tác phẩm đó có thể là bài hát;
      - "title_hashtag": chỉ khi `allow_title_hashtag` - hashtag đúng bằng tên phim (#traibuonnguoi)
        và tên đủ đặc trưng. Caption TikTok/Threads thường chỉ có vậy, còn nội dung phim nằm trong
        video. Bên gọi tắt cờ này cho phim tên là cụm từ thông dụng (settings.strict_relevance_movie_slugs).

    Dùng khi AI không đưa ra phán quyết (tắt, vượt hạn mức ngày, lỗi) hoặc chỉ trả
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
        # Tách cả " và " / " and " / "&": đạo diễn "Khương Ngọc và Tấn Hoàng Thông" trước 17/10/2026 thành một tên dài
        # không bao giờ khớp. Cast lưu dạng JSON ('["Minh Hằng", ...]') - dấu ngoặc/nháy bị normalize_title bỏ đi.
        for name in re.split(r"[,;/|&\[\]\n]|\s+(?:và|and)\s+", str(movie.get(key) or "")):
            folded = normalize_title(name)
            # Tên một chữ quá dễ trùng ("Hiếu", "Linh") - chỉ tin tên từ hai chữ trở lên.
            if len(folded.split()) >= 2 and (_contains_phrase(text, folded) or _squash(folded) in tags):
                return key

    # Xét sau mọi tín hiệu theo tên phim để lý do cũ giữ nguyên; dùng cả khi bài không chứa nguyên văn tên phim.
    keyword_hashtag = "keyword_hashtag" if allow_title_hashtag and _film_specific_keyword_hashtag(tags, movie) else None
    if allow_title_hashtag:
        acronym = title_acronym(movie.get("title"))
        if acronym and len(acronym) >= ACRONYM_MIN_LEN and (acronym in tags or _contains_phrase(text, acronym)):
            return "acronym"
        if _agency_hashtag(tags, movie.get("title")):
            return "agency_hashtag"

    title = normalize_title(movie.get("title"))
    if not title:
        return keyword_hashtag
    squashed_text = _squash(text)
    if not (_contains_phrase(text, title) or _squash(title) in squashed_text):
        return keyword_hashtag
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
    return keyword_hashtag


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


FULL_TITLE_MIN_WORDS = 3


def full_title_variants(title: str | None) -> list[str]:
    """Các dạng tên phim đủ đặc trưng để quy tắc full_title chấp nhận (đã bỏ dấu, viết liền):
    - cả tên, khi từ FULL_TITLE_MIN_WORDS chữ trở lên;
    - phần trước dấu ":" / "(" khi phần đó có số phần phim - bài thường chỉ ghi "Quỷ Ăn Tạng 4" cho "QUỶ ĂN TẠNG 4: HỔ
      TINH (Tee Yod)" (hàng trăm bài như "Xem QUỶ ĂN TẠNG 4 chỉ từ 40.000Đ/vé" bị ẩn). Phần trước dấu ":"
      KHÔNG có số ("Thám Tử Kiên", "Shaun the Sheep") là tên loạt phim - bài phần cũ - nên không tính."""
    raw = str(title or "")
    variants: list[str] = []
    words = normalize_title(raw).split()
    if len(words) >= FULL_TITLE_MIN_WORDS:
        variants.append("".join(words))
    head = normalize_title(re.split(r"[:(（]", raw, maxsplit=1)[0]).split()
    if len(head) >= 2 and _SEQUEL_DIGIT_RE.search(" ".join(head)):
        squashed = "".join(head)
        if squashed not in variants:
            variants.append(squashed)
    return variants


def mentions_full_title(content: str | None, title: str | None) -> bool:
    """Bài có đủ tên phim (bỏ dấu/hoa thường/khoảng trắng/dấu câu) - xem full_title_variants. Tên ngắn ("Mẹ Mìn", "Huyết
    Thống", "Loạn Thế") trùng từ ngữ thường ngày nên không tính. Tên kết thúc bằng số không khớp khi ngay sau là chữ số
    khác ("quyantang4" không khớp "quyantang40")."""
    spaced = normalize_title(content)
    words = normalize_title(title).split()
    # So theo từng từ trước: khi dính liền chữ, số phần phim dính vào số đứng sau ("chị chị em em 3 - 6/10" -> "...em36").
    if len(words) >= FULL_TITLE_MIN_WORDS and _contains_phrase(spaced, " ".join(words)):
        return True
    text = _squash(spaced)
    return any(re.search(re.escape(_squash(variant)) + r"(?!\d)", text) for variant in full_title_variants(title))


def mentions_keyword_or_title(content: str | None, keywords: Iterable[str | None], title: str | None) -> bool:
    """Bài có chứa ĐẦY ĐỦ một trong các từ khoá của phim, hoặc đầy đủ tên phim không - so sau
    khi bỏ dấu/hoa thường/khoảng trắng/dấu câu, nên "#PhimNguoiDuocChon", "phim Người Được
    Chọn" và "PHIM NGƯỜI ĐƯỢC CHỌN" đều khớp từ khoá "#PhimNguoiDuocChon". Từ khoá nối bằng "+"
    cần đủ mọi phần (giống contains_keyword). `keywords` nên gồm mọi từ khoá đang bật của phim
    (mọi nền tảng): tên dài hay được viết theo từ khoá ngắn ("Thám Tử Kiên 2" cho "Thám Tử Kiên:
    Lời Nguyền Hoàng Kim").

    Cổng đầu tiên của ingest, trước AI: ngày 2026-10-05 tìm kiếm Facebook trả về bài bàn về
    một phim truyền hình khác (không có chữ nào của "Người Được Chọn") và AI vẫn gán
    "related" 0,65-0,72. Không chứa từ khoá lẫn tên phim thì loại luôn, không tốn lời gọi AI."""
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


# 19/11/2026: phim tên mơ hồ (settings.ambiguous_title_movie_slugs) bị tắt luật "đủ tên phim" nên bài thật như "review Bò Sữa Bay",
# "pass 3 vé bò sữa bay", "CINETOUR BÒ SỮA BAY" bị ẩn. Mở lại CÓ ĐIỀU KIỆN: đủ tên phim (từ FULL_TITLE_MIN_WORDS chữ) VÀ có từ chỉ
# việc xem/đánh giá phim ở rạp. Phim "chặt" (strict) không dùng luật này.
_CINEMA_INTENT_PHRASES = (
    "review",
    "spoil",
    "seeding",
    "cinetour",
    "teaser",
    "trailer",
    "suat chieu",
    "khoi chieu",
    "cong chieu",
    "ra rap",
    "di xem",
    "di coi",
    "pass ve",
    "mua ve",
    "dat ve",
    "bom tan",
    "phim dien anh",
)


def mentions_full_title_with_intent(content: str | None, title: str | None) -> bool:
    if not mentions_full_title(content, title):
        return False
    text = normalize_title(content)
    return any(_contains_phrase(text, phrase) for phrase in _CINEMA_INTENT_PHRASES)


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


def mentions_short_title(content: str | None, movie: dict | None, posted_at: object) -> bool:
    """Bài nhắc tên rút gọn của phim (chữ hoặc hashtag) và đăng trong [công chiếu - SHORT_TITLE_BEFORE_DAYS, công chiếu
    + SHORT_TITLE_AFTER_DAYS]. Không có ngày công chiếu / ngày đăng thì không nhận."""
    movie = movie or {}
    released, posted = _to_date(movie.get("released_at")), _to_date(posted_at)
    if not released or not posted:
        return False
    if (
        not released - timedelta(days=SHORT_TITLE_BEFORE_DAYS)
        <= posted
        <= released + timedelta(days=SHORT_TITLE_AFTER_DAYS)
    ):
        return False
    text = normalize_title(content)
    if not text:
        return False
    tags = _hashtags(content)
    return any(_contains_phrase(text, name) or _squash(name) in tags for name in short_title_names(movie.get("title")))


def resolve_relevance(
    label: str | None,
    content: str | None,
    movie: dict,
    *,
    has_keyword: bool,
    strict: bool,
    confidence: float | None = None,
    full_title_rule: bool = True,
    posted_at: object = None,
) -> tuple[bool | None, str | None, str | None]:
    """Nhãn AI (related/uncertain hoặc None khi AI không kết luận - "not_related" do bên gọi xử lý trước)
    -> (ai_relevant, relevance_label, context). Dùng chung cho ingest consumer và
    scripts/jobs/relabel_post_relevance.py.

    "uncertain" (caption chỉ có hashtag, nội dung phim nằm trong video) hoặc không kết luận: tín hiệu phim
    trong bài quyết định thay. Có tín hiệu -> bài lên dashboard ("uncertain" được nâng thành "related", lý do
    trả ra ở context); không có -> vẫn lưu nhưng ẩn (ai_relevant False, nhãn "uncertain"). Phim "chặt" (strict:
    tên trùng cụm từ thông dụng) chỉ nhận tín hiệu mạnh, và kể cả bài AI gán "related" cũng phải có tín hiệu
    phim. ai_relevant None = chỉ kiểm tra từ khoá quyết định.

    """
    if label == "related":
        if strict and not has_film_context(content, movie):
            return False, "uncertain", None
        return True, label, None
    context = film_context_reason(content, movie, allow_title_hashtag=not strict, allow_names=not strict)
    if context:
        return True, "related" if label == "uncertain" else label, context
    if not strict and full_title_rule and mentions_full_title(content, movie.get("title")):
        # Đủ tên phim dài (từ 3 chữ) là tín hiệu đủ mạnh (đo ~85% bài "uncertain" có đủ tên 3+ chữ thật sự
        # là về phim - "t vừa coi suất chiều Bóng Ma Nhà Hát", "spotlight của Út Lan 2..."); phim tên là cụm từ thông
        # dụng thì bên gọi truyền full_title_rule=False (settings.ambiguous_title_movie_slugs).
        return True, "related", "full_title"
    if not strict and full_title_rule and in_release_window(content, movie, posted_at):
        return True, "related", "release_window"
    if not strict and full_title_rule and mentions_short_title(content, movie, posted_at):
        return True, "related", "short_title"
    if not strict and not full_title_rule and mentions_full_title_with_intent(content, movie.get("title")):
        return True, "related", "full_title_intent"
    if has_keyword or label == "uncertain":
        return False, label or "uncertain", None
    return None, label, None


# --- Chế độ lọc thủ công (settings.relevance_rules_only) ---
# Bỏ hẳn AI ở bước lọc bài để ingest nhanh: bài liên quan = chứa tên phim (mọi dạng dấu/hoa thường/viết liền như
# #HoLinhTrangSi), tên viết tắt (HLTS) hoặc từ khoá của phim; phần chữ (đã bỏ hashtag/mention/link) dài mà không có
# dấu tiếng Việt nào thì coi là tiếng nước ngoài và loại thẳng. Hashtag-only ("#HoLinhTrangSi #fyp") không bị luật dấu
# loại - phần chữ của nó rỗng.


def _clearly_about(content: str | None, movie: dict, *, strict: bool, ambiguous: bool) -> bool:
    """Tín hiệu rõ ràng bài nói về ĐÚNG phim này: ngữ cảnh điện ảnh quanh tên phim / hashtag chỉ trỏ tới phim này
    (film_context_reason, không tính tên diễn viên), hoặc đủ tên phim dài (mentions_full_title) khi tên không phải cụm từ
    thông dụng. Phim "chặt" không nhận hashtag tên phim."""
    if film_context_reason(content, movie, allow_title_hashtag=not strict, allow_names=False):
        return True
    return not strict and not ambiguous and mentions_full_title(content, movie.get("title"))


def find_film(
    content: str | None,
    movies: Iterable[dict],
    posted_at: object = None,
    *,
    exclude_movie_id: str | None = None,
    strict: bool = False,
    strict_slugs: Iterable[str] = (),
    ambiguous_slugs: Iterable[str] = (),
) -> dict | None:
    """Phim đang theo dõi mà bài nói tới, bỏ qua `exclude_movie_id`. Nhiều phim khớp thì lấy phim tên dài nhất (cụ thể
    nhất). `movies`: [{"id", "title", "slug", "keywords": [...], ...}] (ingest _tracked_movies).

    strict=False (bài trang nguồn chung - trang rạp, trang review): tên phim hoặc một từ khoá của phim
    (mentions_keyword_or_title), không có thì tên rút gọn quanh ngày công chiếu khi đúng một phim khớp.
    strict=True (chuyển bài tìm bằng từ khoá phim khác sang phim này): chỉ khi có tín hiệu rõ (_clearly_about) - từ khoá
    của phim có cả hashtag chung chung, và tên ngắn ("Mẹ Mìn", "Người Được Chọn") trùng cụm từ thường ngày, nên khớp lỏng
    gán truyện đời thường hay bài phim khác vào phim (chạy thử 13/11/2026 trên bản sao lưu đợt xoá)."""
    if not content:
        return None
    candidates = [m for m in movies if m.get("keywords") and m.get("id") != exclude_movie_id]
    if strict:
        strict_set, ambiguous_set = set(strict_slugs), set(ambiguous_slugs)
        matches = [
            m
            for m in candidates
            if _clearly_about(content, m, strict=m.get("slug") in strict_set, ambiguous=m.get("slug") in ambiguous_set)
        ]
    else:
        matches = [m for m in candidates if mentions_keyword_or_title(content, m["keywords"], m.get("title"))]
        if not matches:
            short = [m for m in candidates if mentions_short_title(content, m, posted_at)]
            matches = short if len(short) == 1 else []
    return max(matches, key=lambda m: len(str(m.get("title") or ""))) if matches else None


def title_acronym(title: str | None) -> str | None:
    """Chữ đầu các từ của tên phim, khi tên có từ ba chữ trở lên ("Hộ Linh Tráng Sĩ" -> "hlts"). Tên ngắn hơn thì
    viết tắt quá dễ trùng nên không dùng."""
    words = normalize_title(title).split()
    return "".join(word[0] for word in words) if len(words) >= 3 else None


def mentions_film_manual(content: str | None, keywords: Iterable[str | None], title: str | None) -> bool:
    """mentions_keyword_or_title, cộng thêm tên viết tắt đứng thành một từ riêng hoặc hashtag ("HLTS", "#HLTS")."""
    if mentions_keyword_or_title(content, keywords, title):
        return True
    acronym = title_acronym(title)
    return bool(acronym) and _contains_phrase(normalize_title(content), acronym)


def no_diacritics_reason(text: str | None) -> str | None:
    """ "no_diacritics" khi phần chữ của bài đủ dài (_MIN_LETTERS) mà không có một chữ có dấu tiếng Việt nào."""
    body = _body(text).lower()
    if any(ch in _VI_CHARS for ch in body):
        return None
    letters = sum(1 for ch in body if ch.isalpha())
    return "no_diacritics" if letters >= _MIN_LETTERS else None
