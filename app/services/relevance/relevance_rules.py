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
from datetime import timedelta

from app.services.relevance.film_phrases import STRONG_FILM_PHRASES
from app.services.relevance.release_window import (
    SHORT_TITLE_AFTER_DAYS,
    SHORT_TITLE_BEFORE_DAYS,
    in_release_window,
    short_title_names,
    to_date,
)
from app.services.relevance.text_rules import contains_phrase, extract_hashtags, normalize_title, squash


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
    if contains_phrase(text, own) or (own and own.replace(" ", "") in squashed):
        return None
    if kw and (contains_phrase(text, kw) or kw.replace(" ", "") in squashed):
        return None
    for title in other_titles:
        other = normalize_title(title)
        if len(other.split()) < 3 or not own or other in own or own in other:
            continue
        if contains_phrase(text, other) or other.replace(" ", "") in squashed:
            return title
    return None


# Từ điện ảnh mạnh phải cách tên phim đích không quá chừng này từ: "Người Được Chọn dự kiến
# khởi chiếu" là về phim này, còn "...chưa phải người được chọn. Phim Madam T | Khởi chiếu
# 25.09" là bài của phim khác chỉ dùng cụm từ đó (gặp thật 2026-10-05).
_STRONG_PHRASE_WINDOW = 4
_QUOTE_PAIRS = (('"', '"'), ("“", "”"), ("'", "'"), ("‘", "’"), ("«", "»"))


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


def _distinctive_title(title: str) -> bool:
    """Tên đủ đặc trưng để hashtag của nó là tín hiệu phim: từ ba chữ, hoặc viết liền dài từ 10 ký
    tự. Tên ngắn như "Anh Hùng"/"Loạn Thế" là cụm từ thường ngày, hashtag của chúng không nói lên gì."""
    return len(title.split()) >= 3 or len(squash(title)) >= 10


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
    aliases = {squash(normalize_title(str(k).lstrip("#"))) for k in movie.get("keywords") or ()} - {""}
    if not aliases & tags:
        return False
    sequel = bool(_SEQUEL_DIGIT_RE.search(normalize_title(raw_title))) or any(
        _SEQUEL_DIGIT_RE.search(a) for a in aliases
    )
    franchise = squash(normalize_title(raw_title.split(":", 1)[0])) if ":" in raw_title else None
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
    name = squash(normalize_title(raw))
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
    tags = extract_hashtags(content)
    for key in ("director", "cast") if allow_names else ():
        # Tách cả " và " / " and " / "&": đạo diễn "Khương Ngọc và Tấn Hoàng Thông" trước 17/10/2026 thành một tên dài
        # không bao giờ khớp. Cast lưu dạng JSON ('["Minh Hằng", ...]') - dấu ngoặc/nháy bị normalize_title bỏ đi.
        for name in re.split(r"[,;/|&\[\]\n]|\s+(?:và|and)\s+", str(movie.get(key) or "")):
            folded = normalize_title(name)
            # Tên một chữ quá dễ trùng ("Hiếu", "Linh") - chỉ tin tên từ hai chữ trở lên.
            if len(folded.split()) >= 2 and (contains_phrase(text, folded) or squash(folded) in tags):
                return key

    # Xét sau mọi tín hiệu theo tên phim để lý do cũ giữ nguyên; dùng cả khi bài không chứa nguyên văn tên phim.
    keyword_hashtag = "keyword_hashtag" if allow_title_hashtag and _film_specific_keyword_hashtag(tags, movie) else None
    if allow_title_hashtag:
        acronym = title_acronym(movie.get("title"))
        if acronym and len(acronym) >= ACRONYM_MIN_LEN and (acronym in tags or contains_phrase(text, acronym)):
            return "acronym"
        if _agency_hashtag(tags, movie.get("title")):
            return "agency_hashtag"

    title = normalize_title(movie.get("title"))
    if not title:
        return keyword_hashtag
    squashed_text = squash(text)
    if not (contains_phrase(text, title) or squash(title) in squashed_text):
        return keyword_hashtag
    if any(squash(prefix + title) in squashed_text for prefix in ("phim ", "dien anh ", "movie ", "film ")):
        return "phim_title"
    tokens = text.split()
    if _strong_phrase_near_title(tokens, title.split()):
        return "strong_near_title"
    has_film_word = any(token.startswith("phim") for token in tokens) or contains_phrase(text, "dien anh")
    has_music = any(contains_phrase(text, phrase) for phrase in _MUSIC_PHRASES)
    if has_film_word and not has_music and _quoted_title(content, title):
        return "quoted_title"
    if allow_title_hashtag and _distinctive_title(title) and squash(title) in tags:
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
    for phrase in STRONG_FILM_PHRASES:
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
    if len(words) >= FULL_TITLE_MIN_WORDS and contains_phrase(spaced, " ".join(words)):
        return True
    text = squash(spaced)
    return any(re.search(re.escape(squash(variant)) + r"(?!\d)", text) for variant in full_title_variants(title))


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
    text = squash(normalize_title(content))
    if not text:
        return False
    for keyword in keywords:
        parts = [squash(normalize_title(part)) for part in (keyword or "").split("+")]
        parts = [part for part in parts if part]
        if parts and all(part in text for part in parts):
            return True
    folded_title = squash(normalize_title(title))
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
    return any(contains_phrase(text, phrase) for phrase in _CINEMA_INTENT_PHRASES)


def mentions_short_title(content: str | None, movie: dict | None, posted_at: object) -> bool:
    """Bài nhắc tên rút gọn của phim (chữ hoặc hashtag) và đăng trong [công chiếu - SHORT_TITLE_BEFORE_DAYS, công chiếu
    + SHORT_TITLE_AFTER_DAYS]. Không có ngày công chiếu / ngày đăng thì không nhận."""
    movie = movie or {}
    released, posted = to_date(movie.get("released_at")), to_date(posted_at)
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
    tags = extract_hashtags(content)
    return any(contains_phrase(text, name) or squash(name) in tags for name in short_title_names(movie.get("title")))


# 26/11/2026 - video YouTube chỉ trùng tên phim: phim bộ chia tập ("Nữ Binh Phong Hỏa - Tập 12", anime "[Việt sub]"), truyện
# audio, nhạc chế/remix/live stage, tiết mục văn nghệ trường. Không chặn "Provided to YouTube"/"Official MV": bài hát mang tên phim
# thường là nhạc phim. Đo trên 3.000 bài luật đã nâng thành
# "related": đây là phần lớn rác của các phim tên thông dụng (Nhất Quỷ Nhì Ma, Viên Đạn Cuối Cùng, Huyết Thống, EM...).
# Nhạc phim (OST) VẪN tính là bài phim (người vận hành, 26/11) - bài nhắc OST / nhạc phim / ra rạp thì không bị chặn.
_YOUTUBE_NOISE_RE = re.compile(
    r"\btap \d{1,3}\b|\bviet ?sub\b|\blive ?stage\b|\bremix\b|"
    r"\baudio\b|\bdoc truyen\b|\breview truyen\b|\bnghe truyen\b|\bchuong \d+|\btiet muc\b|\bvan nghe\b|"
    r"\bhoi thi\b|\bflashmob\b|\bdan vu\b|\bnhac che\b|\bthe masked singer\b|\bca si mat na\b|\btron bo\b"
)
_YOUTUBE_FILM_HINT_RE = re.compile(
    r"\bost\b|\bnhac phim\b|\bsoundtrack\b|\btheme song\b|\bmotion picture\b|\bhau truong\b|\bbts\b|\bbehind the scenes?\b|\bra mat phim\b|\btham do\b|\bpremiere\b|\bshowcase\b|\bcinetour\b|\bphim dien anh\b|\bchieu rap\b|\bra rap\b|\bkhoi chieu\b|\bcong chieu\b|\btrailer\b|\bteaser\b"
)


def youtube_noise_reason(content: str | None, platform: str | None) -> str | None:
    """Video YouTube có dấu hiệu phim bộ/nhạc/truyện/văn nghệ và không nhắc gì tới phim chiếu rạp hay OST: cụm khớp, hoặc
    None. Chỉ YouTube - ở TikTok/Threads các từ này (remix, audio...) hay đi cùng bài thật về phim."""
    if platform != "youtube":
        return None
    # Chỉ xét phần chữ: video thật về phim hay gắn hashtag rác (#remix #music #audio).
    text = normalize_title(re.sub(r"#\S+", " ", content or ""))
    if not text or _YOUTUBE_FILM_HINT_RE.search(text):
        return None
    match = _YOUTUBE_NOISE_RE.search(text)
    return match.group(0) if match else None


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
    platform: str | None = None,
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
    if youtube_noise_reason(content, platform):
        return False, "uncertain", None
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
    nhất). `movies`: [{"id", "title", "slug", "keywords": [...], ...}] (ingest tracked_movies).

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
    return bool(acronym) and contains_phrase(normalize_title(content), acronym)
