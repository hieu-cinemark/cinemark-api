"""Deterministic relevance rules applied at ingest, before (and regardless
of) the keyword substring shortcut and the Kira classifier.

Why rules, not just the model: measured 2026-09-28, the ingest pipeline
admitted any post whose text contains the keyword without any model
looking at it - so foreign videos sharing an unaccented hashtag (#memin:
17% of its posts were not Vietnamese, mostly Spanish posts about the
Mexican comic Memín) went straight to the dashboard. And PhoBERT (trained
on keyword_match labels) confidently marked posts about a *different*
tracked film as related (e.g. "Lan Trinh trong Án Mạng Xém Hoàn Hảo"
attributed to Án Mạng Karaoke). Both have cheap, checkable signals:

  foreign_language_reason() - the post's own words (hashtags, mentions,
      URLs and emoji removed) are clearly not Vietnamese, or TikTok's own
      textLanguage says so. Deliberately conservative: short/ambiguous text
      returns None (no verdict) rather than guessing.
  mentions_other_film() - the post doesn't reference the target film at
      all but names another tracked film.

Every tracked film is Vietnamese; a relevant post about one is in
Vietnamese (with or without diacritics) in practice. A dropped post is
archived via the ingest_decisions Kafka topic (-> lake writer, under
bronze/entity=decisions/), so a false positive here is replayable from
the lake's NDJSON files rather than a now-deleted D1 table.
"""

from __future__ import annotations

import re
import unicodedata

_STRIP_RE = re.compile(r"(?:#|@)\S+|https?://\S+|www\.\S+")
_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)

# Letters that only occur in Vietnamese among Latin-script languages this
# pipeline sees (after NFC). Any one of them settles the question.
_VI_CHARS = frozenset("ăâđêôơưàáảãạằắẳẵặầấẩẫậèéẻẽẹềếểễệìíỉĩịòóỏõọồốổỗộờớởỡợùúủũụừứửữựỳýỷỹỵ")
# Unaccented Vietnamese words that are not also common words in English,
# Spanish, Indonesian or Tagalog - evidence for diacritic-less Vietnamese.
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
# Frequent function words of the foreign languages that actually show up
# under colliding hashtags (English, Spanish/Portuguese, Indonesian/Malay,
# Tagalog). Two or more, with no Vietnamese signal, means foreign text.
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
# Film/cinema vocabulary: a foreign-language caption that uses it is often a
# Vietnamese fan or distributor writing in English about the film (seen live:
# "One more time for cinetourrr ... #nghihesonghihuu"), so the function-word
# rule stands down. Script and platform-language verdicts still apply.
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
    # NFKC folds decorative Unicode ("𝐞𝐛𝐞́", fullwidth letters) back to
    # plain letters + combining marks, then NFC recomposes the Vietnamese
    # diacritics so _VI_CHARS can match them.
    folded = unicodedata.normalize("NFC", unicodedata.normalize("NFKC", text or ""))
    return _STRIP_RE.sub(" ", folded)


def _is_latin(ch: str) -> bool:
    return "LATIN" in unicodedata.name(ch, "")


def foreign_language_reason(text: str | None, text_language: str | None = None) -> str | None:
    """A short reason string when the post is clearly not Vietnamese, else
    None (Vietnamese, or not enough text to tell)."""
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
    # "cinetourrr" -> "cinetour": collapse stretched letters before the lookup.
    film_context = any(re.sub(r"(.)\1{2,}", r"\1", w) in _FILM_CONTEXT_WORDS for w in words)
    if len(words) >= 3 and foreign_hits >= 2 and not film_context:
        return "foreign_function_words"
    return None


def normalize_title(value: str | None) -> str:
    """Accent/case/punctuation-insensitive form ("Án Mạng Karaoke" ->
    "an mang karaoke"), with đ folded to d."""
    stripped = "".join(
        ch for ch in unicodedata.normalize("NFD", (value or "").lower()) if unicodedata.category(ch) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", " ", stripped.replace("đ", "d")).strip()


def _contains_phrase(haystack: str, phrase: str) -> bool:
    return bool(phrase) and f" {phrase} " in f" {haystack} "


def mentions_other_film(
    content: str | None, movie_title: str | None, keyword: str | None, other_titles: list[str]
) -> str | None:
    """The other tracked film this post names, when it doesn't reference
    the target film (title or keyword, accent-insensitive, hashtag form
    included) at all. None otherwise. Short or nested titles are skipped:
    one-or-two-word titles like "Anh Hùng"/"Loạn Thế" are everyday phrases,
    and "Út Lan" vs "Út Lan 2" would match each other."""
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
