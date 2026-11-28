"""Lọc bài báo / kết quả web theo ĐÚNG phim (28/11/2026). Trước đó lấy nguyên 10 kết quả của "phim <tên> review" nên lẫn phim khác
(Mẹ Mìn 8/10 bài về phim khác). Bài được giữ khi tiêu đề hoặc trích đoạn nêu tên phim (hoặc tên gốc trong ngoặc, phần chính trước
dấu ":"); không nêu thì đọc nội dung và chỉ giữ khi nội dung nhắc tên từ MIN_CONTENT_MENTIONS lần (nhắc thoáng một lần thường là bài
về phim khác điểm danh phim này). So khớp giữ dấu tiếng Việt, theo ranh giới từ."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Awaitable, Callable
from datetime import date, timedelta

from app.services import news_articles

MIN_CONTENT_MENTIONS = 2
MAX_DAYS_BEFORE_RELEASE = 365
_PARENS = re.compile(r"[(\[]([^)\]]*)[)\]]")
_QUOTED = re.compile("[\"“‘']([^\"“”‘’']{2,80})[\"”’']")
SHORT_TITLE_WORDS = 2


def normalize(text: str | None) -> str:
    words = re.findall(r"\w+", unicodedata.normalize("NFC", str(text or "")).lower())
    return f" {' '.join(words)} " if words else " "


def search_title(title: str) -> str:
    """Tên dùng để tìm: bỏ chú thích trong ngoặc và dấu ngoặc kép."""
    return " ".join(_PARENS.sub(" ", title).replace('"', " ").split()) or title.strip()


def title_aliases(title: str) -> list[str]:
    main = search_title(title)
    extras = [p for p in _PARENS.findall(title) if len(p.split()) >= 2 or len(p.strip()) >= 5]
    # Phim có số phần: tên gốc trong ngoặc không mang số (vd. "Tee Yod") là tên cả loạt, dùng riêng sẽ bắt cả phần khác.
    if re.search(r"\d", main):
        extras = [p for p in extras if re.search(r"\d", p)]
    candidates = [title, main, *extras]
    # "Quỷ Ăn Tạng 4: Hổ Tinh" cũng được gọi là "Quỷ Ăn Tạng 4"; chỉ lấy phần chính khi đủ dài để không thành từ chung chung.
    head = re.split(r"\s*[:–|]\s*|\s-\s", main)[0]
    if len(head.split()) >= 3:
        candidates.append(head)
    return list(dict.fromkeys(a for a in (normalize(c) for c in candidates) if a.strip()))


def mention_count(text: str | None, aliases: list[str]) -> int:
    haystack = normalize(text)
    return max((haystack.count(alias) for alias in aliases), default=0)


def headline_matches(item: dict, aliases: list[str]) -> bool:
    head = " . ".join(str(item.get(key) or "") for key in ("title", "extracted_title", "snippet"))
    return mention_count(head, aliases) > 0


def named_as_a_work(text: str | None, aliases: list[str]) -> bool:
    """Tên được viết như tên tác phẩm: sau chữ "phim", hoặc nằm trong ngoặc kép."""
    raw = unicodedata.normalize("NFC", str(text or ""))
    quoted = {normalize(part) for part in _QUOTED.findall(raw)}
    haystack = normalize(raw)
    return any(alias in quoted or f" phim{alias}" in haystack for alias in aliases)


def content_matches(text: str | None, aliases: list[str]) -> bool:
    if mention_count(text, aliases) < MIN_CONTENT_MENTIONS:
        return False
    # Tên 1-2 từ ("Huyết Thống", "Mẹ Mìn") hay trùng cụm từ thường: nội dung phải viết nó như tên phim.
    common_phrase = all(len(alias.split()) <= SHORT_TITLE_WORDS for alias in aliases)
    return not common_phrase or named_as_a_work(text, aliases)


def is_relevant(item: dict, aliases: list[str]) -> bool:
    return headline_matches(item, aliases) or content_matches(item.get("content"), aliases)


def published_since(release_date: str | None) -> str | None:
    """Mốc 'YYYY-MM-DD': bài đăng trước ngày công chiếu hơn MAX_DAYS_BEFORE_RELEASE ngày là về tác phẩm khác trùng tên."""
    try:
        return (date.fromisoformat(str(release_date)[:10]) - timedelta(days=MAX_DAYS_BEFORE_RELEASE)).isoformat()
    except ValueError:
        return None


def too_old(item: dict, since: str | None) -> bool:
    """Chỉ loại khi ngày đăng đọc được dạng ISO; không rõ ngày thì giữ."""
    stamp = str(item.get("published_at") or item.get("iso_date") or item.get("date") or "")[:10]
    return bool(since) and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", stamp)) and stamp < since


async def keep_relevant(
    title: str,
    items: list[dict],
    enrich: Callable[[list[dict]], Awaitable[None]] | None = None,
    since: str | None = None,
) -> list[dict]:
    """Giữ nguyên thứ tự. Bài chưa khớp ở tiêu đề được trích nội dung (tối đa news_articles.MAX_ARTICLES bài) rồi xét lại."""
    aliases = title_aliases(title)
    items = [item for item in items if not too_old(item, since)]
    if not aliases:
        return items
    unsure = [item for item in items if not is_relevant(item, aliases)][: news_articles.MAX_ARTICLES]
    if unsure:
        await (enrich or news_articles.enrich)(unsure)
    # Trích nội dung có thể lộ ngày đăng thật, nên xét tuổi bài thêm lần nữa.
    return [item for item in items if is_relevant(item, aliases) and not too_old(item, since)]
