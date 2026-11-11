"""Bài "hưởng ứng trend" của một phim: người xem gắn hashtag phim vào nội dung của mình, vd. "Ngày mẹ đồng ý
#bosuabay" kèm ảnh cưới bố mẹ. Bình luận dưới các bài này nói về chuyện khác ("mẹ tui siêu đẹp"), nên không tính vào các chủ đề
của phim mà gom vào nhóm riêng: vẫn là tín hiệu lan toả.

Một bài là trend khi nó không nhắc phim hay diễn viên theo một trong hai cách:
- caption (bỏ hashtag) không có tên phim hay từ về phim, và cả bài không có tên diễn viên, kể cả ở hashtag;
- hoặc cùng một câu caption xuất hiện ở từ MIN_REPEAT_AUTHORS tác giả mà không có tên phim: bài cùng trào lưu vẫn hay kèm hashtag
  ca sĩ/diễn viên hát nhạc nền (#nguyenhung).
Tài khoản có tên phim trong tên (page phim) không bao giờ là trend. Đây là luật, có thể sai ở review chỉ gắn hashtag.
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, update

from app.clients.d1 import d1_query
from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec, json_values
from app.repositories.d1.tables import movie_measure_settings, movies
from app.repositories.d1.tables import posts as posts_table

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
SHOWING_DAYS_AFTER_RELEASE = 30
MIN_REPEAT_AUTHORS = 3
MIN_TITLE_KEY = 5
UPDATE_CHUNK = 90  # D1 tối đa 100 tham số mỗi câu
_HASHTAG = re.compile(r"#\S+")
_FILM_WORDS = re.compile(
    r"\b(phim|rap|ra rap|trailer|teaser|review|ve|suat|chieu|khoi chieu|dien vien|nhan vat|premiere|dao dien|ost|canh)\b"
)
_REVIEW_TAG = re.compile(r"#\w*review", re.IGNORECASE)
_column_ready = False


def plain(text: str | None) -> str:
    """Chữ thường, bỏ dấu."""
    folded = unicodedata.normalize("NFD", (text or "").lower().replace("đ", "d"))
    return "".join(char for char in folded if unicodedata.category(char) != "Mn")


def _compact(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text)


def title_keys(title: str) -> list[str]:
    """Các cách viết tên phim: cả tên và từng vế tách bởi ":", ngoặc, gạch ngang, bỏ số phần ở cuối. Caption hiếm khi viết đủ
    "Shaun the Sheep: Quái Lạ Ghé Nhà" hay "QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)"."""
    folded = plain(title)
    parts = [folded, *re.split(r"[:()\[\]\-–—|/]", folded)]
    keys = {_compact(part) for part in parts} | {_compact(re.sub(r"\s*\d+\s*$", "", part)) for part in parts}
    return sorted(key for key in keys if len(key) >= MIN_TITLE_KEY)


def caption_key(content: str | None) -> str:
    """Caption bỏ hashtag, icon và dấu câu, để so hai bài có cùng câu không."""
    words = re.sub(r"[^a-z0-9 ]", " ", _HASHTAG.sub(" ", plain(content)))
    return " ".join(words.split())


def parse_cast(cast_json: str | None) -> list[str]:
    try:
        names = json.loads(cast_json or "[]")
    except ValueError:
        return []
    return [name for name in names if isinstance(name, str) and name.strip()] if isinstance(names, list) else []


def repeated_captions(posts: list[dict[str, Any]]) -> set[str]:
    authors: dict[str, set[str]] = defaultdict(set)
    for post in posts:
        key = caption_key(post.get("content"))
        if key:
            authors[key].add(str(post.get("author") or ""))
    return {key for key, names in authors.items() if len(names) >= MIN_REPEAT_AUTHORS}


def is_trend_post(post: dict[str, Any], title: str, cast: list[str], repeated: set[str]) -> bool:
    keys = title_keys(title)
    author = _compact(plain(post.get("author")))
    if any(key in author for key in keys):
        return False
    key = caption_key(post.get("content"))
    caption = _compact(key)
    if any(title_key in caption for title_key in keys):
        return False
    if key in repeated:
        return True
    if _FILM_WORDS.search(key) or _REVIEW_TAG.search(plain(post.get("content"))):
        return False
    whole = _compact(plain(post.get("content")))
    return not any(len(name) >= 4 and name in whole for name in (_compact(plain(member)) for member in cast))


async def ensure_column() -> None:
    global _column_ready
    if _column_ready:
        return
    cols = await d1_query("PRAGMA table_info(posts)", quiet=True)
    if cols is not None and "trend_post" not in {row.get("name") for row in cols}:
        await d1_query("ALTER TABLE posts ADD COLUMN trend_post INTEGER NOT NULL DEFAULT 0", quiet=True)
    _column_ready = True


async def showing_movies(now: datetime) -> list[dict[str, Any]]:
    today = now.astimezone(TIMEZONE).date().isoformat()
    m, s = movies.alias("m"), movie_measure_settings.alias("s")
    opening = func.coalesce(func.nullif(s.c.premiere_date, ""), m.c.released_at)
    rows = await d1_exec(
        select(m.c.id, m.c.title, m.c.cast.label("cast_json"))
        .select_from(m.outerjoin(s, s.c.movie_id == m.c.id))
        .where(
            m.c.enabled == 1,
            m.c.released_at.is_not(None),
            func.date(opening, "-120 day") <= func.date(today),
            func.date(m.c.released_at, f"+{SHOWING_DAYS_AFTER_RELEASE} day") >= func.date(today),
        ),
        quiet=True,
    )
    return rows or []


async def mark_movie(movie: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    """Quét mọi bài liên quan của phim, chỉ ghi các bài có nhãn đổi."""
    if not dry_run:
        await ensure_column()
    cols = await d1_query("PRAGMA table_info(posts)", quiet=True)
    has_column = "trend_post" in {row.get("name") for row in cols or []}
    p = posts_table.alias("p")
    columns = [p.c.id, p.c.author, func.substr(p.c.content, 1, 400).label("content")]
    if has_column:
        columns.append(p.c.trend_post)
    posts = await d1_exec(
        select(*columns).where(p.c.movie_id == movie["id"], relevant_post(p)), timeout=60.0, quiet=True
    )
    if posts is None:
        raise RuntimeError("trend_posts_read_failed")
    cast = parse_cast(movie.get("cast_json"))
    repeated = repeated_captions(posts)
    flagged = [(post, is_trend_post(post, movie["title"], cast, repeated)) for post in posts]
    changes = {1: [], 0: []}
    for post, trend in flagged:
        if int(post.get("trend_post") or 0) != int(trend):
            changes[int(trend)].append(post["id"])
    if not dry_run:
        for value, ids in changes.items():
            for i in range(0, len(ids), UPDATE_CHUNK):
                chunk = ids[i : i + UPDATE_CHUNK]
                await d1_exec(
                    update(posts_table).where(posts_table.c.id.in_(json_values(chunk))).values(trend_post=value),
                    timeout=60.0,
                    quiet=True,
                )
    return {
        "posts": len(posts),
        "trend": sum(trend for _, trend in flagged),
        "changed": len(changes[1]) + len(changes[0]),
        "trend_samples": [post for post, trend in flagged if trend],
        "other_samples": [post for post, trend in flagged if not trend],
    }


async def mark_showing_movies() -> int:
    total = 0
    for movie in await showing_movies(datetime.now(tz=UTC)):
        total += (await mark_movie(movie))["changed"]
    return total
