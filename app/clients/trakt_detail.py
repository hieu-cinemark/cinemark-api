"""Trang chi tiết phim Trakt (28/11/2026): gộp thông tin, diễn viên / ê-kíp, điểm các nguồn, bình luận, video, phim liên quan, hãng
và lượt xem trong một lần gọi. Chỉ giữ trường giao diện cần (không chuyển tiếp hồ sơ người bình luận). Phần nào Trakt lỗi thì để
trống, chỉ thiếu thông tin phim mới coi là lỗi. "Where to watch" (watchnow) không có: Trakt từ chối key thường."""

from __future__ import annotations

import asyncio
from typing import Any

from app.clients import trakt
from app.core.errors import NotFoundError, UpstreamError

CAST_LIMIT = 24
COMMENT_LIMIT = 10
RELATED_LIMIT = 12


def _first(values: Any) -> str | None:
    return values[0] if isinstance(values, list) and values else None


def people_of(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Diễn viên (tên, vai, ảnh) + tên đạo diễn / biên kịch."""
    raw = raw or {}
    cast = [
        {
            "id": (item.get("person") or {}).get("ids", {}).get("trakt"),
            "name": (item.get("person") or {}).get("name"),
            "character": ", ".join(item.get("characters") or []) or item.get("character") or None,
            "headshot": _first(((item.get("person") or {}).get("images") or {}).get("headshot")),
        }
        for item in (raw.get("cast") or [])[:CAST_LIMIT]
    ]
    crew = raw.get("crew") or {}

    def names(group: str, jobs: set[str]) -> list[str]:
        found = [
            (item.get("person") or {}).get("name")
            for item in crew.get(group) or []
            if jobs & set(item.get("jobs") or [item.get("job")])
        ]
        return list(dict.fromkeys(name for name in found if name))

    return {
        "cast": [item for item in cast if item["name"]],
        "directors": names("directing", {"Director"}),
        "writers": names("writing", {"Writer", "Screenplay", "Story"}),
    }


def comments_of(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Bình luận không đánh dấu spoiler: nội dung, điểm người viết chấm, tên hiển thị."""
    out = []
    for item in raw or []:
        if item.get("spoiler"):
            continue
        user = item.get("user") or {}
        out.append(
            {
                "id": item.get("id"),
                "comment": item.get("comment") or "",
                "created_at": item.get("created_at"),
                "likes": item.get("likes") or 0,
                "rating": item.get("user_rating"),
                "language": item.get("language"),
                "author": user.get("username") or "Trakt user",
            }
        )
    return out[:COMMENT_LIMIT]


async def detail(movie_id: str) -> dict[str, Any]:
    summary = await trakt.movie(movie_id)
    if not summary:
        raise NotFoundError("Trakt has no movie with this id")

    async def optional(path: str, params: dict[str, Any] | None = None) -> Any:
        try:
            return await trakt._get(f"/movies/{movie_id}{path}", params)
        except UpstreamError:
            return None

    people, ratings, comments, videos, related, studios, stats = await asyncio.gather(
        optional("/people", {"extended": "images"}),
        optional("/ratings", {"extended": "all"}),
        optional("/comments/likes", {"limit": COMMENT_LIMIT}),
        optional("/videos"),
        optional("/related", {"limit": RELATED_LIMIT, "extended": trakt.EXTENDED}),
        optional("/studios"),
        optional("/stats"),
    )
    return {
        "movie": summary,
        "people": people_of(people),
        "ratings": ratings or {},
        "comments": comments_of(comments),
        "videos": [v for v in videos or [] if v.get("site") == "youtube" and v.get("url")],
        "related": related or [],
        "studios": list(dict.fromkeys(s.get("name") for s in studios or [] if s.get("name"))),
        "stats": {k: (stats or {}).get(k) for k in ("watchers", "plays", "lists")},
    }
