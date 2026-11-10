"""Tách nguồn (kênh) của một bài: handle, loại kênh và link kênh.

Phải cho cùng kết quả với bản TS ở Worker (`extractHandle`, `channelOfPost`) vì nhãn tác giả và `channel_profiles`
được tra theo handle này ở cả hai phía.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlsplit

_FB_SKIP_FIRST = {"groups", "watch", "reel", "story", "photo", "profile.php", "permalink.php", "pages"}
_GROUP_URL = re.compile(r"facebook\.com/groups/([^/?#]+)", re.IGNORECASE)


def extract_handle(platform: str, author: str | None, url: str | None) -> str:
    safe_author = (author or "").lower()
    url = url or ""
    if platform in ("tiktok", "threads"):
        match = re.search(r"@([^/?]+)", url)
        return match.group(1).lower() if match else safe_author
    if platform == "facebook" and url:
        try:
            parts = urlsplit(url)
            if parts.path in ("/profile.php", "/permalink.php"):
                ids = parse_qs(parts.query).get("id")
                if ids and ids[0]:
                    return ids[0].lower()
            segments = [segment for segment in parts.path.split("/") if segment]
            if segments and segments[0].lower() not in _FB_SKIP_FIRST:
                return segments[0].lower()
        except ValueError:
            pass
    return safe_author


def channel_of_post(post: dict[str, Any]) -> dict[str, str] | None:
    url = post.get("url") or ""
    if post.get("platform") == "facebook":
        group = _GROUP_URL.search(url)
        if group:
            return {"kind": "group", "handle": f"groups/{group.group(1).lower()}"}
    handle = extract_handle(post.get("platform") or "", post.get("author"), url)
    if not handle:
        return None
    return {"kind": "page" if post.get("platform") == "facebook" else "creator", "handle": handle}


def channel_url(platform: str, kind: str, handle: str) -> str:
    if kind == "group":
        return f"https://www.facebook.com/{handle}"
    if platform == "tiktok":
        return f"https://www.tiktok.com/@{handle}"
    if platform == "threads":
        return f"https://www.threads.net/@{handle}"
    return (
        f"https://www.facebook.com/profile.php?id={handle}"
        if handle.isdigit()
        else f"https://www.facebook.com/{handle}"
    )


RELEVANT_POST_FILTER = "(p.relevance_label = 'related' OR (p.relevance_label IS NULL AND p.keyword_match > 0))"
