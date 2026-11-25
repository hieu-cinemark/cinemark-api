"""Trang hashtag YouTube (youtube.com/hashtag/<tên>), không key, không đăng nhập (25/11/2026).

Đầu trang ghi sẵn tổng số video và số kênh của hashtag ("464 videos • 124 channels") trong ytInitialData:
header.pageHeaderRenderer.content.pageHeaderViewModel.metadata.contentMetadataViewModel.metadataRows[].metadataParts[].
Gọi với hl=en để chữ luôn là tiếng Anh ("8.5M videos"), dễ đọc thành số. YouTube không cho tổng lượt xem của hashtag.
Đây là trang web chứ không phải API chính thức: YouTube đổi giao diện thì `parse_counts` trả None (không đoán số).
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

HASHTAG_URL = "https://www.youtube.com/hashtag/{name}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
# Bỏ qua trang xin đồng ý cookie (chỉ hiện ở một số vùng).
COOKIES = {"SOCS": "CAI", "CONSENT": "YES+"}

_INITIAL_DATA = re.compile(r"var ytInitialData = (\{.*?\});</script>", re.DOTALL)
_SCALE = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}


def _number(text: str, unit: str) -> int | None:
    """'464 videos' / '1,234 videos' / '8.5M videos' -> số; không thấy thì None."""
    match = re.search(rf"([\d.,]+)\s*([KMB]?)\s+{unit}s?\b", text, re.IGNORECASE)
    if not match:
        return None
    raw, scale = match.group(1), match.group(2).upper()
    value = float(raw.replace(",", "")) if scale else float(raw.replace(",", "").replace(".", ""))
    return round(value * _SCALE[scale])


def _metadata_texts(data: dict[str, Any]) -> list[str]:
    model = (((data.get("header") or {}).get("pageHeaderRenderer") or {}).get("content") or {}).get(
        "pageHeaderViewModel"
    ) or {}
    rows = ((model.get("metadata") or {}).get("contentMetadataViewModel") or {}).get("metadataRows") or []
    return [
        str((part.get("text") or {}).get("content") or "") for row in rows for part in row.get("metadataParts") or []
    ]


def parse_counts(html: str) -> dict[str, int] | None:
    """{video_count, channel_count} từ HTML trang hashtag; None khi trang không có số thật (hashtag không tồn tại, dưới 100
    video - YouTube chỉ ghi "< 100", hoặc đổi giao diện)."""
    match = _INITIAL_DATA.search(html)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except ValueError:
        return None
    for text in _metadata_texts(data):
        # Hashtag nhỏ: YouTube chỉ ghi "< 100 videos and channels", không có số thật - không đoán.
        if "<" in text:
            return None
        videos = _number(text, "video")
        if videos is not None:
            return {"video_count": videos, "channel_count": _number(text, "channel") or 0}
    return None


async def hashtag_counts(client: httpx.AsyncClient, name: str) -> dict[str, int] | None:
    """Tổng video / kênh của một hashtag (tên không có "#"). Lỗi mạng / HTTP thì ném httpx.HTTPError."""
    response = await client.get(
        HASHTAG_URL.format(name=name),
        params={"hl": "en", "gl": "VN"},
        headers=HEADERS,
        cookies=COOKIES,
        follow_redirects=True,
    )
    response.raise_for_status()
    return parse_counts(response.text)
