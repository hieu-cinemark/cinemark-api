"""Rút gọn raw_json của bài và dựng lại URL ảnh/video từ nó khi đọc ra (D1 đã ~7,6 GB nên chỉ giữ các khoá đang được đọc).

Tách từ posts.py (17/11/2026); posts.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

import json
from typing import Any

from app.core.logging import get_logger

logger = get_logger(__name__)


# Khoá của raw_json bài đang được đọc (json_extract ở đây và ở cinemark). Bản đầy đủ nằm ở tầng bronze trên R2 (lake_writer):
# lưu cả khối trên D1 khiến mọi truy vấn json_extract phải đọc thêm nhiều trang (D1 đã ~7,6 GB).
# 18/11/2026: thêm tín hiệu tác giả cho mô hình phân loại tầng (app/ml/author_tier.py) - tick xanh, tài khoản business / shop,
# nhãn quảng cáo / hợp tác, bio. Trước đó chúng bị cắt ở đây nên D1 không có, mô hình chỉ học được từ tên và caption.
RAW_JSON_KEEP = (
    "reactions",
    "collect_count",
    "author_name",
    "author_followers",
    "media_url",
    "cover_url",
    "author_verified",
    "author_is_business",
    "author_is_seller",
    "author_signature",
    "is_ad",
    "is_branded",
)
# Bio lặp lại ở mọi bài của tác giả: chỉ giữ đoạn đầu.
SIGNATURE_MAX_CHARS = 160


def trim_post_raw(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    kept = {key: raw[key] for key in RAW_JSON_KEEP if raw.get(key) is not None}
    if isinstance(kept.get("author_signature"), str):
        kept["author_signature"] = kept["author_signature"][:SIGNATURE_MAX_CHARS] or None
        if kept["author_signature"] is None:
            kept.pop("author_signature")
    return kept or None


# Video phát được / permalink không đặt vào <img> được. Các dòng TikTok cũ lưu playAddr
# làm media_url và chỉ để cover_url trong raw_json.
_VIDEO_URL_HINTS = (
    ".mp4",
    ".m3u8",
    "/video/tos/",
    "webapp-prime.tiktok.com",
    "facebook.com/reel/",
    "facebook.com/watch",
    "facebook.com/share/v",
    "facebook.com/video",
    "tiktok.com/@",
    "threads.com/@",
    "threads.net/@",
)
_IMAGE_URL_HINTS = (
    "fbcdn.net",
    "cdninstagram.com",
    "tiktokcdn",
    "byteicdn",
    "ibyteimg",
    "byteimg.com",
    "muscdn.com",
    "scontent",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
)


def _is_preview_image_url(url: Any) -> bool:
    if not isinstance(url, str) or not url.startswith("http"):
        return False
    lower = url.lower()
    if any(hint in lower for hint in _VIDEO_URL_HINTS):
        return False
    return any(hint in lower for hint in _IMAGE_URL_HINTS)


def hydrate_post_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out = rows or []
    for row in out:
        media = json.loads(row.pop("media_json") or "{}")
        raw_cover = row.pop("raw_cover_url", None)
        row["media_type"] = media.get("media_type")
        candidates = (
            media.get("cover_url"),
            raw_cover,
            media.get("thumbnail_url"),
            media.get("media_url"),
        )
        row["media_url"] = next((url for url in candidates if _is_preview_image_url(url)), None)
        quoted = media.get("quoted") if isinstance(media.get("quoted"), dict) else None
        row["quoted"] = (
            {
                "author": quoted.get("author"),
                "content": quoted.get("content"),
                "url": quoted.get("url"),
                "media_url": quoted.get("media_url") if _is_preview_image_url(quoted.get("media_url")) else None,
            }
            if quoted
            else None
        )
    return out
