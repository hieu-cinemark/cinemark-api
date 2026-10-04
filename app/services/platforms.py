"""Danh sách đăng ký các nền tảng mạng xã hội mà service này biết cách kích hoạt crawl
và ingest bài - cùng kiểu registry với SPIDER_BY_PLATFORM (crawl_request_consumer.py)
của spider-hub và scrapers/registry.ts của cinemark-scraper, được giữ đồng bộ theo
quy ước chứ không qua code dùng chung nào giữa ba service.

Thêm một nền tảng mới ở đây nghĩa là:
1. Thêm mapper của nó bên dưới (payload Kafka thô -> PostDraft).
2. Bảo đảm crawl_request_consumer.py của spider-hub có mục SPIDER_BY_PLATFORM tương
   ứng, nếu không các lượt crawl được kích hoạt cho nó sẽ âm thầm biến mất (xem
   docstring get_keyword trong app/services/d1.py).
3. Thêm router của nó (app/api/routes/<platform>.py, cùng dạng với facebook.py) và
   đăng ký trong app/main.py.
Không chỗ nào khác trong service này - app/services/d1.py,
app/workers/ingest_consumer, app/api/routes/platform_scraper.py - cần sửa."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

# Dạng chuẩn hoá mà mọi mapper nền tảng bên dưới phải sinh ra - cùng các trường với
# kiểu PostDraft của cinemark-scraper (src/scrapers/types.ts), để persist_post() trong
# app/services/d1.py không phải tự biết trường riêng của nền tảng nào. `media`/`raw` ở
# đây là dict, chỉ mã hoá JSON lúc ghi vào D1.
PostDraft = dict[str, Any]


def _quoted_media(payload: dict[str, Any]) -> dict[str, Any] | None:
    quoted = payload.get("quoted")
    if not isinstance(quoted, dict):
        return None
    out = {key: quoted.get(key) for key in ("author", "content", "url", "media_url") if quoted.get(key)}
    return out or None


def _map_facebook_post(payload: dict[str, Any]) -> PostDraft:
    """Tên trường FacebookPostItem của spider-hub -> PostDraft. Cùng cách ánh xạ mà scraper
    facebook.ts của cinemark-scraper dùng (reactions = likes, comments = replies, shares
    = reposts) - Facebook không có khái niệm quote/reshare riêng như Threads, nên các số
    đó giữ 0."""
    timestamp = payload.get("timestamp")
    return {
        "external_id": payload.get("post_id"),
        "url": payload.get("url"),
        "author": payload.get("author_name"),
        "content": payload.get("message"),
        "media": {
            "media_type": payload.get("media_type"),
            "media_url": payload.get("media_url"),
            "cover_url": payload.get("cover_url"),
            "duration_seconds": payload.get("duration_seconds"),
            **({"quoted": quoted} if (quoted := _quoted_media(payload)) else {}),
        },
        "like_count": payload.get("reactions_count") or 0,
        "reply_count": payload.get("comments_count") or 0,
        "repost_count": payload.get("shares_count") or 0,
        "quote_count": 0,
        "reshare_count": 0,
        "view_count": 0,
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


def _map_threads_post(payload: dict[str, Any]) -> PostDraft:
    """Tên trường ThreadsPostItem của spider-hub -> PostDraft. Khác Facebook, Threads có số
    repost/quote thật của riêng nó (không gộp thành một con số "shares"), nên ánh xạ thẳng
    thay vì mặc định 0."""
    timestamp = payload.get("timestamp")
    return {
        "external_id": payload.get("post_id"),
        "url": payload.get("url"),
        "author": payload.get("author_username") or payload.get("author_name"),
        "content": payload.get("message"),
        "media": {
            "media_type": payload.get("media_type"),
            "media_url": payload.get("media_url"),
            "cover_url": payload.get("cover_url"),
            **({"quoted": quoted} if (quoted := _quoted_media(payload)) else {}),
        },
        "like_count": payload.get("like_count") or 0,
        "reply_count": payload.get("reply_count") or 0,
        "repost_count": payload.get("repost_count") or 0,
        "quote_count": payload.get("quote_count") or 0,
        "reshare_count": 0,
        "view_count": 0,
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


def _map_tiktok_post(payload: dict[str, Any]) -> PostDraft:
    """Tên trường TikTokVideoItem của spider-hub -> PostDraft. Khác Facebook/Threads, TikTok
    có sẵn số lượt phát/xem thật (không gộp vào likes như "reactions" của Facebook), nên
    view_count ánh xạ vào đó thay vì mặc định 0. TikTok không có khái niệm repost/quote
    tách khỏi số share, nên repost_count lấy số share còn quote_count/reshare_count giữ
    0, cùng lý do như shares -> repost_count của _map_facebook_post."""
    timestamp = payload.get("create_time")
    return {
        "external_id": payload.get("video_id"),
        "url": payload.get("url"),
        "author": payload.get("author_username") or payload.get("author_name"),
        "content": payload.get("desc"),
        "media": {
            "media_type": "video",
            "media_url": payload.get("play_url"),
            "cover_url": payload.get("cover_url"),
            "play_url": payload.get("play_url"),
            "duration_seconds": payload.get("duration"),
        },
        "like_count": payload.get("like_count") or 0,
        "reply_count": payload.get("comment_count") or 0,
        "repost_count": payload.get("share_count") or 0,
        "quote_count": 0,
        "reshare_count": 0,
        "view_count": payload.get("play_count") or 0,
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


PLATFORM_POST_MAPPERS: dict[str, Callable[[dict[str, Any]], PostDraft]] = {
    "facebook": _map_facebook_post,
    "threads": _map_threads_post,
    "tiktok": _map_tiktok_post,
}


def get_post_mapper(platform: str) -> Callable[[dict[str, Any]], PostDraft] | None:
    return PLATFORM_POST_MAPPERS.get(platform)


def registered_platforms() -> set[str]:
    return set(PLATFORM_POST_MAPPERS)


# Dạng chuẩn hoá mà mọi mapper comment bên dưới phải sinh ra - giống bảng comments của
# cinemark-scraper (src/db/schema.ts) theo cùng cách PostDraft giống bảng posts.
CommentDraft = dict[str, Any]


def _map_facebook_comment(payload: dict[str, Any]) -> CommentDraft:
    """Tên trường FacebookCommentItem của spider-hub -> CommentDraft (xem
    social_crawler/spiders/facebook/items.py bên đó)."""
    timestamp = payload.get("timestamp")
    return {
        "external_id": payload.get("comment_id"),
        "message": payload.get("message"),
        "author_name": payload.get("author_name"),
        "author_id": payload.get("author_id"),
        "author_url": payload.get("author_url"),
        "author_profile_picture": payload.get("author_profile_picture"),
        "reactions_count": payload.get("reactions_count") or 0,
        "replies_count": payload.get("replies_count") or 0,
        "parent_external_id": payload.get("parent_comment_id"),
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


def _map_threads_comment(payload: dict[str, Any]) -> CommentDraft:
    """Tên trường ThreadsCommentItem của spider-hub -> CommentDraft (xem
    social_crawler/spiders/threads/items.py bên đó). Threads không tách
    reactions/replies theo cách payload của Facebook đặt tên - "like_count"/"reply_count"
    ánh xạ thẳng vào cùng các cột reactions_count/replies_count. author_url không phải
    trường spider-hub lấy cho Threads (chỉ có username/name/id/avatar), nên được dựng ở
    đây từ username theo đúng cách author_url của ThreadsPostItem được dựng."""
    timestamp = payload.get("timestamp")
    username = payload.get("author_username")
    return {
        "external_id": payload.get("reply_id"),
        "message": payload.get("message"),
        "author_name": payload.get("author_name") or username,
        "author_id": payload.get("author_id"),
        "author_url": f"https://www.threads.com/@{username}" if username else None,
        "author_profile_picture": payload.get("author_profile_picture"),
        "reactions_count": payload.get("like_count") or 0,
        "replies_count": payload.get("reply_count") or 0,
        "parent_external_id": payload.get("parent_reply_id"),
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


def _map_tiktok_comment(payload: dict[str, Any]) -> CommentDraft:
    """Tên trường TikTokCommentItem của spider-hub -> CommentDraft (xem
    social_crawler/spiders/tiktok/items.py bên đó)."""
    timestamp = payload.get("timestamp")
    username = payload.get("author_username")
    return {
        "external_id": payload.get("comment_id"),
        "message": payload.get("message"),
        "author_name": payload.get("author_name") or username,
        "author_id": payload.get("author_id"),
        "author_url": f"https://www.tiktok.com/@{username}" if username else None,
        "author_profile_picture": payload.get("author_avatar_url"),
        "reactions_count": payload.get("like_count") or 0,
        "replies_count": payload.get("reply_count") or 0,
        "parent_external_id": payload.get("parent_comment_id"),
        "posted_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
        "raw": payload,
    }


PLATFORM_COMMENT_MAPPERS: dict[str, Callable[[dict[str, Any]], CommentDraft]] = {
    "facebook": _map_facebook_comment,
    "threads": _map_threads_comment,
    "tiktok": _map_tiktok_comment,
}


def get_comment_mapper(platform: str) -> Callable[[dict[str, Any]], CommentDraft] | None:
    return PLATFORM_COMMENT_MAPPERS.get(platform)


# Các nền tảng mà spider-hub thực sự có spider comment (xem COMMENTS_SPIDER_BY_PLATFORM
# trong crawl_request_consumer.py bên đó) - cùng tập với các key của
# PLATFORM_COMMENT_MAPPERS, đặt tên riêng vì các chỗ gọi như lịch crawl comment trong
# app/services/scheduler.py quan tâm "comment của nền tảng này có crawl được không",
# không phải bản thân mapper.
COMMENT_CRAWL_PLATFORMS: set[str] = set(PLATFORM_COMMENT_MAPPERS.keys())
