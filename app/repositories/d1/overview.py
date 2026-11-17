"""Số liệu tổng quan cho dashboard (đếm bài/bình luận theo nền tảng, chuỗi theo ngày) và hashtag đi kèm của từ khoá TikTok.

Tách từ store.py (17/11/2026); store.py vẫn re-export mọi tên để code cũ chạy như cũ.
"""

from __future__ import annotations

import json
from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger

logger = get_logger(__name__)


# Redis khoá chung với spider-hub (cùng prefix social_crawler:). Spider: list top hashtag đi kèm sau mỗi lượt crawl
# (constants/tiktok.py RELATED_HASHTAGS_KEY_TMPL). ShortVid: hash hashtag -> số video, cộng dồn qua các lượt
# (app/services/shortvid/shortvid_tiktok.py) - giữ nguyên dấu, chữ thường.
SPIDER_RELATED_KEY_TMPL = f"{REDIS_KEY_PREFIX}tiktok:related_hashtags:{{keyword_id}}"
SHORTVID_COOCCUR_KEY_TMPL = f"{REDIS_KEY_PREFIX}shortvid:cooccur:{{keyword_id}}"
# Hashtag ShortVid thấy đi kèm từ chừng này video trở lên mới tính là liên quan; tối đa chừng này mỗi từ khoá.
SHORTVID_RELATED_MIN_VIDEOS = 3
SHORTVID_RELATED_PER_KEYWORD = 15


async def related_hashtags_for_keywords(keyword_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Hashtag TikTok xuất hiện cùng hashtag của từng từ khoá - gộp từ spider-hub (list sau mỗi lượt crawl) và ShortVid
    (số video cộng dồn), cùng tag (không phân biệt hoa thường) lấy count lớn hơn. Dùng chung cho chip duyệt trên
    dashboard (stats_summary) (AI duyệt hashtag đã bỏ 17/11/2026). Thiếu Redis / key rỗng = chưa có gì."""
    if not keyword_ids:
        return {}
    try:
        client = get_redis_client()
        values = await client.mget([SPIDER_RELATED_KEY_TMPL.format(keyword_id=kid) for kid in keyword_ids])
        pipe = client.pipeline()
        for kid in keyword_ids:
            pipe.hgetall(SHORTVID_COOCCUR_KEY_TMPL.format(keyword_id=kid))
        shortvid_values = await pipe.execute()
    except Exception as exc:  # noqa: BLE001 - Redis lỗi thì coi như chưa có hashtag liên quan
        logger.warning("related_hashtags_redis_failed", error=str(exc))
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for kid, raw, shortvid_raw in zip(keyword_ids, values or [], shortvid_values or [], strict=False):
        merged: dict[str, dict[str, Any]] = {}
        try:
            parsed = json.loads(raw) if raw else []
        except TypeError, json.JSONDecodeError:
            parsed = []
        for item in parsed if isinstance(parsed, list) else []:
            if not isinstance(item, dict) or not item.get("title"):
                continue
            title = str(item["title"]).lstrip("#")
            merged[title.lower()] = {
                "id": str(item.get("id") or ""),
                "title": title,
                "count": int(item.get("count") or 0),
                "bfs_depth": int(item.get("bfs_depth") or 1),
            }
        shortvid_items = sorted(
            ((tag.decode() if isinstance(tag, bytes) else tag, int(n)) for tag, n in (shortvid_raw or {}).items()),
            key=lambda pair: -pair[1],
        )
        for tag, count in [pair for pair in shortvid_items if pair[1] >= SHORTVID_RELATED_MIN_VIDEOS][
            :SHORTVID_RELATED_PER_KEYWORD
        ]:
            entry = merged.setdefault(tag.lower(), {"id": "", "title": tag, "count": 0, "bfs_depth": 1})
            entry["count"] = max(entry["count"], count)
        if merged:
            out[kid] = sorted(merged.values(), key=lambda item: -item["count"])
    return out


async def get_post_counts_by_platform() -> list[dict[str, Any]]:
    """Tổng số bài đã ingest theo nền tảng, cộng thời điểm crawl gần nhất và số ingest hôm
    nay so với hôm qua. Đọc bảng tổng hợp sẵn stats_platform_daily (xem
    app/services/stats/stats_summary.py) - COUNT(*) toàn bộ posts qua D1 HTTP quá chậm cho
    trang Overview."""
    from app.services.stats.stats_summary import get_post_counts_by_platform as _from_summary

    return await _from_summary()


async def get_post_timeseries(days: int) -> list[dict[str, Any]]:
    """Số bài theo ngày của từng nền tảng trong `days` ngày gần nhất - từ bảng tổng hợp
    stats_platform_daily."""
    from app.services.stats.stats_summary import get_post_timeseries as _from_summary

    return await _from_summary(days)


async def get_comment_counts_by_platform() -> list[dict[str, Any]]:
    """Tổng số comment đã ingest theo nền tảng - cùng dạng với get_post_counts_by_platform,
    từ stats_platform_daily."""
    from app.services.stats.stats_summary import get_comment_counts_by_platform as _from_summary

    return await _from_summary()


async def get_comment_timeseries(days: int) -> list[dict[str, Any]]:
    """Số comment theo ngày của từng nền tảng trong `days` ngày gần nhất."""
    from app.services.stats.stats_summary import get_comment_timeseries as _from_summary

    return await _from_summary(days)


async def get_keyword_volume(platform: str | None = None) -> list[dict[str, Any]]:
    """Tổng số bài/comment theo từng từ khoá tìm kiếm cộng số ingest hôm nay so với hôm qua
    - từ bảng tổng hợp stats_keyword_daily."""
    from app.services.stats.stats_summary import get_keyword_volume as _from_summary

    return await _from_summary(platform)
