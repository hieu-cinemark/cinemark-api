"""Thống kê dashboard được tổng hợp sẵn cho D1 remote.

COUNT(*) toàn bảng posts/comments qua HTTP API của Cloudflare quá chậm cho trang
Overview (vài giây mỗi endpoint). Hai bảng tổng hợp theo ngày này đủ nhỏ để đọc trong
vài chục mili giây. Từ 30/10/2026 chúng KHÔNG còn được cộng theo từng bài/comment (mỗi lần
lưu tốn thêm 2 lần ghi D1 - hơn 200k lần ghi phụ mỗi ngày, góp phần làm D1 quá tải 7429):
reconcile_recent_days() đếm lại từ bảng gốc theo first_seen_at mỗi 10 phút (scheduler), nên
số trong ngày trễ tối đa ~10 phút.

    stats_platform_daily  (day, platform) -> posts, comments, last_scraped_at
    stats_keyword_daily   (day, keyword_id) -> platform, posts, comments, last_scraped_at

Dựng lại từ các dòng hiện có bằng:

    python -m scripts.jobs.rebuild_stats_summaries

Ngoài ra có bộ đếm theo giờ (biểu đồ "24 giờ gần nhất" của dashboard) nằm trong Redis
chứ không phải D1: mỗi bài/comment mới thêm một lượt HINCRBY, giữ HOURLY_RETENTION_HOURS
rồi tự hết hạn - không tốn thêm lượt ghi D1 nào. Không dựng lại được từ D1; bị mất thì
biểu đồ chỉ trống phần giờ đó.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    and_,
    case,
    func,
    select,
)

from app.core.logging import get_logger

# Bảng nguồn của phần tổng hợp - import với tên khác để không trùng với tham số
# `posts` / `comments` của subtract_deleted (biến trùng tên bảng làm dùng nhầm, lỗi bị
# `except` nuốt im lặng - xem docs/review-angles-va-attack.md mục 3).
from app.services.stats.stats_hourly import bump_hourly
from app.services.stats.stats_tables import (
    SQL_TODAY,
    SQL_YESTERDAY,
    _as_int,
    ensure_stats_tables,
    keyword_daily,
    platform_daily,
    run_stats_query,
)

logger = get_logger(__name__)


async def record_post(
    *,
    platform: str,
    keyword_id: str | None,
    scraped_at: str,
    is_new: bool,
) -> None:
    """Đếm bài vào bộ đếm theo giờ (Redis) sau khi ghi - bảng tổng hợp ngày do reconcile_recent_days lo (xem docstring
    module). keyword_id giữ trong chữ ký cho bên gọi, không dùng nữa."""
    if not scraped_at or not platform:
        return
    await bump_hourly(platform, scraped_at, "posts" if is_new else "updated")


async def record_comment(
    *,
    platform: str,
    keyword_id: str | None,
    scraped_at: str,
    is_new: bool,
) -> None:
    """Đếm comment MỚI vào bộ đếm theo giờ (Redis) - xem record_post."""
    if not scraped_at or not platform or not is_new:
        return
    await bump_hourly(platform, scraped_at, "comments")


async def _counts_by_platform(metric: str) -> list[dict[str, Any]]:
    """Tổng `metric` (posts/comments) theo nền tảng, kèm hôm nay và hôm qua."""
    await ensure_stats_tables()
    t = platform_daily.c
    value = t[metric]
    rows = await run_stats_query(
        select(
            t.platform,
            func.sum(value).label("count"),
            func.max(t.last_scraped_at).label("last_scraped_at"),
            func.sum(case((t.day == SQL_TODAY, value), else_=0)).label("count_today"),
            func.sum(case((t.day == SQL_YESTERDAY, value), else_=0)).label("count_prev"),
        ).group_by(t.platform)
    )
    return [
        {
            **row,
            "count": _as_int(row.get("count")),
            "count_today": _as_int(row.get("count_today")),
            "count_prev": _as_int(row.get("count_prev")),
        }
        for row in (rows or [])
    ]


async def get_post_counts_by_platform() -> list[dict[str, Any]]:
    return await _counts_by_platform("posts")


async def get_comment_counts_by_platform() -> list[dict[str, Any]]:
    return await _counts_by_platform("comments")


async def _timeseries(metric: str, days: int) -> list[dict[str, Any]]:
    await ensure_stats_tables()
    t = platform_daily.c
    rows = await run_stats_query(
        select(t.day, t.platform, t[metric].label("count"))
        .where(t.day >= func.date("now", f"-{days} days"))
        .order_by(t.day)
    )
    return rows or []


async def get_post_timeseries(days: int) -> list[dict[str, Any]]:
    return await _timeseries("posts", days)


async def get_comment_timeseries(days: int) -> list[dict[str, Any]]:
    return await _timeseries("comments", days)


async def get_keyword_volume(platform: str | None = None) -> list[dict[str, Any]]:
    """Cùng dạng response với phép join toàn bảng cũ, lấy dữ liệu từ bảng tổng hợp theo ngày."""
    from app.repositories.d1.overview import related_hashtags_for_keywords

    await ensure_stats_tables()
    from app.repositories.d1.tables import keywords, movies

    kd = keyword_daily.c
    totals = (
        select(
            kd.keyword_id,
            func.sum(kd.posts).label("posts_total"),
            func.sum(kd.comments).label("comments_total"),
            func.max(kd.last_scraped_at).label("last_scraped_at"),
        )
        .group_by(kd.keyword_id)
        .subquery("tot")
    )
    today, prev = keyword_daily.alias("today"), keyword_daily.alias("prev")
    k, m = keywords.alias("k"), movies.alias("m")
    query = select(
        k.c.id.label("keyword_id"),
        k.c.movie_id,
        k.c.keyword,
        k.c.platform,
        k.c.enabled,
        m.c.title.label("movie_title"),
        func.coalesce(totals.c.posts_total, 0).label("posts_total"),
        func.coalesce(today.c.posts, 0).label("posts_today"),
        func.coalesce(prev.c.posts, 0).label("posts_prev"),
        totals.c.last_scraped_at,
        func.coalesce(totals.c.comments_total, 0).label("comments_total"),
        func.coalesce(today.c.comments, 0).label("comments_today"),
        func.coalesce(prev.c.comments, 0).label("comments_prev"),
    ).select_from(
        k.outerjoin(m, m.c.id == k.c.movie_id)
        .outerjoin(totals, totals.c.keyword_id == k.c.id)
        .outerjoin(today, and_(today.c.keyword_id == k.c.id, today.c.day == SQL_TODAY))
        .outerjoin(prev, and_(prev.c.keyword_id == k.c.id, prev.c.day == SQL_YESTERDAY))
    )
    if platform:
        query = query.where(k.c.platform == platform)
    rows = await run_stats_query(query)
    out: list[dict[str, Any]] = []
    for row in rows or []:
        posts_total = _as_int(row.get("posts_total"))
        enabled = bool(row.get("enabled"))
        if posts_total == 0 and not enabled:
            continue
        out.append(
            {
                "keyword_id": row["keyword_id"],
                "movie_id": row.get("movie_id") or "",
                "keyword": row.get("keyword") or "",
                "platform": row.get("platform") or "",
                "enabled": enabled,
                "movie_title": row.get("movie_title"),
                "posts_total": posts_total,
                "posts_today": _as_int(row.get("posts_today")),
                "posts_prev": _as_int(row.get("posts_prev")),
                "comments_total": _as_int(row.get("comments_total")),
                "comments_today": _as_int(row.get("comments_today")),
                "comments_prev": _as_int(row.get("comments_prev")),
                "last_scraped_at": row.get("last_scraped_at"),
                "related_hashtags": [],
            }
        )
    related_by_kw = await related_hashtags_for_keywords(
        [row["keyword_id"] for row in out if row.get("platform") == "tiktok"]
    )
    for row in out:
        row["related_hashtags"] = related_by_kw.get(row["keyword_id"], [])
    out.sort(key=lambda r: (r["posts_today"], r["posts_total"]), reverse=True)
    return out
