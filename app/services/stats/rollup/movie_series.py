"""Tổng số liệu của một phim trong một khoảng ngày bất kỳ, đọc từ bảng tổng hợp theo ngày + prefix sum (prefix_sum.py).

Mỗi phim đọc D1 hai câu nhỏ (bài: movie_post_daily, bình luận: movie_comment_daily - vài trăm dòng), cache theo PHIM ở Redis
(stats_cache.cached_rows, tươi 60 giây); mọi khoảng ngày sau đó tính trong bộ nhớ. Trước đây mỗi khoảng ngày là một lần GROUP BY
trên bảng posts / comments (hàng trăm nghìn dòng).

`complete` = mọi ngày đăng trong khoảng đã có trong bảng tổng hợp bài (movie_post_rollup_days); False thì số bài có thể thiếu -
bên gọi nên báo "chưa đủ dữ liệu" thay vì coi là 0.
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

from app.repositories.d1 import post_rollup as repo
from app.services.stats.rollup.helper import day_label
from app.services.stats.rollup.prefix_sum import DailySeries
from app.services.stats.stats_cache import cached_rows


async def _series(movie_id: str) -> tuple[DailySeries, DailySeries]:
    posts = await cached_rows(f"movie_post_days:{movie_id}", lambda: _rows(repo.load_movie_post_days(movie_id)))
    comments = await cached_rows(f"movie_comment_days:{movie_id}", lambda: _rows(repo.load_movie_comment_days(movie_id)))
    return DailySeries.from_rows(posts, repo.POST_METRICS), DailySeries.from_rows(comments, repo.COMMENT_METRICS)


async def _rows(pending: Any) -> list[dict[str, Any]]:
    """None (D1 lỗi) thành [] - cached_rows sẽ trả số gần nhất đã có thay vì rỗng."""
    return (await pending) or []


async def _computed_days() -> set[str]:
    return {row["day"] for row in await cached_rows("post_rollup_days", _days)}


async def _days() -> list[dict[str, Any]]:
    return [{"day": day} for day in (await repo.load_computed_post_days() or [])]


def previous_range(from_day: str, to_day: str) -> tuple[str, str]:
    """Khoảng liền trước, cùng số ngày."""
    start, end = date.fromisoformat(from_day), date.fromisoformat(to_day)
    length = (end - start).days + 1
    return (start - timedelta(days=length)).isoformat(), (start - timedelta(days=1)).isoformat()


async def movie_totals(movie_id: str, from_day: str, to_day: str, *, with_previous: bool = True) -> dict[str, Any]:
    """Tổng bài + bình luận của phim từ `from_day` tới `to_day` (ngày VN, tính cả hai đầu), kèm khoảng liền trước."""
    post_series, comment_series = await _series(movie_id)
    computed = await _computed_days()
    today = day_label(int(time.time() * 1000))

    def block(start: str, end: str) -> dict[str, Any]:
        last = min(end, today)
        wanted = {(date.fromisoformat(start) + timedelta(days=i)).isoformat()
                  for i in range((date.fromisoformat(last) - date.fromisoformat(start)).days + 1)} if start <= last else set()
        return {
            "from": start,
            "to": end,
            "complete": wanted <= computed,
            "posts": post_series.totals(start, end),
            "comments": comment_series.totals(start, end),
        }

    result: dict[str, Any] = {"movie_id": movie_id, **block(from_day, to_day)}
    if with_previous:
        result["previous"] = block(*previous_range(from_day, to_day))
    return result
