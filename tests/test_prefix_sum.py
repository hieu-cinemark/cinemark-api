"""Prefix sum trên chuỗi theo ngày của phim (app/services/stats/rollup/prefix_sum.py, movie_series.py)."""

import asyncio

from app.services.stats.rollup import movie_series
from app.services.stats.rollup.prefix_sum import DailySeries

ROWS = [
    {"day": "2026-11-01", "posts": 3, "views": 10},
    {"day": "2026-11-01", "posts": 2, "views": 5},  # cùng ngày (nền tảng khác) - cộng lại
    {"day": "2026-11-03", "posts": 4, "views": None},  # thiếu ngày 02/11 = 0, None = 0
    {"day": "2026-11-05", "posts": 1, "views": 1},
]


def test_range_sum_counts_both_ends_and_fills_missing_days():
    series = DailySeries.from_rows(ROWS, ("posts", "views"))
    assert series.first_day == "2026-11-01" and series.days == 5
    assert series.range_sum("posts", "2026-11-01", "2026-11-05") == 10
    assert series.range_sum("posts", "2026-11-02", "2026-11-03") == 4
    assert series.range_sum("posts", "2026-11-03", "2026-11-03") == 4
    assert series.totals("2026-11-01", "2026-11-01") == {"posts": 5, "views": 15}


def test_range_outside_or_reversed_is_clamped_or_zero():
    series = DailySeries.from_rows(ROWS, ("posts",))
    assert series.range_sum("posts", "2026-10-01", "2026-11-01") == 5
    assert series.range_sum("posts", "2026-11-05", "2026-12-31") == 1
    assert series.range_sum("posts", "2026-12-01", "2026-12-31") == 0
    assert series.range_sum("posts", "2026-10-01", "2026-10-31") == 0
    assert series.range_sum("posts", "2026-11-05", "2026-11-01") == 0


def test_empty_series():
    series = DailySeries.from_rows([], ("posts",))
    assert series.days == 0 and series.totals("2026-11-01", "2026-11-30") == {"posts": 0}


def test_prefix_matches_brute_force_on_every_range():
    rows = [{"day": f"2026-11-{d:02d}", "posts": (d * 7) % 5} for d in range(1, 31) if d % 4]
    series = DailySeries.from_rows(rows, ("posts",))
    by_day = {r["day"]: r["posts"] for r in rows}
    days = [f"2026-11-{d:02d}" for d in range(1, 31)]
    for i, start in enumerate(days):
        for end in days[i:]:
            assert series.range_sum("posts", start, end) == sum(by_day.get(day, 0) for day in days if start <= day <= end)


def test_previous_range_has_same_length():
    assert movie_series.previous_range("2026-11-08", "2026-11-14") == ("2026-11-01", "2026-11-07")
    assert movie_series.previous_range("2026-11-01", "2026-11-01") == ("2026-10-31", "2026-10-31")


def test_movie_totals_reads_each_table_once_and_flags_incomplete_ranges(monkeypatch):
    calls: list[str] = []

    async def fake_cached(name, compute):
        calls.append(name)
        return await compute()

    async def posts(_movie):
        return [{"day": "2026-11-01", "posts": 2, "replies": 3, "shares": 0, "views": 9, "organic_posts": 1},
                {"day": "2026-11-08", "posts": 5, "replies": 1, "shares": 1, "views": 1, "organic_posts": 5}]

    async def comments(_movie):
        return [{"day": "2026-11-08", "comments": 4, "meaningful": 3, "positive": 2, "negative": 1, "neutral": 0, "watched": 1,
                 "anticipating": 0}]

    async def computed(_mode="live"):
        return [f"2026-11-{d:02d}" for d in range(8, 15)]  # khoảng 01-07/11 chưa có trong bảng tổng hợp

    monkeypatch.setattr(movie_series, "cached_rows", fake_cached)
    monkeypatch.setattr(movie_series.repo, "load_movie_post_days", posts)
    monkeypatch.setattr(movie_series.repo, "load_movie_comment_days", comments)
    monkeypatch.setattr(movie_series.repo, "load_computed_post_days", computed)
    monkeypatch.setattr(movie_series, "day_label", lambda _ms: "2026-11-30")

    result = asyncio.run(movie_series.movie_totals("m1", "2026-11-08", "2026-11-14"))
    assert result["complete"] is True
    assert result["posts"] == {"posts": 5, "replies": 1, "shares": 1, "views": 1, "organic_posts": 5}
    assert result["comments"]["positive"] == 2
    assert result["previous"]["from"] == "2026-11-01" and result["previous"]["complete"] is False
    assert result["previous"]["posts"]["posts"] == 2
    assert sorted(calls) == ["movie_comment_days:m1", "movie_post_days:m1", "post_rollup_days"]
