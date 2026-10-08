"""Phần chọn bài thuần của app/services/comment_planner.py: độ nóng, hạn quét lại, loại nguồn, lấy mẫu phân tầng."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

from app.services import comment_planner as cp

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)


def _post(i: int, *, movie: str = "m1", hours_ago: float = 2, likes: int = 0, replies: int = 10, **extra):
    return {
        "id": f"p{i}",
        "external_id": f"x{i}",
        "url": f"https://example.com/{i}",
        "platform": "tiktok",
        "movie_id": movie,
        "author": extra.pop("author", f"user{i}"),
        "movie_title": extra.pop("movie_title", "Trại Buôn Người"),
        "posted_at": (NOW - timedelta(hours=hours_ago)).isoformat(),
        "like_count": likes,
        "reply_count": replies,
        "shares": 0,
        **extra,
    }


def test_velocity_prefers_fast_growing_recent_posts():
    fresh = _post(1, hours_ago=2, likes=200, replies=0)  # 100 / giờ
    old = _post(2, hours_ago=48, likes=2000, replies=0)  # ~42 / giờ
    assert cp.velocity(fresh, NOW) > cp.velocity(old, NOW)


def test_refresh_due_by_age_gap_and_growth():
    crawled = (NOW - timedelta(hours=4)).isoformat()
    young_grown = _post(1, hours_ago=10, replies=40, comments_crawled_at=crawled, comments_crawled_replies=20)
    young_flat = _post(2, hours_ago=10, replies=22, comments_crawled_at=crawled, comments_crawled_replies=20)
    too_soon = _post(3, hours_ago=10, replies=80, comments_crawled_at=(NOW - timedelta(hours=1)).isoformat(), comments_crawled_replies=20)
    too_old = _post(4, hours_ago=100, replies=900, comments_crawled_at=(NOW - timedelta(hours=30)).isoformat(), comments_crawled_replies=20)
    assert cp.refresh_due(_post(5), NOW)  # chưa từng quét
    assert cp.refresh_due(young_grown, NOW)
    assert not cp.refresh_due(young_flat, NOW)  # tăng 2 < 5
    assert not cp.refresh_due(too_soon, NOW)  # bài < 24h phải cách 3 giờ
    assert not cp.refresh_due(too_old, NOW)  # quá 72h thì thôi theo dõi


def test_pick_hot_limits_per_movie_and_skips_posts_without_comments():
    rows = [_post(i, likes=100 * i) for i in range(1, 6)] + [_post(9, movie="m2", likes=50)] + [_post(10, movie="m2", replies=0, likes=999)]
    picks = cp.pick_hot(rows, per_movie=2, now=NOW)
    by_movie = {}
    for pick in picks:
        by_movie.setdefault(pick.movie_id, []).append(pick.post_id)
    assert by_movie["m1"] == ["p5", "p4"]
    assert by_movie["m2"] == ["p9"]  # p10 nhanh nhất nhưng không có comment nào để lấy
    assert all(pick.reason == "hot" and pick.max_pages == cp.HOT_FIRST_PAGES for pick in picks)


def test_pick_hot_refresh_uses_fewer_pages():
    row = _post(1, hours_ago=10, replies=60, comments_crawled_at=(NOW - timedelta(hours=5)).isoformat(), comments_crawled_replies=20)
    [pick] = cp.pick_hot([row], per_movie=5, now=NOW)
    assert pick.reason == "hot_refresh" and pick.max_pages == cp.HOT_REFRESH_PAGES


def test_source_type():
    reputable = {("tiktok", "tintucphim")}
    assert cp.source_type(_post(1, author="traibuonnguoi.movie"), reputable) == cp.SOURCE_OFFICIAL
    assert cp.source_type(_post(2, author="tintucphim"), reputable) == cp.SOURCE_MEDIA
    assert cp.source_type(_post(3, author="an.nguyen"), reputable) == cp.SOURCE_PERSONAL


def test_pick_sample_spreads_over_slots_and_sources():
    rows = []
    # 4 khung 6 giờ x 2 loại nguồn, mỗi ô 5 bài
    for slot in range(4):
        for k in range(5):
            rows.append(_post(slot * 100 + k, hours_ago=slot * 6 + 1, author=f"fan{slot}{k}"))
            rows.append(_post(slot * 100 + 50 + k, hours_ago=slot * 6 + 1, author="tintucphim"))
    rows.append(_post(999, hours_ago=1, comments_crawled_at=NOW.isoformat()))  # đã quét: không lấy mẫu lại
    picks = cp.pick_sample(rows, now=NOW, reputable={("tiktok", "tintucphim")}, rng=random.Random(1))
    assert len(picks) == 4 * 2 * cp.SAMPLE_PER_CELL
    assert "p999" not in {pick.post_id for pick in picks}
    assert all(pick.reason == "sample" and pick.max_pages == cp.SAMPLE_PAGES for pick in picks)


def test_young_post_refreshes_every_six_hours_even_without_reply_count_update():
    # reply_count không đổi (lượt crawl bài chưa tìm lại bài này) nhưng bài < 24h, đã 6 giờ từ lần quét trước
    stale = _post(1, hours_ago=12, replies=20, comments_crawled_at=(NOW - timedelta(hours=6)).isoformat(), comments_crawled_replies=20)
    recent = _post(2, hours_ago=12, replies=20, comments_crawled_at=(NOW - timedelta(hours=4)).isoformat(), comments_crawled_replies=20)
    older = _post(3, hours_ago=40, replies=20, comments_crawled_at=(NOW - timedelta(hours=13)).isoformat(), comments_crawled_replies=20)
    assert cp.refresh_due(stale, NOW)
    assert not cp.refresh_due(recent, NOW)  # mới 4 giờ, reply_count không đổi
    assert not cp.refresh_due(older, NOW)  # bài > 24h vẫn cần reply_count tăng
