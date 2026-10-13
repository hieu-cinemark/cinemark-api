from app.services.omar_trending import mark_hot, velocity


def _snap(at: str, views: int, comments: int = 0, likes: int = 0, shares: int = 0) -> dict:
    return {"at": at, "views": views, "likes": likes, "comments": comments, "shares": shares}


def test_velocity_uses_latest_and_previous_at_least_one_hour_apart() -> None:
    snaps = [
        _snap("2026-10-13T06:30:00+00:00", 1900, comments=95),  # mới nhất
        _snap("2026-10-13T00:00:00+00:00", 100, comments=10),
        _snap("2026-10-13T04:00:00+00:00", 1000, comments=50),  # cách 2,5 giờ -> cặp so sánh
        _snap("2026-10-13T06:00:00+00:00", 1800, comments=90),  # chỉ cách 30 phút -> bỏ qua
    ]
    vel = velocity(snaps)
    assert vel is not None
    assert vel["span_hours"] == 2.5
    assert vel["views_per_hour"] == 360.0
    assert vel["comments_per_hour"] == 18.0
    assert vel["growth_pct_per_hour"] == 36.0
    assert vel["since"] == "2026-10-13T04:00:00+00:00"


def test_velocity_none_without_pair_and_clamps_decreases() -> None:
    assert velocity([]) is None
    assert velocity([_snap("2026-10-13T06:00:00+00:00", 10), _snap("2026-10-13T06:30:00+00:00", 20)]) is None
    vel = velocity([_snap("2026-10-13T00:00:00Z", 0, comments=5), _snap("2026-10-13T02:00:00Z", 0, comments=3)])
    assert vel is not None
    assert vel["views_per_hour"] == 0 and vel["comments_per_hour"] == 0
    assert vel["growth_pct_per_hour"] is None  # không có gốc để tính %


def test_mark_hot_top_ten_percent_or_fast_growth() -> None:
    items = [
        {"video_id": str(i), "velocity": {"views_per_hour": float(i * 10), "growth_pct_per_hour": 5.0}}
        for i in range(20)
    ]
    items[0]["velocity"]["growth_pct_per_hour"] = 80.0  # tốc độ 0 nhưng tăng > 50%/giờ
    mark_hot(items)
    hot = {item["video_id"] for item in items if item["hot"]}
    assert hot == {"19", "18", "0"}


def test_mark_hot_ignores_zero_velocity() -> None:
    items = [{"velocity": {"views_per_hour": 0.0, "growth_pct_per_hour": None}} for _ in range(3)]
    mark_hot(items)
    assert not any(item["hot"] for item in items)
    mark_hot([])
