"""Chọn phim cho job gom nhóm bình luận tự động (scripts/jobs/topic_clusters.py)."""

from datetime import UTC, datetime

from scripts.jobs.topic_clusters import pick_movies

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
MOVIES = [{"id": "a", "title": "A"}, {"id": "b", "title": "B"}, {"id": "c", "title": "C"}]


def test_picks_missing_and_stale_oldest_first():
    generated = {"a": "2026-11-17T10:00:00+00:00", "b": "2026-11-15T10:00:00+00:00"}
    picked = pick_movies(MOVIES, generated, now=NOW, min_age_hours=24, max_films=5)
    assert [m["id"] for m in picked] == ["c", "b"]


def test_respects_max_films_and_force():
    generated = {"a": "2026-11-17T11:59:00+00:00"}
    assert [m["id"] for m in pick_movies(MOVIES, generated, now=NOW, min_age_hours=24, max_films=1)] == ["b"]
    forced = pick_movies(MOVIES, generated, now=NOW, min_age_hours=0, max_films=5)
    assert {m["id"] for m in forced} == {"a", "b", "c"}
