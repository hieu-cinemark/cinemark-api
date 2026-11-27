"""Bảng tổng hợp bài theo ngày (chuyển từ Worker 27/11/2026). Các ca của phần tính thuần chép NGUYÊN từ
cinemark tests/post-rollup.test.mjs (cùng dữ liệu, cùng kết quả) - để bản Python và bản TS ra cùng số khi chạy song song."""

import asyncio
import sqlite3
from datetime import UTC, datetime

import pytest

from app.repositories.d1 import post_rollup as repo
from app.services.stats.rollup import post_rollup as service
from app.services.stats.rollup.helper import DAY_MS, day_label, day_start, parse_ms, utc_iso
from app.services.stats.rollup.post_rollup_core import (
    BACKFILL_DAYS,
    CURRENT_AGE,
    build_rollup_rows,
    next_rollup_day,
    rollup_handle,
)

DAY = "2026-10-10"
NOW = parse_ms("2026-10-13T12:00:00+07:00")


def post(pid, **extra):
    base = {"id": pid, "movie_id": "m", "platform": "tiktok", "scraped_at": "2026-10-10T05:00:00.000Z", "replies": 10,
            "shares": 2, "views": 100, "organic": True}
    return {**base, **extra}


def snap(post_id, recorded_at, replies):
    return {"post_id": post_id, "recorded_at": recorded_at, "replies": replies, "shares": 1, "views": 50}


def row(rows, age, organic=1):
    return next((r for r in rows if r.seen_age == age and r.organic == organic), None)


def test_current_row_counts_each_post_plus_its_replies():
    rows = build_rollup_rows(DAY, [post("a"), post("b", replies=None)], [], NOW)
    r = row(rows, CURRENT_AGE)
    assert (r.movie_id, r.platform, r.organic, r.seen_age, r.posts, r.discussions, r.shares, r.views) == (
        "m", "tiktok", 1, -1, 2, 12, 4, 200)


def test_seen_age_takes_latest_snapshot_before_cutoff_and_skips_post_not_found_yet():
    # Hết ngày đăng (tuổi 0) là 17:00Z 10/10, tuổi 1 là 17:00Z 11/10.
    snapshots = [snap("a", "2026-10-11T10:00:00.000Z", 3), snap("a", "2026-10-12T10:00:00.000Z", 8)]
    rows = build_rollup_rows(DAY, [post("a")], snapshots, NOW)
    assert row(rows, 0) is None
    assert row(rows, 1).discussions == 4
    assert row(rows, 2).discussions == 9
    assert row(rows, CURRENT_AGE).discussions == 11


def test_post_without_snapshots_uses_current_numbers_once_scraped_and_future_ages_absent():
    rows = build_rollup_rows(DAY, [post("a", scraped_at="2026-10-11T20:00:00.000Z")], [], NOW)
    assert row(rows, 0) is None
    assert row(rows, 1) is None
    assert row(rows, 2).discussions == 11
    assert row(rows, 4) is None


def test_organic_and_other_posts_stay_in_separate_rows():
    rows = build_rollup_rows(DAY, [post("a"), post("b", organic=False)], [], NOW)
    assert row(rows, CURRENT_AGE, 1).posts == 1
    assert row(rows, CURRENT_AGE, 0).posts == 1


def test_next_day_newest_missing_first_then_most_overdue_nothing_when_fresh():
    now = parse_ms("2026-10-13T12:00:00+07:00")
    base = parse_ms("2026-10-13T00:00:00Z")
    fresh = {datetime.fromtimestamp((base - i * DAY_MS) / 1000, UTC).date().isoformat(): now for i in range(BACKFILL_DAYS)}
    assert next_rollup_day(fresh, now) is None
    stale = dict(fresh)
    stale["2026-10-13"] = now - 31 * 60_000
    assert next_rollup_day(stale, now) == ("2026-10-13", False)
    del stale["2026-10-01"]
    assert next_rollup_day(stale, now) == ("2026-10-01", True)


def test_time_helpers_match_javascript():
    assert utc_iso(day_start("2026-11-27")) == "2026-11-26T17:00:00.000Z"
    assert day_label(day_start("2026-11-27") + DAY_MS - 1) == "2026-11-27"
    assert parse_ms("2026-10-10T05:00:00+00:00") == parse_ms("2026-10-10T05:00:00.000Z") == parse_ms("2026-10-10T05:00:00")
    assert parse_ms(None) is None and parse_ms("hỏng") is None


def test_handle_strips_youtube_at_like_the_worker():
    assert rollup_handle("youtube", "@KenhPhim", "") == "kenhphim"
    assert rollup_handle("youtube", "", "https://www.youtube.com/@KenhPhim/videos") == "kenhphim"
    assert rollup_handle("tiktok", "x", "https://www.tiktok.com/@abc/video/1") == "abc"


# --- đọc/ghi D1 (SQLite thay D1) ---


@pytest.fixture
def db(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE posts (id TEXT PRIMARY KEY, movie_id TEXT, platform TEXT, author TEXT, url TEXT, posted_at TEXT,
                            scraped_at TEXT, reply_count INTEGER, repost_count INTEGER, reshare_count INTEGER,
                            quote_count INTEGER, view_count INTEGER, relevance_label TEXT, keyword_match INTEGER);
        CREATE TABLE post_engagement_snapshots (post_id TEXT, recorded_at TEXT, reply_count INTEGER, repost_count INTEGER,
                            reshare_count INTEGER, quote_count INTEGER, view_count INTEGER);
        CREATE TABLE author_classifications (handle TEXT, tier TEXT);
        CREATE TABLE movie_post_daily (day TEXT NOT NULL, seen_age INTEGER NOT NULL, movie_id TEXT NOT NULL,
                            platform TEXT NOT NULL, organic INTEGER NOT NULL, posts INTEGER NOT NULL, discussions INTEGER NOT NULL,
                            shares INTEGER NOT NULL, views INTEGER NOT NULL, PRIMARY KEY (day, seen_age, movie_id, platform, organic));
        CREATE TABLE movie_post_rollup_days (day TEXT PRIMARY KEY, computed_at INTEGER);
        -- Bài trong ngày 27/11 giờ VN: 2 liên quan, 1 không liên quan, 1 nhãn trống không khớp từ khoá; p5 sang 28/11. Tránh đúng giây
        -- 00:00:00 VN: bản Worker so chuỗi '...000Z' với '...+00:00' nên bài đúng giây đó rơi về ngày trước - giữ y hệt để đối chiếu.
        INSERT INTO posts VALUES
          ('p1','m1','tiktok','a','https://www.tiktok.com/@fanacc/video/1','2026-11-26T17:00:01+00:00','2026-11-26T18:00:00+00:00',5,1,0,0,100,'related',1),
          ('p2','m1','tiktok','b','https://www.tiktok.com/@brandacc/video/2','2026-11-27T03:00:00+00:00','2026-11-27T04:00:00+00:00',2,NULL,NULL,NULL,50,NULL,1),
          ('p3','m1','tiktok','c','https://www.tiktok.com/@x/video/3','2026-11-27T05:00:00+00:00','2026-11-27T05:00:00+00:00',9,0,0,0,9,'not_related',1),
          ('p4','m1','tiktok','d','https://www.tiktok.com/@y/video/4','2026-11-27T06:00:00+00:00','2026-11-27T06:00:00+00:00',9,0,0,0,9,NULL,0),
          ('p5','m1','tiktok','e','https://www.tiktok.com/@z/video/5','2026-11-27T17:00:01+00:00','2026-11-27T17:00:00+00:00',1,0,0,0,1,'related',1);
        INSERT INTO author_classifications VALUES ('brandacc', 'cinema'), ('fanacc', 'fan');
        -- Dòng cũ của Worker (không có run) phải bị thay hết.
        INSERT INTO movie_post_daily VALUES ('2026-11-27', -1, 'old', 'tiktok', 1, 99, 99, 99, 99);
        """
    )

    async def fake_query(sql, params=None, **_kwargs):
        try:
            cur = conn.execute(sql, params or [])
        except sqlite3.OperationalError as exc:
            if "duplicate column" in str(exc):
                return None
            raise
        conn.commit()
        return [dict(r) for r in cur.fetchall()]

    monkeypatch.setattr(repo, "d1_query", fake_query)
    monkeypatch.setattr(repo, "_ready", set())
    return conn


def _daily(conn, table, day="2026-11-27"):
    return conn.execute(
        f"SELECT movie_id, organic, posts, discussions, shares, views FROM {table} WHERE day = ? AND seen_age = -1 ORDER BY organic",
        [day],
    ).fetchall()


def test_live_write_replaces_worker_rows_and_splits_organic(db):
    now = parse_ms("2026-11-28T12:00:00+07:00")
    assert asyncio.run(service.compute_and_write("live", "2026-11-27", now)) is not None
    rows = [tuple(r) for r in _daily(db, "movie_post_daily")]
    # p1 (fan -> tự nhiên) và p2 (rạp -> không tự nhiên) thuộc ngày; p3/p4 không liên quan; p5 sang ngày 28/11 giờ VN.
    assert rows == [("m1", 0, 1, 3, 0, 50), ("m1", 1, 1, 6, 1, 100)]
    assert db.execute("SELECT computed_at FROM movie_post_rollup_days WHERE day = '2026-11-27'").fetchone()[0] == now
    assert db.execute("SELECT COUNT(*) FROM movie_post_daily WHERE run IS NULL").fetchone()[0] == 0


def test_shadow_mode_never_touches_the_worker_tables(db):
    now = parse_ms("2026-11-28T12:00:00+07:00")
    asyncio.run(repo.ensure_schema("shadow"))
    assert asyncio.run(service.compute_and_write("shadow", "2026-11-27", now)) is not None
    assert [tuple(r) for r in _daily(db, "movie_post_daily_shadow")] == [("m1", 0, 1, 3, 0, 50), ("m1", 1, 1, 6, 1, 100)]
    assert [tuple(r) for r in _daily(db, "movie_post_daily")] == [("old", 1, 99, 99, 99, 99)]
    assert db.execute("SELECT COUNT(*) FROM movie_post_rollup_days").fetchone()[0] == 0


def test_process_builds_missing_days_newest_first(db, monkeypatch):
    monkeypatch.setattr(service, "PAUSE_SECONDS", 0)
    result = asyncio.run(service.process("shadow", max_days=2))
    assert result == {"done": 2, "failed": 0}
    days = [r[0] for r in db.execute("SELECT day FROM movie_post_rollup_days_shadow ORDER BY day DESC")]
    assert len(days) == 2 and days[0] > days[1]


def test_process_off_mode_does_nothing(db):
    assert asyncio.run(service.process("off")) == {"skipped": 1}
