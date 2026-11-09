import asyncio
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from app.repositories.d1 import comment_rollup as cr
from app.services.stats.comment_rollup import vn_days


@pytest.fixture
def db(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE posts (id TEXT PRIMARY KEY, movie_id TEXT, platform TEXT, relevance_label TEXT, keyword_match INTEGER);
        CREATE TABLE comments (id TEXT, post_id TEXT, posted_at TEXT, audience_stage TEXT, sentiment TEXT,
                               sentiment_source TEXT, aspects TEXT);
        INSERT INTO posts VALUES ('p1','m1','tiktok','related',1), ('p2','m1','facebook',NULL,1), ('p3','m2','tiktok','not_related',0);
        ALTER TABLE posts ADD COLUMN trend_post INTEGER NOT NULL DEFAULT 0;
        INSERT INTO comments VALUES
          ('c1','p1','2026-10-28T17:00:00+00:00','da_xem','positive','jev','["dien_xuat:+"]'),
          ('c2','p1','2026-10-29T16:59:59+00:00',NULL,'negative','jev','["dien_xuat:+","kich_ban:-"]'),
          ('c3','p2','2026-10-29T03:00:00+00:00','dang_hong',NULL,NULL,NULL),
          ('c4','p1','2026-10-29T05:00:00+00:00',NULL,'neutral','rule','["x:+"]'),
          ('c5','p3','2026-10-29T05:00:00+00:00',NULL,'positive','jev',NULL),
          ('c6','p1','2026-10-29T17:00:00+00:00',NULL,'positive','jev',NULL);
        ALTER TABLE posts ADD COLUMN posted_at TEXT;
        ALTER TABLE comments ADD COLUMN message TEXT NOT NULL DEFAULT 'chữ';
        ALTER TABLE comments ADD COLUMN parent_external_id TEXT;
        CREATE TABLE movies (id TEXT PRIMARY KEY, released_at TEXT);
        CREATE TABLE movie_measure_settings (movie_id TEXT PRIMARY KEY, premiere_date TEXT);
        INSERT INTO movies VALUES ('m1', '2026-10-30');
        INSERT INTO movie_measure_settings VALUES ('m1', '2026-10-29');
        UPDATE posts SET posted_at = '2026-10-28T20:00:00+00:00' WHERE id = 'p1';
        UPDATE posts SET posted_at = '2026-10-27T20:00:00+00:00' WHERE id = 'p2';
        """
    )

    async def fake_query(sql, params=None, **_kwargs):
        cur = conn.execute(sql, params or [])
        conn.commit()
        return cur.fetchall()

    monkeypatch.setattr(cr, "d1_query", fake_query)
    monkeypatch.setattr(cr, "_ready", False)
    return conn


def test_rebuild_day_counts_one_vn_day_of_relevant_comments(db):
    assert asyncio.run(cr.rebuild_day("2026-10-29"))
    rows = db.execute(
        "SELECT platform, stage, sentiment, meaningful, n FROM movie_comment_daily WHERE day='2026-10-29' ORDER BY 1, 2, 3"
    ).fetchall()
    # c1 (00:00 VN 29/10) thuộc ngày 29, c6 (00:00 VN 30/10) không; c5 thuộc bài không liên quan.
    assert rows == [
        ("facebook", "dang_hong", "", 1, 1),
        ("tiktok", "", "negative", 1, 1),
        ("tiktok", "", "neutral", 0, 1),
        ("tiktok", "da_xem", "positive", 1, 1),
    ]
    aspects = db.execute("SELECT aspect, n FROM movie_aspect_daily ORDER BY 1").fetchall()
    assert aspects == [("dien_xuat:+", 2), ("kich_ban:-", 1)]  # bình luận 'rule' không đếm khía cạnh


def test_rebuild_day_replaces_stale_rows(db):
    asyncio.run(cr.rebuild_day("2026-10-29"))
    db.execute("UPDATE comments SET sentiment = 'positive' WHERE id = 'c2'")
    asyncio.run(cr.rebuild_day("2026-10-29"))
    rows = db.execute("SELECT stage, sentiment, n FROM movie_comment_daily WHERE platform='tiktok' ORDER BY 1, 2").fetchall()
    assert rows == [("", "neutral", 1), ("", "positive", 1), ("da_xem", "positive", 1)]


def test_vn_days_uses_vietnam_calendar():
    late_utc = datetime(2026, 10, 29, 18, 0, tzinfo=ZoneInfo("UTC"))  # 01:00 VN ngày 30/10
    assert vn_days(2, late_utc) == ["2026-10-30", "2026-10-29"]


def test_rebuild_day_counts_comment_groups(db):
    db.executescript(
        """
        INSERT INTO comments (id, post_id, posted_at, sentiment, sentiment_source, aspects, message, parent_external_id) VALUES
          ('r1','p1','2026-10-29T05:00:00+00:00','positive','jev','["dien_vien:+"]','phản hồi','c1'),
          ('e1','p1','2026-10-29T05:00:00+00:00',NULL,NULL,NULL,'  ',NULL);
        """
    )
    assert asyncio.run(cr.rebuild_day("2026-10-29"))
    rows = db.execute(
        "SELECT platform, sentiment, after_premiere, grp, n FROM movie_comment_group_daily WHERE day='2026-10-29' ORDER BY 1, 2, 4"
    ).fetchall()
    # p1 đăng 03:00 VN 29/10 = từ ngày chiếu sớm; p2 đăng trước. Phản hồi (r1) và bình luận rỗng (e1) không đếm.
    assert rows == [
        ("facebook", "", 0, "none", 1),
        ("facebook", "", 0, "total", 1),
        ("tiktok", "negative", 1, "dien_xuat:+", 1),
        ("tiktok", "negative", 1, "kich_ban:-", 1),
        ("tiktok", "negative", 1, "total", 1),
        ("tiktok", "neutral", 1, "icon", 1),
        ("tiktok", "positive", 1, "dien_xuat:+", 1),
        ("tiktok", "positive", 1, "total", 1),
    ]
