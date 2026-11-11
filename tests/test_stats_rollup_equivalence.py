"""So sánh SQL cũ (viết tay) và SQL mới (dựng bằng SQLAlchemy Core) trên SQLite trong bộ nhớ:
cùng dữ liệu mẫu, cùng câu lệnh, kết quả phải giống hệt (cùng số dòng, cùng giá trị mỗi cột).

SQL cũ được chép nguyên văn vào đây (không lấy từ `git show` - sau khi commit thì HEAD đã là bản mới),
nên test chạy độc lập với lịch sử git."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from app.repositories.d1.sql import compile_sql
from app.services.stats import stats_summary

SCHEMA = """
CREATE TABLE posts (
    id TEXT PRIMARY KEY, movie_id TEXT, keyword_id TEXT, platform TEXT,
    scraped_at TEXT, first_seen_at TEXT
);
CREATE TABLE comments (
    id TEXT PRIMARY KEY, post_id TEXT, platform TEXT, scraped_at TEXT, first_seen_at TEXT
);
CREATE TABLE stats_platform_daily (
    day TEXT NOT NULL, platform TEXT NOT NULL, posts INTEGER NOT NULL DEFAULT 0,
    comments INTEGER NOT NULL DEFAULT 0, last_scraped_at TEXT,
    PRIMARY KEY (day, platform)
);
CREATE TABLE stats_keyword_daily (
    day TEXT NOT NULL, keyword_id TEXT NOT NULL, platform TEXT NOT NULL,
    posts INTEGER NOT NULL DEFAULT 0, comments INTEGER NOT NULL DEFAULT 0, last_scraped_at TEXT,
    PRIMARY KEY (day, keyword_id)
);
"""

ROWS = [
    # (id, movie_id, keyword_id, platform, scraped_at, first_seen_at)
    ("p1", "m1", "kw1", "tiktok", "2026-10-20T10:00:00+00:00", "2026-10-20T09:00:00+00:00"),
    ("p2", "m1", "kw1", "tiktok", "2026-10-20T11:00:00+00:00", "2026-10-20T09:30:00+00:00"),
    ("p3", "m1", "kw2", "facebook", "2026-10-21T08:00:00+00:00", "2026-10-21T07:00:00+00:00"),
    # Dòng cũ không có first_seen_at -> ngày rơi về scraped_at.
    ("p4", "m1", None, "facebook", "2026-10-22T08:00:00+00:00", None),
    # scraped_at quá ngắn -> bị loại.
    ("p5", "m1", "kw1", "threads", "2026", None),
]

COMMENT_ROWS = [
    # (id, post_id, platform, scraped_at, first_seen_at)
    ("c1", "p1", "tiktok", "2026-10-20T12:00:00+00:00", "2026-10-20T11:30:00+00:00"),
    ("c2", "p1", "tiktok", "2026-10-20T13:00:00+00:00", "2026-10-20T11:45:00+00:00"),
    ("c3", "p3", "facebook", "2026-10-21T09:00:00+00:00", "2026-10-21T08:30:00+00:00"),
    # Comment của bài không có keyword_id -> không vào bảng theo từ khoá.
    ("c4", "p4", "facebook", "2026-10-22T09:00:00+00:00", "2026-10-22T08:30:00+00:00"),
]

# --- SQL cũ (viết tay, chép nguyên văn) --------------------------------------------------------
OLD_REBUILD = [
    "DELETE FROM stats_platform_daily",
    "DELETE FROM stats_keyword_daily",
    """
    INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
    SELECT substr(COALESCE(first_seen_at, scraped_at), 1, 10), platform, COUNT(*), 0, MAX(scraped_at)
    FROM posts
    WHERE scraped_at IS NOT NULL AND length(scraped_at) >= 10
    GROUP BY substr(COALESCE(first_seen_at, scraped_at), 1, 10), platform
    ON CONFLICT(day, platform) DO UPDATE SET
        posts = excluded.posts,
        last_scraped_at = excluded.last_scraped_at
    """,
    """
    INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
    SELECT substr(COALESCE(first_seen_at, scraped_at), 1, 10), platform, 0, COUNT(*), MAX(scraped_at)
    FROM comments
    WHERE scraped_at IS NOT NULL AND length(scraped_at) >= 10
    GROUP BY substr(COALESCE(first_seen_at, scraped_at), 1, 10), platform
    ON CONFLICT(day, platform) DO UPDATE SET
        comments = stats_platform_daily.comments + excluded.comments,
        last_scraped_at = CASE
            WHEN excluded.last_scraped_at > COALESCE(stats_platform_daily.last_scraped_at, '')
            THEN excluded.last_scraped_at
            ELSE stats_platform_daily.last_scraped_at
        END
    """,
    """
    INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
    SELECT substr(COALESCE(first_seen_at, scraped_at), 1, 10), keyword_id, platform, COUNT(*), 0, MAX(scraped_at)
    FROM posts
    WHERE keyword_id IS NOT NULL AND scraped_at IS NOT NULL AND length(scraped_at) >= 10
    GROUP BY substr(COALESCE(first_seen_at, scraped_at), 1, 10), keyword_id, platform
    ON CONFLICT(day, keyword_id) DO UPDATE SET
        posts = excluded.posts,
        last_scraped_at = excluded.last_scraped_at
    """,
    """
    INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
    SELECT substr(COALESCE(c.first_seen_at, c.scraped_at), 1, 10), p.keyword_id, c.platform, 0, COUNT(*), MAX(c.scraped_at)
    FROM comments c
    INNER JOIN posts p ON p.id = c.post_id
    WHERE p.keyword_id IS NOT NULL AND c.scraped_at IS NOT NULL AND length(c.scraped_at) >= 10
    GROUP BY substr(COALESCE(c.first_seen_at, c.scraped_at), 1, 10), p.keyword_id, c.platform
    ON CONFLICT(day, keyword_id) DO UPDATE SET
        comments = stats_keyword_daily.comments + excluded.comments,
        last_scraped_at = CASE
            WHEN excluded.last_scraped_at > COALESCE(stats_keyword_daily.last_scraped_at, '')
            THEN excluded.last_scraped_at
            ELSE stats_keyword_daily.last_scraped_at
        END
    """,
    "SELECT COUNT(*) AS n FROM stats_platform_daily",
    "SELECT COUNT(*) AS n FROM stats_keyword_daily",
]

OLD_RECONCILE = [
    """
    INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
    SELECT substr(first_seen_at, 1, 10), platform, COUNT(*), 0, MAX(scraped_at)
    FROM posts WHERE scraped_at >= ?1 AND first_seen_at >= ?1
    GROUP BY substr(first_seen_at, 1, 10), platform
    ON CONFLICT(day, platform) DO UPDATE SET
        posts = excluded.posts,
        last_scraped_at = MAX(COALESCE(stats_platform_daily.last_scraped_at, ''), excluded.last_scraped_at)
    """,
    """
    INSERT INTO stats_platform_daily (day, platform, posts, comments, last_scraped_at)
    SELECT substr(first_seen_at, 1, 10), platform, 0, COUNT(*), MAX(scraped_at)
    FROM comments WHERE scraped_at >= ?1 AND first_seen_at >= ?1
    GROUP BY substr(first_seen_at, 1, 10), platform
    ON CONFLICT(day, platform) DO UPDATE SET
        comments = excluded.comments,
        last_scraped_at = MAX(COALESCE(stats_platform_daily.last_scraped_at, ''), excluded.last_scraped_at)
    """,
    """
    INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
    SELECT substr(first_seen_at, 1, 10), keyword_id, platform, COUNT(*), 0, MAX(scraped_at)
    FROM posts WHERE keyword_id IS NOT NULL AND scraped_at >= ?1 AND first_seen_at >= ?1
    GROUP BY substr(first_seen_at, 1, 10), keyword_id, platform
    ON CONFLICT(day, keyword_id) DO UPDATE SET
        posts = excluded.posts,
        last_scraped_at = MAX(COALESCE(stats_keyword_daily.last_scraped_at, ''), excluded.last_scraped_at)
    """,
    """
    INSERT INTO stats_keyword_daily (day, keyword_id, platform, posts, comments, last_scraped_at)
    SELECT substr(c.first_seen_at, 1, 10), p.keyword_id, c.platform, 0, COUNT(*), MAX(c.scraped_at)
    FROM comments c INNER JOIN posts p ON p.id = c.post_id
    WHERE p.keyword_id IS NOT NULL AND c.scraped_at >= ?1 AND c.first_seen_at >= ?1
    GROUP BY substr(c.first_seen_at, 1, 10), p.keyword_id, c.platform
    ON CONFLICT(day, keyword_id) DO UPDATE SET
        comments = excluded.comments,
        last_scraped_at = MAX(COALESCE(stats_keyword_daily.last_scraped_at, ''), excluded.last_scraped_at)
    """,
]

OLD_SUBTRACT_PLATFORM = "UPDATE stats_platform_daily SET {kind} = MAX(0, {kind} - ?) WHERE day = ? AND platform = ?"
OLD_SUBTRACT_KEYWORD = "UPDATE stats_keyword_daily SET {kind} = MAX(0, {kind} - ?) WHERE day = ? AND keyword_id = ?"


def _seed(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.executemany("INSERT INTO posts VALUES (?,?,?,?,?,?)", ROWS)
    db.executemany("INSERT INTO comments VALUES (?,?,?,?,?)", COMMENT_ROWS)


def _new_statements() -> list[tuple[str, list]]:
    out: list[tuple[str, list]] = []
    for builder in (
        stats_summary._platform_posts_insert,
        stats_summary._platform_comments_insert,
        stats_summary._keyword_posts_insert,
        stats_summary._keyword_comments_insert,
    ):
        out.append(compile_sql(builder()))
    return out


def _snapshot(db: sqlite3.Connection) -> dict:
    return {
        "platform": db.execute("SELECT * FROM stats_platform_daily ORDER BY day, platform").fetchall(),
        "keyword": db.execute("SELECT * FROM stats_keyword_daily ORDER BY day, keyword_id").fetchall(),
    }


def test_rebuild_old_and_new_queries_produce_identical_tables():
    db_old = sqlite3.connect(":memory:")
    _seed(db_old)
    for sql in OLD_REBUILD:
        db_old.execute(sql)
    db_old.commit()

    db_new = sqlite3.connect(":memory:")
    _seed(db_new)
    for sql, params in _new_statements():
        db_new.execute(sql, params)
    db_new.commit()

    assert _snapshot(db_old) == _snapshot(db_new)
    # Bảng không rỗng - nếu rỗng thì so sánh vô nghĩa.
    assert _snapshot(db_new)["platform"]
    assert _snapshot(db_new)["keyword"]


@pytest.mark.parametrize("since", ["2026-10-01", "2026-10-20"])
def test_reconcile_old_and_new_queries_produce_identical_tables(since: str):
    db_old = sqlite3.connect(":memory:")
    _seed(db_old)
    for sql in OLD_RECONCILE:
        db_old.execute(sql, {"1": since})
    db_old.commit()

    db_new = sqlite3.connect(":memory:")
    _seed(db_new)
    for stmt, _label in stats_summary._reconcile_statements(since):
        sql, params = compile_sql(stmt)
        db_new.execute(sql, params)
    db_new.commit()

    assert _snapshot(db_old) == _snapshot(db_new)


def test_reconcile_upserts_overwrite_without_adding_or_deleting():
    """Bản mới: mỗi câu ghi đè, không cộng dồn và không DELETE - số trong bảng luôn bằng số đếm từ bảng gốc."""
    for stmt, _label in stats_summary._reconcile_statements("2026-10-01"):
        sql, _params = compile_sql(stmt)
        assert "DO UPDATE SET" in sql
        assert "excluded.posts +" not in sql
        assert "excluded.comments +" not in sql
        assert "DELETE" not in sql.upper()


def test_subtract_deleted_matches_the_raw_update():
    deleted_posts = [
        {"id": "p1", "platform": "tiktok", "keyword_id": "kw1", "first_seen_at": "2026-10-20T09:00:00+00:00"}
    ]
    deleted_comments = [
        {"id": "c1", "post_id": "p1", "platform": "tiktok", "first_seen_at": "2026-10-20T11:30:00+00:00"}
    ]

    db = sqlite3.connect(":memory:")
    _seed(db)
    db.execute("INSERT INTO stats_platform_daily VALUES ('2026-10-20', 'tiktok', 5, 3, '2026-10-20T13:00:00+00:00')")
    db.execute("INSERT INTO stats_keyword_daily VALUES ('2026-10-20', 'kw1', 'tiktok', 5, 3, 'x')")
    db.commit()

    captured: list[tuple[str, list]] = []

    async def fake_q(sql, params=None, **_kwargs):
        captured.append((str(sql), list(params or [])))
        db.execute(sql, params or [])
        return []

    original = stats_summary._q
    stats_summary._q = fake_q
    try:
        asyncio.run(stats_summary.subtract_deleted(deleted_posts, deleted_comments))
    finally:
        stats_summary._q = original
    db.commit()

    # Bản cũ: 4 câu UPDATE (2 bảng x 2 kind) với cùng tham số.
    old_plan = [
        (OLD_SUBTRACT_PLATFORM.replace("{kind}", "posts"), [1, "2026-10-20", "tiktok"]),
        (OLD_SUBTRACT_PLATFORM.replace("{kind}", "comments"), [1, "2026-10-20", "tiktok"]),
        (OLD_SUBTRACT_KEYWORD.replace("{kind}", "posts"), [1, "2026-10-20", "kw1"]),
        (OLD_SUBTRACT_KEYWORD.replace("{kind}", "comments"), [1, "2026-10-20", "kw1"]),
    ]
    db_old = sqlite3.connect(":memory:")
    _seed(db_old)
    db_old.execute(
        "INSERT INTO stats_platform_daily VALUES ('2026-10-20', 'tiktok', 5, 3, '2026-10-20T13:00:00+00:00')"
    )
    db_old.execute("INSERT INTO stats_keyword_daily VALUES ('2026-10-20', 'kw1', 'tiktok', 5, 3, 'x')")
    db_old.commit()
    for sql, params in old_plan:
        db_old.execute(sql.replace("MAX(", "max("), params)
    db_old.commit()

    # Cùng số câu, cùng tham số, cùng kết quả.
    assert len(captured) == len(old_plan)
    assert [params for _sql, params in captured] == [params for _sql, params in old_plan]
    assert all("max(" in sql.lower() for sql, _ in captured)
    assert _snapshot(db) == _snapshot(db_old)
    # Không xuống dưới 0.
    assert db.execute("SELECT posts, comments FROM stats_platform_daily").fetchall() == [(4, 2)]
    assert db.execute("SELECT posts, comments FROM stats_keyword_daily").fetchall() == [(4, 2)]
