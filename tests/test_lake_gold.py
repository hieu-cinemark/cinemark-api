"""app/lake/gold.py chạy trên một cây silver local nhỏ xíu dựng thẳng bằng DuckDB - không bronze,
không R2."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from app.lake import gold

# 2026-10-01 00:00 UTC = 07:00 giờ VN
DAY1 = "2026-10-01"


def _ts(value: str) -> str:
    return f"TIMESTAMPTZ '{value}+00'"


def _copy(con: duckdb.DuckDBPyConnection, select: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({select}) TO '{dest}' (FORMAT parquet)")


@pytest.fixture
def silver(tmp_path: Path) -> Path:
    root = tmp_path / "silver"
    con = duckdb.connect()
    _copy(
        con,
        """SELECT * FROM (VALUES ('kw_a', 'movie_a'), ('kw_b', 'movie_b'))
           t(keyword_id, movie_id)""",
        root / "dim_keywords" / "dim_keywords.parquet",
    )
    # p1: được giữ. p2: bị loại. p3: đăng lúc 23:30 UTC ngày 1 = 06:30 ngày 2 giờ VN.
    # p4: lưu nhưng ẩn (không có tín hiệu phim) - không được tính.
    _copy(
        con,
        f"""SELECT * FROM (VALUES
              ('p1', 'kw_a', {_ts("2026-10-01 01:00:00")}, 'kept', 'kira_related'),
              ('p2', 'kw_a', {_ts("2026-10-01 02:00:00")}, 'dropped', 'kira_irrelevant'),
              ('p3', 'kw_b', {_ts("2026-10-01 23:30:00")}, NULL, NULL),
              ('p4', 'kw_a', {_ts("2026-10-01 03:00:00")}, 'kept', 'no_film_context')
           ) t(post_id, keyword_id, posted_at, decision, reason)""",
        root / "posts" / "platform=facebook" / "data.parquet",
    )
    # p1 crawl 3 lần qua 2 ngày VN; lần cuối ngày 2 báo số thấp hơn ngày 1 -> delta 0.
    # p3 crawl 2 ngày, tăng 5. p2 bị loại nên không được tính dù có snapshot.
    _copy(
        con,
        f"""SELECT * FROM (VALUES
              ('p1', 10, 2, 1, NULL::BIGINT, {_ts("2026-10-01 03:00:00")}),
              ('p1', 20, 4, 1, NULL::BIGINT, {_ts("2026-10-01 10:00:00")}),
              ('p1', 18, 4, 1, NULL::BIGINT, {_ts("2026-10-02 05:00:00")}),
              ('p2', 99, 0, 0, NULL::BIGINT, {_ts("2026-10-01 05:00:00")}),
              ('p3',  5, 0, 0, NULL::BIGINT, {_ts("2026-10-02 00:00:00")}),
              ('p3', 10, 0, 0, NULL::BIGINT, {_ts("2026-10-03 00:00:00")})
           ) t(post_id, likes, comments, shares, views, scraped_at)""",
        root / "posts_snapshots" / "platform=facebook" / f"dt={DAY1}" / "data.parquet",
    )
    # c1 trên p1 (được giữ), c2 trên p2 (bị loại), c3 trên bài không có trong silver.
    _copy(
        con,
        f"""SELECT * FROM (VALUES
              ('c1', 'p1', {_ts("2026-10-01 04:00:00")}),
              ('c2', 'p2', {_ts("2026-10-01 04:00:00")}),
              ('c3', 'p_old', {_ts("2026-10-01 04:00:00")})
           ) t(comment_id, post_id, posted_at)""",
        root / "comments" / "platform=facebook" / "data.parquet",
    )
    return root


def _row(con: duckdb.DuckDBPyConnection, movie_id: str, day: str) -> dict:
    cur = con.execute(
        "SELECT * EXCLUDE (movie_id, platform, day) FROM movie_daily WHERE movie_id = ? AND day = ?",
        [movie_id, date.fromisoformat(day)],
    )
    return dict(zip([c[0] for c in cur.description], cur.fetchone(), strict=True))


def test_build_movie_daily_end_to_end(silver: Path, tmp_path: Path) -> None:
    con = duckdb.connect()
    written = gold.build_movie_daily(con, silver=str(silver), out=str(tmp_path / "gold"))

    # movie_a: ngày 1, 2. movie_b: ngày 2 (đăng + thấy lần đầu), 3.
    assert written == 4

    # Ngày đầu thấy p1: chưa có mốc -> first_seen = bản chụp cuối ngày (20+4+1), gained = 0.
    # p2 bị loại nên cả bài lẫn comment c2 không được tính.
    assert _row(con, "movie_a", "2026-10-01") == {
        "new_posts": 1,
        "posts_tracked": 1,
        "engagement_gained": 0,
        "engagement_first_seen": 25,
        "views_gained": 0,
        "new_comments": 1,
    }
    # Ngày 2: p1 báo 23 < 25 -> delta âm tính là 0, không âm.
    assert _row(con, "movie_a", "2026-10-02")["engagement_gained"] == 0

    # p3 đăng 23:30 UTC ngày 1 -> ngày 2 giờ VN.
    day2_b = _row(con, "movie_b", "2026-10-02")
    assert (day2_b["new_posts"], day2_b["engagement_first_seen"]) == (1, 5)
    assert _row(con, "movie_b", "2026-10-03")["engagement_gained"] == 5

    on_disk = con.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}/gold/movie_daily/*/*.parquet')").fetchone()[0]
    assert on_disk == 4


def test_check_rejects_duplicate_rows(silver: Path, tmp_path: Path, monkeypatch) -> None:
    real_check = gold._check_movie_daily

    def duplicate_then_check(con: duckdb.DuckDBPyConnection) -> None:
        con.execute("INSERT INTO movie_daily SELECT * FROM movie_daily LIMIT 1")
        real_check(con)

    monkeypatch.setattr(gold, "_check_movie_daily", duplicate_then_check)
    with pytest.raises(ValueError, match="duplicate"):
        gold.build_movie_daily(duckdb.connect(), silver=str(silver), out=str(tmp_path / "gold"))
    assert not (tmp_path / "gold").exists()  # không ghi gì khi kiểm tra thất bại
