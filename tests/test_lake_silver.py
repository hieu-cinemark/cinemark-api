"""app/lake/silver.py chạy trên một cây bronze local nhỏ xíu theo đúng định dạng phong bì
của lake writer - không R2, không mạng."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import duckdb
import pytest

from app.lake import silver


def _write_bronze(root: Path, entity: str, platform: str, dt: str, rows: list[dict]) -> None:
    folder = root / f"entity={entity}" / f"platform={platform}" / f"dt={dt}"
    folder.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {
                "topic": f"t_{entity}",
                "partition": 0,
                "offset": r["offset"],
                "kafka_ts": r["ts"] * 1000,
                "payload": r["payload"],
            }
        )
        for r in rows
    ]
    with gzip.open(folder / f"p0-{len(list(folder.iterdir())):012d}.ndjson.gz", "wt") as fh:
        fh.write("\n".join(lines) + "\n")


@pytest.fixture
def bronze(tmp_path: Path) -> Path:
    root = tmp_path / "bronze"
    fb = {
        "platform": "facebook",
        "keyword_id": "kw_fb",
        "post_id": "fb1",
        "message": "Phim hay",
        "timestamp": 1790000000,
        "author_id": "a1",
        "author_name": "An",
        "hashtags": ["villah%E1%BB%99ian"],
        "reactions_count": 12,
        "comments_count": 1,
        "shares_count": 0,
    }
    _write_bronze(
        root,
        "posts",
        "facebook",
        "2026-10-01",
        [
            {"offset": 1, "ts": 1790000100, "payload": fb},
            {"offset": 1, "ts": 1790000100, "payload": fb},  # cùng một message Kafka được ghi hai lần
            {"offset": 2, "ts": 1790009000, "payload": {**fb, "reactions_count": 40}},
        ],
    )
    _write_bronze(
        root,
        "posts",
        "threads",
        "2026-10-01",
        [
            {
                "offset": 3,
                "ts": 1790000200,
                "payload": {
                    "platform": "threads",
                    "keyword_id": "kw_th",
                    "post_id": "th1",
                    "message": "Xem chưa",
                    "timestamp": 1790000000,
                    "author_username": "th_user",
                    "like_count": 5,
                    "reply_count": 2,
                    "repost_count": 1,
                },
            },
        ],
    )
    _write_bronze(
        root,
        "posts",
        "tiktok",
        "2026-10-01",
        [
            {
                "offset": 4,
                "ts": 1790000300,
                "payload": {
                    "platform": "tiktok",
                    "keyword_id": "kw_tt",
                    "video_id": "tt1",
                    "desc": "Trailer",
                    "create_time": 1790000000,
                    "author_username": "tt_user",
                    "hashtags": ["conmatthuba"],
                    "like_count": 7,
                    "comment_count": 3,
                    "share_count": 1,
                    "play_count": 3_000_000_000,
                },
            },
        ],
    )
    _write_bronze(
        root,
        "decisions",
        "facebook",
        "2026-10-01",
        [
            {
                "offset": 5,
                "ts": 1790000101,
                "payload": {
                    "platform": "facebook",
                    "post_id": "fb1",
                    "decision": "kept",
                    "reason": "keyword",
                    "decided_at": "2026-10-01T00:00:01+00:00",
                },
            },
            {
                "offset": 6,
                "ts": 1790009001,
                "payload": {
                    "platform": "facebook",
                    "post_id": "fb1",
                    "decision": "dropped",
                    "reason": "kira_irrelevant",
                    "decided_at": "2026-10-01T02:30:00+00:00",
                },
            },
        ],
    )
    _write_bronze(
        root,
        "decisions",
        "tiktok",
        "2026-10-01",
        [
            {
                "offset": 7,
                "ts": 1790000301,
                "payload": {
                    "platform": "tiktok",
                    "post_id": "tt1",
                    "decision": "kept",
                    "reason": "kira",
                    "decided_at": "2026-10-01T00:00:05+00:00",
                },
            },
            {
                "offset": 8,
                "ts": 1790000302,
                "payload": {
                    "platform": "tiktok",
                    "post_id": None,
                    "decision": "dropped",
                    "reason": "pre-fix event without an id",
                    "decided_at": "2026-10-01T00:00:06+00:00",
                },
            },
        ],
    )
    return root


def _post(con: duckdb.DuckDBPyConnection, post_id: str) -> dict:
    # Cột TIMESTAMPTZ cần pytz mới chuyển thành datetime Python được - ở đây không cần.
    cur = con.execute("SELECT * EXCLUDE (posted_at, scraped_at) FROM posts WHERE post_id = ?", [post_id])
    return dict(zip([c[0] for c in cur.description], cur.fetchone(), strict=True))


def test_build_posts_end_to_end(bronze: Path, tmp_path: Path) -> None:
    con = duckdb.connect()
    counts = silver.build_posts(con, bronze=str(bronze), out=str(tmp_path / "silver"))

    assert counts == {"snapshots": 4, "posts": 3}  # message Kafka bị trùng chỉ được tính một lần

    fb = _post(con, "fb1")
    assert fb["likes"] == 40  # lần crawl mới nhất thắng
    assert (fb["decision"], fb["reason"]) == ("dropped", "kira_irrelevant")  # quyết định mới nhất thắng
    assert fb["hashtags"] == ["villahộian"]  # đã URL-decode

    tt = _post(con, "tt1")
    assert (tt["content"], tt["views"], tt["decision"]) == ("Trailer", 3_000_000_000, "kept")
    assert tt["author_username"] == "tt_user" and tt["hashtags"] == ["conmatthuba"]

    th = _post(con, "th1")
    assert (th["likes"], th["comments"], th["shares"], th["decision"]) == (5, 2, 1, None)

    written = con.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}/silver/posts/*/*.parquet')").fetchone()[0]
    assert written == 3


def test_mapping_with_a_missing_column_is_rejected() -> None:
    broken = {**silver.POST_FIELDS, "threads": {k: v for k, v in silver.POST_FIELDS["threads"].items() if k != "likes"}}
    with pytest.raises(ValueError, match="threads mapping: missing \\['likes'\\]"):
        silver._check_mapping(broken, silver.POST_COLUMNS)


def test_check_catches_a_field_mapped_to_the_wrong_name(bronze: Path, tmp_path: Path, monkeypatch) -> None:
    broken = {**silver.POST_FIELDS, "tiktok": {**silver.POST_FIELDS["tiktok"], "content": "payload->>'description'"}}
    monkeypatch.setattr(silver, "POST_FIELDS", broken)
    with pytest.raises(ValueError, match="tiktok: 100% of 1 rows have no content/posted_at"):
        silver.build_posts(duckdb.connect(), bronze=str(bronze), out=str(tmp_path / "silver"))
    assert not (tmp_path / "silver").exists()  # không ghi gì khi một bước kiểm tra thất bại


@pytest.fixture
def bronze_with_comments(bronze: Path) -> Path:
    fb = {
        "platform": "facebook",
        "post_id": "fb1",
        "comment_id": "c1",
        "message": "Đỉnh",
        "timestamp": 1790000500,
        "author_id": "u1",
        "author_name": "Bình",
        "reactions_count": 1,
        "replies_count": 0,
    }
    _write_bronze(
        bronze,
        "comments",
        "facebook",
        "2026-10-01",
        [
            {"offset": 10, "ts": 1790000600, "payload": fb},
            {"offset": 10, "ts": 1790000600, "payload": fb},  # cùng một message Kafka được ghi hai lần
            {"offset": 11, "ts": 1790009600, "payload": {**fb, "reactions_count": 9}},  # crawl lại
        ],
    )
    _write_bronze(
        bronze,
        "comments",
        "threads",
        "2026-10-01",
        [
            {
                "offset": 12,
                "ts": 1790000700,
                "payload": {
                    "platform": "threads",
                    "post_id": "th1",
                    "reply_id": "r2",
                    "parent_reply_id": "r1",
                    "message": "Đồng ý",
                    "timestamp": 1790000650,
                    "author_username": "th_fan",
                    "like_count": 2,
                    "reply_count": 0,
                },
            },
        ],
    )
    _write_bronze(
        bronze,
        "comments",
        "tiktok",
        "2026-10-01",
        [
            {
                "offset": 13,
                "ts": 1790000800,
                "payload": {
                    "platform": "tiktok",
                    "post_id": "tt_old",  # bài crawl từ trước khi có lake -> comment mồ côi
                    "comment_id": "k1",
                    "message": "Hay",
                    "timestamp": 1790000750,
                    "author_name": "Khoa",
                    "like_count": 4,
                    "reply_count": 1,
                },
            },
        ],
    )
    return bronze


def _comment(con: duckdb.DuckDBPyConnection, comment_id: str) -> dict:
    cur = con.execute("SELECT * EXCLUDE (posted_at, scraped_at) FROM comments WHERE comment_id = ?", [comment_id])
    return dict(zip([c[0] for c in cur.description], cur.fetchone(), strict=True))


def test_comment_mapping_matches_columns() -> None:
    silver._check_mapping(silver.COMMENT_FIELDS, silver.COMMENT_COLUMNS)


def test_build_comments_end_to_end(bronze_with_comments: Path, tmp_path: Path) -> None:
    con = duckdb.connect()
    out = str(tmp_path / "silver")
    silver.build_posts(con, bronze=str(bronze_with_comments), out=out)
    counts = silver.build_comments(con, bronze=str(bronze_with_comments), out=out)

    assert counts == {"comments": 3, "orphans": 1}  # tt_old không có trong silver/posts

    c1 = _comment(con, "c1")
    assert (c1["platform"], c1["post_id"], c1["likes"], c1["author_name"]) == ("facebook", "fb1", 9, "Bình")

    r2 = _comment(con, "r2")  # Threads: reply_id/parent_reply_id -> comment_id/parent_comment_id
    assert (r2["parent_comment_id"], r2["content"], r2["author_name"]) == ("r1", "Đồng ý", "th_fan")

    k1 = _comment(con, "k1")
    assert (k1["post_id"], k1["likes"], k1["replies"]) == ("tt_old", 4, 1)

    written = con.sql(f"SELECT count(*) FROM read_parquet('{out}/comments/*/*.parquet')").fetchone()[0]
    assert written == 3


def test_build_comments_rejects_rows_without_ids(bronze_with_comments: Path, tmp_path: Path, monkeypatch) -> None:
    broken = {**silver.COMMENT_FIELDS, "threads": {**silver.COMMENT_FIELDS["threads"], "comment_id": "payload->>'id'"}}
    monkeypatch.setattr(silver, "COMMENT_FIELDS", broken)
    with pytest.raises(ValueError, match="1 comment rows without comment_id/post_id"):
        silver.build_comments(duckdb.connect(), bronze=str(bronze_with_comments), out=str(tmp_path / "silver"))
    assert not (tmp_path / "silver").exists()
