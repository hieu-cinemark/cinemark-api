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


def test_mapping_with_a_missing_column_is_rejected(monkeypatch) -> None:
    broken = {**silver.POST_FIELDS, "threads": {k: v for k, v in silver.POST_FIELDS["threads"].items() if k != "likes"}}
    monkeypatch.setattr(silver, "POST_FIELDS", broken)
    with pytest.raises(ValueError, match="threads mapping: missing \\['likes'\\]"):
        silver._check_mapping()


def test_check_catches_a_field_mapped_to_the_wrong_name(bronze: Path, tmp_path: Path, monkeypatch) -> None:
    broken = {**silver.POST_FIELDS, "tiktok": {**silver.POST_FIELDS["tiktok"], "content": "payload->>'description'"}}
    monkeypatch.setattr(silver, "POST_FIELDS", broken)
    with pytest.raises(ValueError, match="tiktok: 100% of 1 rows have no content/posted_at"):
        silver.build_posts(duckdb.connect(), bronze=str(bronze), out=str(tmp_path / "silver"))
    assert not (tmp_path / "silver").exists()  # không ghi gì khi một bước kiểm tra thất bại
