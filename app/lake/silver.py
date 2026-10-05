"""Tầng silver của data lake trên R2: làm sạch kho lưu trữ Kafka thô ở tầng bronze
(app/workers/lake_writer) thành các bảng Parquet có kiểu dữ liệu rõ ràng.

    silver/posts_snapshots/platform=<p>/dt=<d>/  mỗi lần crawl một bài là một dòng (lịch sử tương tác)
    silver/posts/platform=<p>/                    mỗi bài một dòng: lần crawl mới nhất + quyết định của ingest
    silver/comments/platform=<p>/                 mỗi comment một dòng: lần crawl mới nhất

Mỗi bước là một view/table DuckDB có tên (raw -> deduped -> snapshots -> posts),
được kiểm tra trước khi ghi bất cứ thứ gì. Mỗi lần chạy là dựng lại toàn bộ: cùng
một bronze luôn cho ra cùng một silver, nên chạy lại bao nhiêu lần cũng an toàn.
Chạy bằng scripts/build_silver.py."""

from __future__ import annotations

import asyncio
from pathlib import Path
from urllib.parse import urlsplit

import duckdb

from app.clients.lake import delete_prefix
from app.core.config import settings
from app.core.logging import get_logger
from app.lake.constants import (
    BRONZE,
    COLS,
    COMMENT_COLUMNS,
    COMMENT_COMMON,
    COMMENT_FIELDS,
    COMMON,
    MAX_NULL_SHARE,
    POST_COLUMNS,
    POST_FIELDS,
    SILVER,
    WRITABLE_PREFIXES,
)

logger = get_logger(__name__)


def connect() -> duckdb.DuckDBPyConnection:
    """DuckDB chạy trong bộ nhớ, đọc và ghi lake trên R2 qua đường dẫn r2://
    (một R2 secret tạo từ chính các setting mà app/clients/lake.py dùng)."""
    if not (settings.r2_endpoint and settings.r2_access_key_id and settings.r2_secret_access_key):
        raise RuntimeError("R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY)")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    account_id = urlsplit(settings.r2_endpoint).hostname.split(".")[0]
    con.execute(
        f"CREATE SECRET r2 (TYPE r2, KEY_ID '{settings.r2_access_key_id}', "
        f"SECRET '{settings.r2_secret_access_key}', ACCOUNT_ID '{account_id}')"
    )
    return con


def _check_mapping(mapping: dict[str, dict[str, str]], columns: set[str]) -> None:
    for platform, fields in mapping.items():
        if set(fields) != columns:
            raise ValueError(
                f"{platform} mapping: missing {sorted(columns - set(fields))}, "
                f"unexpected {sorted(set(fields) - columns)}"
            )


def _platform_select(platform: str, fields: dict[str, str], common=COMMON) -> str:
    cols = ",\n    ".join(f"{expr} AS {name}" for name, expr in {**common, **fields}.items())
    return f"SELECT '{platform}' AS platform, dt,\n    {cols}\nFROM deduped WHERE platform = '{platform}'"


def _read(bronze: str, entity: str) -> str:
    return (
        f"read_json('{bronze}/entity={entity}/*/*/*.ndjson.gz', "
        f"format='newline_delimited', hive_partitioning=true, columns={COLS})"
    )


def build_posts(con: duckdb.DuckDBPyConnection, *, bronze: str = BRONZE, out: str = SILVER) -> dict[str, int]:
    """Dựng lại silver posts_snapshots + posts từ bronze. `bronze`/`out` có thể
    là thư mục local (khi test, khi thử) hoặc đường dẫn r2://."""
    _check_mapping(POST_FIELDS, POST_COLUMNS)

    # 1. raw: một view - chưa đọc gì cho tới khi có bước sau cần dùng.
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM {_read(bronze, 'posts')}")

    # 2. deduped: mỗi message Kafka chỉ giữ một dòng (lake writer có thể ghi lại
    #    một batch sau khi bị crash).
    con.execute(
        """CREATE OR REPLACE VIEW deduped AS SELECT * FROM raw
        QUALIFY row_number() OVER (PARTITION BY topic, "partition", "offset" ORDER BY kafka_ts) = 1"""
    )

    # 3. snapshots: là table vì cả posts lẫn lệnh COPY đều đọc nó - các file gzip
    #    trên R2 chỉ phải đọc một lần.
    con.execute(
        "CREATE OR REPLACE TABLE snapshots AS\n"
        + "\nUNION ALL BY NAME\n".join(_platform_select(p, f) for p, f in POST_FIELDS.items())
    )

    # 4. decisions: quyết định giữ/loại mới nhất của mỗi bài.
    con.execute(
        f"""CREATE OR REPLACE VIEW decisions AS
        SELECT payload->>'platform' AS platform, payload->>'post_id' AS post_id,
               payload->>'decision' AS decision, payload->>'reason' AS reason
        FROM {_read(bronze, "decisions")}
        WHERE payload->>'post_id' IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY payload->>'platform', payload->>'post_id' ORDER BY payload->>'decided_at' DESC) = 1"""
    )

    # 5. posts: mỗi bài một dòng (lần crawl mới nhất) + quyết định của nó.
    con.execute(
        """CREATE OR REPLACE TABLE posts AS
        SELECT s.*, d.decision, d.reason
        FROM (SELECT * FROM snapshots
              QUALIFY row_number() OVER (PARTITION BY platform, post_id ORDER BY scraped_at DESC) = 1) s
        LEFT JOIN decisions d USING (platform, post_id)"""
    )

    # 6. kiểm tra xong mới ghi.
    _check(con)

    # 7. ghi.
    _write(con, "snapshots", f"{out}/posts_snapshots", partition_by="platform, dt")
    _write(con, "posts", f"{out}/posts", partition_by="platform")
    return {table: con.sql(f"SELECT count(*) FROM {table}").fetchone()[0] for table in ("snapshots", "posts")}


def _check(con: duckdb.DuckDBPyConnection) -> None:
    problems = []
    dupes = con.sql(
        "SELECT count(*) FROM (SELECT platform, post_id FROM posts GROUP BY ALL HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        problems.append(f"{dupes} duplicate posts")
    no_id = con.sql("SELECT count(*) FROM snapshots WHERE post_id IS NULL").fetchone()[0]
    if no_id:
        problems.append(f"{no_id} snapshot rows without post_id")
    for platform, rows, null_share in con.sql(
        """SELECT platform, count(*),
                  count(*) FILTER (WHERE content IS NULL OR posted_at IS NULL) / count(*)
           FROM snapshots GROUP BY platform"""
    ).fetchall():
        if null_share > MAX_NULL_SHARE:
            problems.append(f"{platform}: {null_share:.0%} of {rows} rows have no content/posted_at")
    snapshots, posts = (con.sql(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("snapshots", "posts"))
    if posts > snapshots:
        problems.append(f"posts ({posts}) > snapshots ({snapshots})")
    if problems:
        raise ValueError("silver posts check failed: " + "; ".join(problems))


def _write(con: duckdb.DuckDBPyConnection, table: str, dest: str, *, partition_by: str | None = None) -> None:
    if partition_by:
        target, options = dest, f"FORMAT parquet, PARTITION_BY ({partition_by})"
    else:
        target, options = f"{dest}/{Path(dest).name}.parquet", "FORMAT parquet"

    if not dest.startswith("r2://"):
        # DuckDB tự tạo các thư mục partition nhưng không tạo thư mục cha còn thiếu.
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        overwrite = ", OVERWRITE" if partition_by else ""
        con.execute(f"COPY {table} TO '{target}' ({options}{overwrite})")
        return
    # OVERWRITE của DuckDB không hỗ trợ file system từ xa, còn OVERWRITE_OR_IGNORE
    # để lại file của lần chạy trước (dòng bị đếm hai lần) - nên phải xoá đích trước.
    # Chỉ được xoá dưới silver/gold.
    prefix = dest.split(f"r2://{settings.lake_bucket}/", 1)[1].rstrip("/") + "/"
    if not prefix.startswith(WRITABLE_PREFIXES):
        raise ValueError(f"refusing to clear {prefix!r} - lake writes stay under silver/ or gold/")
    asyncio.run(delete_prefix(prefix))
    overwrite = ", OVERWRITE_OR_IGNORE" if partition_by else ""
    con.execute(f"COPY {table} TO '{target}' ({options}{overwrite})")


def build_comments(con: duckdb.DuckDBPyConnection, *, bronze: str = BRONZE, out: str = SILVER) -> dict[str, int]:
    """Dựng lại silver comments từ bronze. Chạy sau build_posts: phần kiểm tra
    comment mồ côi đọc silver/posts vừa ghi ở `out`."""
    _check_mapping(COMMENT_FIELDS, COMMENT_COLUMNS)

    # 1-2. raw + deduped: giống build_posts.
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM {_read(bronze, 'comments')}")
    con.execute(
        """CREATE OR REPLACE VIEW deduped AS SELECT * FROM raw
        QUALIFY row_number() OVER (PARTITION BY topic, "partition", "offset" ORDER BY kafka_ts) = 1"""
    )

    # 3. comment_snapshots: mỗi lần crawl một comment là một dòng. Tên riêng để không
    #    đè bảng snapshots của posts trong cùng connection.
    con.execute(
        "CREATE OR REPLACE TABLE comment_snapshots AS\n"
        + "\nUNION ALL BY NAME\n".join(_platform_select(p, f, common=COMMENT_COMMON) for p, f in COMMENT_FIELDS.items())
    )

    # 4. comments: mỗi comment một dòng, lần crawl mới nhất thắng.
    con.execute(
        """CREATE OR REPLACE TABLE comments AS SELECT * EXCLUDE (dt) FROM comment_snapshots
        QUALIFY row_number() OVER (PARTITION BY platform, comment_id ORDER BY scraped_at DESC) = 1"""
    )

    # 5. kiểm tra xong mới ghi.
    orphans = _check_comments(con, posts=f"{out}/posts")

    # 6. ghi.
    _write(con, "comments", f"{out}/comments", partition_by="platform")
    return {"comments": con.sql("SELECT count(*) FROM comments").fetchone()[0], "orphans": orphans}


def _check_comments(con: duckdb.DuckDBPyConnection, *, posts: str) -> int:
    """Raise khi dữ liệu sai rõ ràng; trả về số comment mồ côi (post_id không có
    trong silver/posts) - chỉ log, vì comment có thể được crawl cho bài có từ
    trước khi có lake."""
    problems = []
    no_id = con.sql("SELECT count(*) FROM comment_snapshots WHERE comment_id IS NULL OR post_id IS NULL").fetchone()[0]
    if no_id:
        problems.append(f"{no_id} comment rows without comment_id/post_id")
    dupes = con.sql(
        "SELECT count(*) FROM (SELECT platform, comment_id FROM comments GROUP BY ALL HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        problems.append(f"{dupes} duplicate comments")
    for platform, rows, null_share in con.sql(
        """SELECT platform, count(*),
                  count(*) FILTER (WHERE content IS NULL OR posted_at IS NULL) / count(*)
           FROM comment_snapshots GROUP BY platform"""
    ).fetchall():
        if null_share > MAX_NULL_SHARE:
            problems.append(f"{platform}: {null_share:.0%} of {rows} comment rows have no content/posted_at")
    if problems:
        raise ValueError("silver comments check failed: " + "; ".join(problems))

    try:
        by_platform = con.sql(
            f"""SELECT platform, count(*) FROM comments c
            ANTI JOIN read_parquet('{posts}/*/*.parquet', hive_partitioning=true) p USING (platform, post_id)
            GROUP BY platform ORDER BY platform"""
        ).fetchall()
    except duckdb.IOException:
        logger.warning("silver_comments_orphan_check_skipped", reason="no silver posts", posts=posts)
        return 0
    orphans = sum(n for _, n in by_platform)
    if orphans:
        logger.info("silver_comments_orphans", total=orphans, by_platform=dict(by_platform))
    return orphans
