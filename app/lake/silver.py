"""Tầng silver của data lake trên R2: làm sạch kho lưu trữ Kafka thô ở tầng bronze
(app/workers/lake_writer) thành các bảng Parquet có kiểu dữ liệu rõ ràng.

    silver/posts_snapshots/platform=<p>/dt=<d>/  mỗi lần crawl một bài là một dòng (lịch sử tương tác)
    silver/posts/platform=<p>/                    mỗi bài một dòng: lần crawl mới nhất + quyết định của ingest

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

BRONZE = f"r2://{settings.lake_bucket}/bronze"
SILVER = f"r2://{settings.lake_bucket}/silver"
COLS = """{topic: 'VARCHAR', "partition": 'INTEGER', "offset": 'BIGINT', kafka_ts: 'BIGINT', payload: 'JSON'}"""

# Biểu thức giống nhau trên mọi nền tảng.
COMMON = {
    "keyword_id": "payload->>'keyword_id'",
    "url": "payload->>'url'",
    "author_id": "payload->>'author_id'",
    "author_name": "payload->>'author_name'",
    "scraped_at": "to_timestamp(kafka_ts / 1000)",
}

# Mỗi nền tảng phải ánh xạ đúng đủ các cột này - xem _check_mapping. Nếu không,
# một tên viết sai sẽ lọt qua UNION ALL BY NAME thành một cột NULL mà không ai biết.
POST_COLUMNS = {
    "post_id",
    "content",
    "posted_at",
    "author_username",
    "hashtags",
    "likes",
    "comments",
    "shares",
    "views",
}

POST_FIELDS = {
    "facebook": {
        "post_id": "payload->>'post_id'",
        "content": "payload->>'message'",
        "posted_at": "to_timestamp(CAST(payload->>'timestamp' AS BIGINT))",
        "author_username": "NULL::VARCHAR",
        # Spider lưu hashtag Facebook ở dạng URL-encoded (villah%E1%BB%99ian).
        "hashtags": "list_transform(CAST(payload->'hashtags' AS VARCHAR[]), tag -> url_decode(tag))",
        "likes": "CAST(payload->>'reactions_count' AS INT)",
        "comments": "CAST(payload->>'comments_count' AS INT)",
        "shares": "CAST(payload->>'shares_count' AS INT)",
        "views": "NULL::BIGINT",
    },
    "threads": {
        "post_id": "payload->>'post_id'",
        "content": "payload->>'message'",
        "posted_at": "to_timestamp(CAST(payload->>'timestamp' AS BIGINT))",
        "author_username": "payload->>'author_username'",
        "hashtags": "NULL::VARCHAR[]",
        "likes": "CAST(payload->>'like_count' AS INT)",
        "comments": "CAST(payload->>'reply_count' AS INT)",
        "shares": "CAST(payload->>'repost_count' AS INT)",
        "views": "NULL::BIGINT",
    },
    "tiktok": {
        "post_id": "payload->>'video_id'",
        "content": "payload->>'desc'",
        "posted_at": "to_timestamp(CAST(payload->>'create_time' AS BIGINT))",
        "author_username": "payload->>'author_username'",
        "hashtags": "CAST(payload->'hashtags' AS VARCHAR[])",
        "likes": "CAST(payload->>'like_count' AS INT)",
        "comments": "CAST(payload->>'comment_count' AS INT)",
        "shares": "CAST(payload->>'share_count' AS INT)",
        # BIGINT: lượt xem của video viral có thể vượt giới hạn 2,1 tỉ của INT.
        "views": "CAST(payload->>'play_count' AS BIGINT)",
    },
}

# Nếu một nền tảng có hơn tỉ lệ này số dòng thiếu content/posted_at thì gần như
# chắc chắn đang ánh xạ sai tên trường.
MAX_NULL_SHARE = 0.5


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


def _check_mapping() -> None:
    for platform, fields in POST_FIELDS.items():
        if set(fields) != POST_COLUMNS:
            raise ValueError(
                f"{platform} mapping: missing {sorted(POST_COLUMNS - set(fields))}, "
                f"unexpected {sorted(set(fields) - POST_COLUMNS)}"
            )


def _platform_select(platform: str, fields: dict[str, str]) -> str:
    cols = ",\n    ".join(f"{expr} AS {name}" for name, expr in {**COMMON, **fields}.items())
    return f"SELECT '{platform}' AS platform, dt,\n    {cols}\nFROM deduped WHERE platform = '{platform}'"


def _read(bronze: str, entity: str) -> str:
    return (
        f"read_json('{bronze}/entity={entity}/*/*/*.ndjson.gz', "
        f"format='newline_delimited', hive_partitioning=true, columns={COLS})"
    )


def build_posts(con: duckdb.DuckDBPyConnection, *, bronze: str = BRONZE, out: str = SILVER) -> dict[str, int]:
    """Dựng lại silver posts_snapshots + posts từ bronze. `bronze`/`out` có thể
    là thư mục local (khi test, khi thử) hoặc đường dẫn r2://."""
    _check_mapping()

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


def _write(con: duckdb.DuckDBPyConnection, table: str, dest: str, *, partition_by: str) -> None:
    if not dest.startswith("r2://"):
        # DuckDB tự tạo các thư mục partition nhưng không tạo thư mục cha còn thiếu.
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table} TO '{dest}' (FORMAT parquet, PARTITION_BY ({partition_by}), OVERWRITE)")
        return
    # OVERWRITE của DuckDB không hỗ trợ file system từ xa, còn OVERWRITE_OR_IGNORE
    # để lại file của lần chạy trước (dòng bị đếm hai lần) - nên phải xoá đích trước.
    # Chỉ được xoá dưới silver/.
    prefix = dest.split(f"r2://{settings.lake_bucket}/", 1)[1].rstrip("/") + "/"
    if not prefix.startswith("silver/"):
        raise ValueError(f"refusing to clear {prefix!r} - silver writes stay under silver/")
    asyncio.run(delete_prefix(prefix))
    con.execute(f"COPY {table} TO '{dest}' (FORMAT parquet, PARTITION_BY ({partition_by}), OVERWRITE_OR_IGNORE)")
