"""Kéo các bài local chưa có từ database D1 thật - bản an toàn, chỉ thêm vào, tương ứng
với phần đẩy posts của scripts/push_local_data_to_remote.py. Viết cho ngày
2026-09-22: sau lần đẩy trước đó, remote có những bài mà local chưa từng có (remote
giữ mọi thứ đã được đẩy lên; trong khi đó crawl ở local vẫn tiếp tục sinh comment mà
remote chưa có - xem lần kiểm tra số bài hôm đó). Script này đưa bảng *posts* của
local về ngang bằng remote mà không đụng tới comments (local đã đi trước ở đó - không
có gì để kéo) và không xoá-rồi-tạo-lại kiểu phá huỷ như scripts/pull_local_db.py
(làm vậy sẽ xoá luôn các dòng comment chỉ có ở local chưa được đẩy lên remote, và
không an toàn khi có server/consumer đang mở file db local này - xem docstring của
script đó). Script này chỉ INSERT (OR IGNORE) vào các bảng local đã có, nên chạy cùng
lúc với server/consumer đang dùng DB_MODE=local là an toàn - chế độ WAL vốn đã cho
đọc/ghi tiếp trên snapshot đã commit gần nhất (xem comment _get_local_conn trong
app/clients/d1.py).

Id của movie/keyword có thể khác nhau giữa local và remote cho cùng một dòng logic
(một movie/keyword chỉ có ở local được tạo khi đang DB_MODE=local trước khi được đẩy
lên sẽ có uuid khác với dòng mà một lần đẩy sau đó ghép với nó theo slug/text từ khoá
- xem các hàm remap của push_local_data_to_remote.py). Ở đây cùng ý tưởng, chỉ là
theo chiều id ngược lại, để movie_id/keyword_id của một bài được kéo về trỏ tới dòng
LOCAL mà các bài local khác đang tham chiếu, không phải một id chỉ có trên remote bị
treo.

Chạy lại lúc nào cũng an toàn - so khác biệt theo id giống script đẩy, nên id đã có ở
local chỉ bị bỏ qua.

    python -m scripts.pull_new_posts_from_remote
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Cùng lý do về kích thước response của D1 như _PAGE_SIZE trong scripts/pull_local_db.py.
_PAGE_SIZE = 500


def _local_ids(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[0] for row in conn.execute(f"SELECT id FROM {table}")}


async def _movie_id_remap_from_remote(d1_query, local_conn: sqlite3.Connection) -> dict[str, str]:
    """remote_id -> local_id cho các movie là cùng một dòng (theo slug) nhưng khác uuid."""
    remote = await d1_query("SELECT id, slug FROM movies")
    if remote is None:
        raise RuntimeError("Could not read remote movies - check D1 credentials/quota.")
    local_id_by_slug = {row["slug"]: row["id"] for row in local_conn.execute("SELECT id, slug FROM movies")}
    remap = {
        r["id"]: local_id_by_slug[r["slug"]]
        for r in remote
        if r["slug"] in local_id_by_slug and local_id_by_slug[r["slug"]] != r["id"]
    }
    if remap:
        logger.info("movie_ids_remapped_from_remote", count=len(remap))
    return remap


async def _keyword_id_remap_from_remote(
    d1_query, local_conn: sqlite3.Connection, movie_id_remap: dict[str, str]
) -> dict[str, str]:
    """remote_id -> local_id cho các keyword là cùng một dòng (theo movie/platform/text) nhưng khác uuid."""
    remote = await d1_query("SELECT id, movie_id, platform, keyword FROM keywords")
    if remote is None:
        raise RuntimeError("Could not read remote keywords - check D1 credentials/quota.")
    local_id_by_key = {
        (row["movie_id"], row["platform"], row["keyword"]): row["id"]
        for row in local_conn.execute("SELECT id, movie_id, platform, keyword FROM keywords")
    }
    remap: dict[str, str] = {}
    for r in remote:
        local_movie_id = movie_id_remap.get(r["movie_id"], r["movie_id"])
        local_id = local_id_by_key.get((local_movie_id, r["platform"], r["keyword"]))
        if local_id and local_id != r["id"]:
            remap[r["id"]] = local_id
    if remap:
        logger.info("keyword_ids_remapped_from_remote", count=len(remap))
    return remap


def _remap_row(row: dict[str, Any], movie_id_remap: dict[str, str], keyword_id_remap: dict[str, str]) -> dict[str, Any]:
    remapped = dict(row)
    if remapped.get("movie_id") in movie_id_remap:
        remapped["movie_id"] = movie_id_remap[remapped["movie_id"]]
    if remapped.get("keyword_id") in keyword_id_remap:
        remapped["keyword_id"] = keyword_id_remap[remapped["keyword_id"]]
    return remapped


async def pull_new_posts() -> None:
    settings.db_mode = "remote"  # đọc từ remote; ghi thẳng vào file sqlite local bên dưới
    from app.services.d1 import d1_query  # import sau khi đã ép remote, không phải lúc nạp module

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError(
            "CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set (in .env) to read "
            "from the real D1 - this script has nothing to pull from without them."
        )

    local_path = Path(settings.local_db_path)
    if not local_path.exists():
        raise RuntimeError(f"No local mirror at {local_path} - nothing to pull into.")

    # timeout=30 + WAL, giống _get_local_conn trong app/clients/d1.py - một consumer
    # ingest/crawl đang chạy có thể đang mở file này cùng lúc.
    local_conn = sqlite3.connect(local_path, timeout=30.0)
    local_conn.execute("PRAGMA journal_mode=WAL")
    local_conn.execute("PRAGMA busy_timeout=30000")
    local_conn.row_factory = sqlite3.Row

    try:
        movie_id_remap = await _movie_id_remap_from_remote(d1_query, local_conn)
        keyword_id_remap = await _keyword_id_remap_from_remote(d1_query, local_conn, movie_id_remap)

        local_ids = _local_ids(local_conn, "posts")
        columns: list[str] | None = None
        insert_sql: str | None = None
        offset = 0
        total_seen = 0
        total_pulled = 0
        while True:
            rows = await d1_query(f"SELECT * FROM posts LIMIT {_PAGE_SIZE} OFFSET {offset}")
            if rows is None:
                raise RuntimeError(f"D1 read failed for posts at offset={offset} - see logged error above.")
            if not rows:
                break
            total_seen += len(rows)
            new_rows = [r for r in rows if r["id"] not in local_ids]
            if new_rows:
                if columns is None:
                    columns = list(new_rows[0].keys())
                    placeholders = ", ".join("?" for _ in columns)
                    insert_sql = f"INSERT OR IGNORE INTO posts ({', '.join(columns)}) VALUES ({placeholders})"
                remapped = [_remap_row(r, movie_id_remap, keyword_id_remap) for r in new_rows]
                local_conn.executemany(insert_sql, [[r[c] for c in columns] for r in remapped])
                local_conn.commit()
                total_pulled += len(new_rows)
                logger.info("posts_batch_pulled", pulled_so_far=total_pulled, remote_seen=total_seen)
            offset += _PAGE_SIZE
            if len(rows) < _PAGE_SIZE:
                break
    finally:
        local_conn.close()

    logger.info("pull_new_posts_finished", telegram=True, remote_seen=total_seen, pulled=total_pulled)


if __name__ == "__main__":
    asyncio.run(pull_new_posts())
