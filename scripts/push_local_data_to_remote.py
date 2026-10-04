"""Đẩy dữ liệu mới từ bản sao SQLite local (xem scripts/pull_local_db.py) lên database D1
thật - chiều ngược lại một lần mà script kia chưa bao giờ cần, viết cho phiên làm việc
ngày 2026-09-15: DB_MODE=local đang bật trong lúc chạy một lô crawl đầy đủ, nên mọi
bài/comment (và mọi từ khoá thêm qua dashboard trong lúc đó) chỉ nằm trong
.local-db/scraper.sqlite, production không thấy cho tới khi được đẩy lên.

Phạm vi (theo yêu cầu - cố ý bỏ post_engagement_snapshots, vì có thể bỏ đi hoặc tạo
lại được, không đáng tốn thêm quota ghi D1):
  - keywords (và, vì là điều kiện khoá ngoại bắt buộc, mọi movie chúng cần) có ở local
    nhưng chưa có trên remote
  - mọi bài ở local, keyword_id được kiểm tra với các keyword đã migrate lên remote
  - mọi comment ở local, mà dòng post_id phải đã có trên remote

Thứ tự insert quan trọng (ràng buộc khoá ngoại): movies -> keywords -> posts ->
comments. Mọi lệnh insert dùng "INSERT OR IGNORE" và chạy lại an toàn - id đã lên tới
remote (một lần chạy dở trước đó, hoặc vốn đã có ở đó) bị bỏ qua âm thầm thay vì làm
lỗi cả lô.

Movie/keyword ở local có thể trùng slug/text duy nhất với một dòng remote nhưng khác
uuid (dashboard tạo chúng khi đang DB_MODE=local). Các id đó được ghi lại thành id của
dòng remote trước khi insert bảng con.

Luôn ép db_mode="remote" cho các lần ghi (cùng lý do như pull_local_db.py: đọc file
sqlite *local* trực tiếp bằng connection riêng, không qua d1_query, nên ép remote
không thể vô tình trỏ d1_query ngược về file đang đọc) và chia insert theo lô để nằm
dưới giới hạn kích thước/response mỗi câu lệnh của D1 và timeout HTTP D1 10s của
project này.

    python -m scripts.push_local_data_to_remote
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# HTTP query API của D1 từ chối câu lệnh vượt quá một số lượng tham số bind nào đó, thấp
# hơn nhiều so với mức 999 thường thấy của SQLite (đã xác nhận thực tế 2026-09-15: một
# lô movies 16 dòng, 12 cột - 192 tham số - đã bị từ chối là "too many SQL variables").
# Không được ghi ở chỗ nào dễ thấy, nên giữ khoảng cách an toàn thay vì đi dò con số
# chính xác: giới hạn mỗi lô theo số *tham số* (dòng * số cột), không theo số dòng cố
# định, để bảng rộng (posts, 19 cột) tự động có lô ít dòng hơn bảng hẹp (comments, 15)
# mà không cần hằng số gán cứng riêng.
_MAX_PARAMS_PER_BATCH = 90


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


async def _remote_ids(d1_query, table: str) -> set[str]:
    """Duyệt id trên remote theo trang - một câu SELECT mọi posts.id có thể vượt timeout HTTP
    10s của d1_query khi bảng đã tới hàng chục nghìn dòng, làm huỷ cả lần đẩy trước khi
    ghi được gì."""
    ids: set[str] = set()
    page_size = 2000
    offset = 0
    while True:
        rows = await d1_query(f"SELECT id FROM {table} LIMIT {page_size} OFFSET {offset}")
        if rows is None:
            raise RuntimeError(f"Could not read remote {table!r} ids - check D1 credentials/quota.")
        ids.update(row["id"] for row in rows)
        if len(rows) < page_size:
            return ids
        offset += page_size


def _row_values(
    row: sqlite3.Row,
    columns: list[str],
    movie_id_remap: dict[str, str],
    keyword_id_remap: dict[str, str],
) -> list[Any]:
    values: list[Any] = []
    for column in columns:
        value = row[column]
        if column == "movie_id" and value in movie_id_remap:
            value = movie_id_remap[value]
        elif column == "keyword_id" and value in keyword_id_remap:
            value = keyword_id_remap[value]
        values.append(value)
    return values


async def _movie_id_remap(d1_query, local_conn: sqlite3.Connection) -> dict[str, str]:
    remote = await d1_query("SELECT id, slug FROM movies")
    if remote is None:
        raise RuntimeError("Could not read remote movies for slug remap - check D1 credentials/quota.")
    remote_id_by_slug = {row["slug"]: row["id"] for row in remote}
    remap: dict[str, str] = {}
    for row in local_conn.execute("SELECT id, slug FROM movies"):
        remote_id = remote_id_by_slug.get(row["slug"])
        if remote_id and remote_id != row["id"]:
            remap[row["id"]] = remote_id
    if remap:
        logger.info("movie_ids_remapped_by_slug", count=len(remap), local_ids=sorted(remap))
    return remap


async def _keyword_id_remap(
    d1_query,
    local_conn: sqlite3.Connection,
    movie_id_remap: dict[str, str],
) -> dict[str, str]:
    remote = await d1_query("SELECT id, movie_id, platform, keyword FROM keywords")
    if remote is None:
        raise RuntimeError("Could not read remote keywords for remap - check D1 credentials/quota.")
    remote_id_by_key = {(row["movie_id"], row["platform"], row["keyword"]): row["id"] for row in remote}
    remap: dict[str, str] = {}
    for row in local_conn.execute("SELECT id, movie_id, platform, keyword FROM keywords"):
        movie_id = movie_id_remap.get(row["movie_id"], row["movie_id"])
        remote_id = remote_id_by_key.get((movie_id, row["platform"], row["keyword"]))
        if remote_id and remote_id != row["id"]:
            remap[row["id"]] = remote_id
    if remap:
        logger.info("keyword_ids_remapped", count=len(remap))
    return remap


async def _insert_batch(
    d1_query,
    table: str,
    columns: list[str],
    rows: list[sqlite3.Row],
    movie_id_remap: dict[str, str],
    keyword_id_remap: dict[str, str],
) -> int:
    if not rows:
        return 0
    placeholders = "(" + ", ".join("?" for _ in columns) + ")"
    sql = f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES " + ", ".join([placeholders] * len(rows))
    params: list[Any] = []
    for row in rows:
        params.extend(_row_values(row, columns, movie_id_remap, keyword_id_remap))
    result = await d1_query(sql, params)
    if result is None:
        if len(rows) > 1:
            inserted = 0
            for row in rows:
                inserted += await _insert_batch(d1_query, table, columns, [row], movie_id_remap, keyword_id_remap)
            return inserted
        logger.warning("row_skipped_fk", table=table, row_id=rows[0]["id"])
        return 0
    return len(rows)


async def _push_new_rows(
    d1_query,
    local_conn: sqlite3.Connection,
    table: str,
    movie_id_remap: dict[str, str] | None = None,
    keyword_id_remap: dict[str, str] | None = None,
) -> int:
    columns = _table_columns(local_conn, table)
    batch_size = max(1, _MAX_PARAMS_PER_BATCH // len(columns))
    remote_ids = await _remote_ids(d1_query, table)
    local_rows = local_conn.execute(f"SELECT * FROM {table}").fetchall()
    new_rows = [r for r in local_rows if r["id"] not in remote_ids]
    movie_remap = movie_id_remap or {}
    keyword_remap = keyword_id_remap or {}
    if table == "keywords" and keyword_remap:
        new_rows = [r for r in new_rows if r["id"] not in keyword_remap]

    logger.info(
        "table_diffed",
        table=table,
        local_total=len(local_rows),
        already_remote=len(remote_ids),
        new=len(new_rows),
        batch_size=batch_size,
    )

    pushed = 0
    stop_file = Path("/tmp/push_local_data_to_remote.STOP")
    for i in range(0, len(new_rows), batch_size):
        if stop_file.exists():
            logger.warning("push_stop_signal_received", table=table, pushed_so_far=pushed)
            return pushed
        batch = new_rows[i : i + batch_size]
        pushed += await _insert_batch(d1_query, table, columns, batch, movie_remap, keyword_remap)
        logger.info("table_batch_pushed", table=table, pushed_so_far=pushed, of=len(new_rows))
    return pushed


async def push() -> None:
    settings.db_mode = "remote"  # xem docstring module - ghi vào D1 thật, đọc thẳng từ file local
    from app.services.d1 import d1_query  # import sau khi đã ép remote, không phải lúc nạp module

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError(
            "CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set (in .env) to push to "
            "the real D1 - this script has nothing to write to without them."
        )

    local_path = Path(settings.local_db_path)
    if not local_path.exists():
        raise RuntimeError(f"No local mirror at {local_path} - nothing to push.")

    local_conn = sqlite3.connect(local_path)
    local_conn.row_factory = sqlite3.Row
    try:
        movies_pushed = await _push_new_rows(d1_query, local_conn, "movies")
        movie_id_remap = await _movie_id_remap(d1_query, local_conn)
        keyword_id_remap = await _keyword_id_remap(d1_query, local_conn, movie_id_remap)
        keywords_pushed = await _push_new_rows(d1_query, local_conn, "keywords", movie_id_remap, keyword_id_remap)
        keyword_id_remap = await _keyword_id_remap(d1_query, local_conn, movie_id_remap)
        posts_pushed = await _push_new_rows(d1_query, local_conn, "posts", movie_id_remap, keyword_id_remap)
        comments_pushed = await _push_new_rows(d1_query, local_conn, "comments")
    finally:
        local_conn.close()

    logger.info(
        "push_finished",
        telegram=True,
        movies_pushed=movies_pushed,
        keywords_pushed=keywords_pushed,
        posts_pushed=posts_pushed,
        comments_pushed=comments_pushed,
    )


if __name__ == "__main__":
    asyncio.run(push())
