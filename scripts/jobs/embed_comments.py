"""Tính embedding e5-small cho toàn bộ comment có nội dung (2026-11-05), làm nền cho gom chủ đề riêng từng phim.

D1 không chứa nổi (~2,2 triệu dòng x 384 chiều), nên lưu ở máy chạy job: data/comment_embeddings.sqlite, float16.
Comment chỉ có tag tên, icon hay tiếng cười (rule_sentiment) không có nội dung để
embed nên bỏ qua. Phản hồi được embed kèm bình luận cha để giữ ngữ cảnh (cột with_context). Đọc theo id, con trỏ lưu trong file
nên dừng rồi chạy lại được.

    python -m scripts.jobs.embed_comments --limit 2000
    python -m scripts.jobs.embed_comments
    python -m scripts.jobs.embed_comments --movie-id <id>   # bù comment mới của một phim (con trỏ chung theo id bỏ sót chúng)
    python -m scripts.jobs.embed_comments --showing --hours 2   # phim đang chiếu, comment về trong 2 giờ qua (timer VPS)
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy import and_, literal, select

from app.ai.local.embedding import embed_texts
from app.ai.tasks.sentiment_rules import rule_sentiment, with_parent_context
from app.repositories.d1.sql import d1_exec, d1_query
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import posts as posts_table

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "comment_embeddings.sqlite"
PAGE_SIZE = 2000
OVERLOAD_PAUSE_SECONDS = 60.0


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS comment_embeddings (
            comment_id TEXT PRIMARY KEY, movie_id TEXT, embedding BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_comment_embeddings_movie ON comment_embeddings (movie_id);
        CREATE TABLE IF NOT EXISTS embed_cursor (id INTEGER PRIMARY KEY CHECK (id = 1), last_id TEXT NOT NULL);
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(comment_embeddings)")}
    if "with_context" not in columns:
        conn.execute("ALTER TABLE comment_embeddings ADD COLUMN with_context INTEGER NOT NULL DEFAULT 0")
    return conn


def texts_to_embed(rows: list[dict[str, Any]], done: dict[str, int] | None = None) -> list[tuple[dict[str, Any], str]]:
    """Bỏ comment không có nội dung và dòng đã embed đúng. Phản hồi đã embed trước khi có ngữ cảnh thì làm lại."""
    done = done or {}
    picked = []
    for row in rows:
        if rule_sentiment(row.get("message")) is not None:
            continue
        parent = row.get("parent_message")
        if row["id"] in done and (not parent or done[row["id"]]):
            continue
        text = with_parent_context(row.get("message"), parent)
        if text:
            picked.append((row, text))
    return picked


def save_page(
    conn: sqlite3.Connection, picked: list[tuple[dict[str, Any], str]], vectors: np.ndarray, last_id: str
) -> None:
    with conn:
        conn.executemany(
            "INSERT OR REPLACE INTO comment_embeddings (comment_id, movie_id, embedding, with_context) VALUES (?, ?, ?, ?)",
            [
                (
                    row["id"],
                    row.get("movie_id"),
                    vector.astype(np.float16).tobytes(),
                    int(bool(row.get("parent_message"))),
                )
                for (row, _), vector in zip(picked, vectors, strict=True)
            ],
        )
        conn.execute("INSERT OR REPLACE INTO embed_cursor (id, last_id) VALUES (1, ?)", (last_id,))


MOVIE_POST_CHUNK = 40


async def _read(stmt: Any) -> list[dict[str, Any]]:
    """Đọc một trang; D1 quá tải thì nghỉ rồi thử lại (câu dựng bằng SQLAlchemy Core)."""
    while True:
        rows = await d1_exec(stmt, timeout=60.0, quiet=True)
        if rows is not None:
            return rows
        print(f"đọc lỗi (D1 có thể quá tải), nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
        await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)


async def _movie_pages(movie_id: str, since: str | None = None):
    """Comment của một phim theo từng nhóm bài, qua index post_id (09/11/2026) - lọc `post_id IN (bài của phim)` trên câu
    sắp theo id quét gần như cả bảng comments mỗi trang và làm D1 quá tải. Chỉ còn dùng cho --movie-id (quét lại cả phim, chạy
    tay); --showing dùng _recent_pages."""
    post_ids = [row["id"] for row in await _read(select(posts_table.c.id).where(posts_table.c.movie_id == movie_id))]
    c = comments_table.alias("c")
    par = comments_table.alias("par")
    for i in range(0, len(post_ids), MOVIE_POST_CHUNK):
        stmt = (
            select(c.c.id, c.c.message, literal(movie_id).label("movie_id"), par.c.message.label("parent_message"))
            .select_from(
                c.outerjoin(par, and_(par.c.platform == c.c.platform, par.c.external_id == c.c.parent_external_id))
            )
            .where(c.c.post_id.in_(post_ids[i : i + MOVIE_POST_CHUNK]))
        )
        if since:
            stmt = stmt.where(c.c.first_seen_at >= since)
        rows = await _read(stmt)
        if rows:
            yield rows


async def _recent_pages(movie_ids: list[str], since: str):
    """Bình luận thu từ `since` của các phim `movie_ids`, đọc thẳng theo index scraped_at (18/11/2026).

    Trước đây --showing đi theo _movie_pages: mỗi phim đọc hết id bài, rồi mỗi nhóm 40 bài một truy vấn - khoảng 3.000 truy vấn
    mỗi giờ (77.752 trong 24 giờ đo bằng wrangler d1 insights) để tìm vài trăm bình luận mới, và là nguồn D1 quá tải lớn nhất theo
    số lần. Bình luận mới có scraped_at = lúc thu, nên lọc scraped_at >= since ra một tập bao trùm (gồm cả bình luận cũ vừa được
    thu lại - dòng đã embed thì main() bỏ qua). INDEXED BY: không ép thì planner có thể đi đường posts.movie_id rồi đọc mọi bình
    luận của phim."""
    if not movie_ids:
        return
    marks = ",".join("?" * len(movie_ids))
    sql = (
        "SELECT c.id, c.message, p.movie_id, par.message AS parent_message "
        "FROM comments c INDEXED BY idx_comments_scraped_at JOIN posts p ON p.id = c.post_id "
        "LEFT JOIN comments par ON par.platform = c.platform AND par.external_id = c.parent_external_id "
        f"WHERE c.scraped_at >= ? AND p.movie_id IN ({marks}) ORDER BY c.scraped_at DESC LIMIT ? OFFSET ?"
    )
    offset = 0
    while True:
        while (rows := await d1_query(sql, [since, *movie_ids, PAGE_SIZE, offset], timeout=60.0, quiet=True)) is None:
            print(f"đọc lỗi (D1 có thể quá tải), nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
            await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)
        if rows:
            yield rows
        if len(rows) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


async def main(args: argparse.Namespace) -> None:
    """Điểm vào dòng lệnh: tính embedding e5-small cho bình luận có nội dung chưa có vector và lưu vào
    data/comment_embeddings.sqlite (float16); bình luận chỉ có tag/icon/tiếng cười được bỏ qua."""
    conn = connect()
    row = conn.execute("SELECT last_id FROM embed_cursor WHERE id = 1").fetchone()
    cursor_id = "" if not row else row[0]
    last_id = "" if args.restart else cursor_id
    # Con trỏ chung chỉ tiến ở chế độ mặc định;

    by_movie = bool(args.movie_id or args.showing)
    read = saved = 0
    started = time.monotonic()

    async def pages():
        nonlocal last_id
        if args.showing:
            from app.services.relevance.trend_posts import showing_movies

            since = (datetime.now(tz=UTC) - timedelta(hours=args.hours)).isoformat(timespec="seconds")
            movie_ids = [movie["id"] for movie in await showing_movies(datetime.now(tz=UTC))]
            async for rows in _recent_pages(movie_ids, since):
                yield rows
            return
        if args.movie_id:
            # id comment là uuid nên comment mới không nằm sau con trỏ chung: quét lại comment của phim, bỏ dòng đã embed.
            async for rows in _movie_pages(args.movie_id):
                yield rows
            return
        c = comments_table.alias("c")
        p = posts_table.alias("p")
        par = comments_table.alias("par")
        while not args.limit or read < args.limit:
            stmt = (
                select(c.c.id, c.c.message, p.c.movie_id, par.c.message.label("parent_message"))
                .select_from(
                    c.outerjoin(p, p.c.id == c.c.post_id).outerjoin(
                        par, and_(par.c.platform == c.c.platform, par.c.external_id == c.c.parent_external_id)
                    )
                )
                .where(c.c.id > last_id)
                .order_by(c.c.id)
                .limit(PAGE_SIZE)
            )
            rows = await _read(stmt)
            if not rows:
                return
            last_id = rows[-1]["id"]
            yield rows

    async for rows in pages():
        marks = ",".join("?" * len(rows))
        done = dict(
            conn.execute(
                f"SELECT comment_id, with_context FROM comment_embeddings WHERE comment_id IN ({marks})",
                [r["id"] for r in rows],
            )
        )
        picked = texts_to_embed(rows, done)
        vectors = embed_texts([text for _, text in picked]) if picked else np.zeros((0, 384), dtype=np.float32)
        save_page(conn, picked, vectors, cursor_id if by_movie else last_id)
        read += len(rows)
        saved += len(picked)
        if read % (PAGE_SIZE * 25) < len(rows):
            print(f"{read} đọc, {saved} embed, {read / (time.monotonic() - started):.0f} dòng/giây", flush=True)
    print(f"xong lượt này: {read} đọc, {saved} embed trong {(time.monotonic() - started) / 60:.1f} phút")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=0, help="đọc tối đa ngần này comment lượt này")
    parser.add_argument("--restart", action="store_true", help="đọc lại từ đầu, dòng đã embed đúng thì bỏ qua")
    parser.add_argument("--movie-id", help="chỉ comment của một phim, quét lại từ đầu")
    parser.add_argument(
        "--showing", action="store_true", help="chỉ phim đang chiếu, bình luận về trong --hours giờ qua"
    )
    parser.add_argument("--hours", type=float, default=6)
    asyncio.run(main(parser.parse_args()))
