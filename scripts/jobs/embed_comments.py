"""Tính embedding e5-small cho toàn bộ comment có nội dung (2026-11-05), làm nền cho gom chủ đề riêng từng phim.

D1 không chứa nổi (~2,2 triệu dòng x 384 chiều), nên lưu ở máy chạy job: data/comment_embeddings.sqlite, float16 như
app/repositories/local/topic_state.py. Comment chỉ có tag tên, icon hay tiếng cười (rule_sentiment) không có nội dung để
embed nên bỏ qua. Phản hồi được embed kèm bình luận cha để giữ ngữ cảnh (cột with_context). Đọc theo id, con trỏ lưu trong file
nên dừng rồi chạy lại được.

    python -m scripts.jobs.embed_comments --limit 2000
    python -m scripts.jobs.embed_comments
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import time
from pathlib import Path
from typing import Any

import numpy as np

from app.ai.local.embedding import embed_texts
from app.ai.tasks.sentiment_rules import rule_sentiment, with_parent_context
from app.repositories.d1.store import d1_query

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


def save_page(conn: sqlite3.Connection, picked: list[tuple[dict[str, Any], str]], vectors: np.ndarray, last_id: str) -> None:
    with conn:
        conn.executemany(
            "INSERT OR REPLACE INTO comment_embeddings (comment_id, movie_id, embedding, with_context) VALUES (?, ?, ?, ?)",
            [
                (row["id"], row.get("movie_id"), vector.astype(np.float16).tobytes(), int(bool(row.get("parent_message"))))
                for (row, _), vector in zip(picked, vectors, strict=True)
            ],
        )
        conn.execute("INSERT OR REPLACE INTO embed_cursor (id, last_id) VALUES (1, ?)", (last_id,))


async def main(args: argparse.Namespace) -> None:
    conn = connect()
    row = conn.execute("SELECT last_id FROM embed_cursor WHERE id = 1").fetchone()
    last_id = "" if args.restart or not row else row[0]
    read = saved = 0
    started = time.monotonic()
    while not args.limit or read < args.limit:
        rows = await d1_query(
            "SELECT c.id, c.message, p.movie_id, par.message AS parent_message FROM comments c "
            "LEFT JOIN posts p ON p.id = c.post_id "
            "LEFT JOIN comments par ON par.platform = c.platform AND par.external_id = c.parent_external_id "
            "WHERE c.id > ? ORDER BY c.id LIMIT ?",
            [last_id, PAGE_SIZE],
            timeout=60.0,
            quiet=True,
        )
        if rows is None:
            print(f"đọc lỗi (D1 có thể quá tải), nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
            await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)
            continue
        if not rows:
            break
        marks = ",".join("?" * len(rows))
        done = dict(conn.execute(f"SELECT comment_id, with_context FROM comment_embeddings WHERE comment_id IN ({marks})", [r["id"] for r in rows]))
        picked = texts_to_embed(rows, done)
        vectors = embed_texts([text for _, text in picked]) if picked else np.zeros((0, 384), dtype=np.float32)
        last_id = rows[-1]["id"]
        save_page(conn, picked, vectors, last_id)
        read += len(rows)
        saved += len(picked)
        if read % (PAGE_SIZE * 25) < PAGE_SIZE:
            print(f"{read} đọc, {saved} embed, {read / (time.monotonic() - started):.0f} dòng/giây", flush=True)
    print(f"xong lượt này: {read} đọc, {saved} embed trong {(time.monotonic() - started) / 60:.1f} phút")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=0, help="đọc tối đa ngần này comment lượt này")
    parser.add_argument("--restart", action="store_true", help="đọc lại từ đầu, dòng đã embed đúng thì bỏ qua")
    asyncio.run(main(parser.parse_args()))
