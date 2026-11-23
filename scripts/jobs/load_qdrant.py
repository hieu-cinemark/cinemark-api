from __future__ import annotations

import argparse
import asyncio
import json
import re
import sqlite3
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy import select

from app.ai.local.embedding import DIM
from app.ai.retrieval.base import Document
from app.ai.retrieval.factory import new_client
from app.ai.retrieval.qdrant_store import COLLECTION, QdrantVectorStore, ensure_collection
from app.repositories.d1.sql import d1_exec, json_values
from app.repositories.d1.tables import comments as comments_table

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DB_PATH = DATA_DIR / "comment_embeddings.sqlite"
CURSOR_PATH = DATA_DIR / "qdrant_cursor.json"
PAGE_SIZE = 2000
MIN_WORDS = 4
TAG_RE = re.compile(r"\[[^\]]{1,30}\]")  # [Sticker], [Ấm lòng]...: nhãn của nền tảng, không phải ý kiến
OVERLOAD_PAUSE_SECONDS = 60.0
Fetch = Callable[[list[str]], Awaitable[dict[str, dict[str, Any]]]]


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    # Phân trang theo (movie_id, comment_id): thiếu chỉ mục này mỗi trang phải sắp lại toàn bộ comment của phim.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_comment_embeddings_movie_comment ON comment_embeddings (movie_id, comment_id)"
    )
    return conn


def read_page(conn, movie_id: str, after: str, size: int) -> list[tuple[str, np.ndarray]]:
    rows = conn.execute(
        "SELECT comment_id, embedding FROM comment_embeddings WHERE movie_id = ? AND comment_id > ? ORDER BY comment_id LIMIT ?",
        (movie_id, after, size),
    ).fetchall()
    return [(cid, np.frombuffer(blob, dtype=np.float16).astype(np.float32)) for cid, blob in rows]


def iso_utc(value: Any) -> str | None:
    """Thời gian hợp lệ cho chỉ mục DATETIME của Qdrant, hoặc None (thiếu/hỏng thì bỏ khỏi payload)."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC).isoformat()


def is_meaningful(text: str, min_chars: int) -> bool:
    """Đủ nội dung để làm nguồn trả lời: bỏ nhãn sticker/emoji rồi còn ít nhất min_chars ký tự và MIN_WORDS từ."""
    body = TAG_RE.sub(" ", text).strip()
    return len(body) >= min_chars and len(body.split()) >= MIN_WORDS


def build_documents(movie_id, vector_rows, comments, *, min_chars: int) -> tuple[list[Document], list[list[float]]]:
    """Ghép vector (SQLite) với nội dung/thuộc tính (D1). Bỏ comment không còn trong D1, quá ngắn, hoặc vector sai số chiều."""
    docs, vectors = [], []
    for comment_id, vector in vector_rows:
        row = comments.get(comment_id)
        text = ((row or {}).get("message") or "").strip()
        if row is None or not is_meaningful(text, min_chars) or vector.shape[0] != DIM:
            continue
        metadata = {
            "movie_id": movie_id,
            "platform": row.get("platform"),
            "sentiment": row.get("sentiment"),
            "reactions": int(row.get("reactions_count") or 0),
            "posted_at": iso_utc(row.get("posted_at")),
        }
        docs.append(Document(comment_id, text, {k: v for k, v in metadata.items() if v is not None}))
        vectors.append(vector.tolist())
    return docs, vectors


async def fetch_comments(ids: list[str]) -> dict[str, dict[str, Any]]:
    c = comments_table.c
    stmt = select(c.id, c.message, c.platform, c.posted_at, c.sentiment, c.reactions_count).where(
        c.id.in_(json_values(ids))
    )
    while True:  # D1 quá tải thì nghỉ rồi thử lại, như embed_comments.py
        rows = await d1_exec(stmt, timeout=60.0, quiet=True)
        if rows is not None:
            return {row["id"]: row for row in rows}
        print(f"đọc D1 lỗi, nghỉ {OVERLOAD_PAUSE_SECONDS:.0f} giây", flush=True)
        await asyncio.sleep(OVERLOAD_PAUSE_SECONDS)


def load_cursor(path: Path = CURSOR_PATH) -> dict[str, str]:
    try:
        return json.loads(path.read_text())
    except OSError, ValueError:
        return {}


def save_cursor(cursor: dict[str, str], path: Path = CURSOR_PATH) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cursor))
    tmp.replace(path)  # ghi nguyên tử: dừng giữa chừng không để lại file hỏng


async def load_movie(
    conn,
    store: QdrantVectorStore,
    movie_id: str,
    *,
    after: str = "",
    limit: int | None = None,
    min_chars: int = 25,
    page_size: int = PAGE_SIZE,
    dry_run: bool = False,
    fetch: Fetch = fetch_comments,
    on_page: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Nạp một phim theo trang. Trả {"seen": số vector đã đọc, "loaded": số điểm đã (hoặc sẽ, nếu dry_run) ghi}."""
    seen = loaded = 0
    while limit is None or seen < limit:
        size = page_size if limit is None else min(page_size, limit - seen)
        page = read_page(conn, movie_id, after, size)
        if not page:
            break
        docs, vectors = build_documents(movie_id, page, await fetch([cid for cid, _ in page]), min_chars=min_chars)
        if docs and not dry_run:
            await store.upsert(docs, vectors)
        seen, loaded, after = seen + len(page), loaded + len(docs), page[-1][0]
        if on_page and not dry_run:
            on_page(after)
    return {"seen": seen, "loaded": loaded}


async def main(args: argparse.Namespace) -> None:
    conn, client = connect(), new_client()
    try:
        await ensure_collection(client)
        store = QdrantVectorStore(client)
        movie_ids = (
            [args.movie_id]
            if args.movie_id
            else [
                r[0]
                for r in conn.execute("SELECT DISTINCT movie_id FROM comment_embeddings WHERE movie_id IS NOT NULL")
            ]
        )
        cursor = {} if args.reset_cursor else load_cursor()
        for movie_id in movie_ids:
            started = time.monotonic()

            def remember(last_id: str, movie_id: str = movie_id) -> None:
                cursor[movie_id] = last_id
                save_cursor(cursor)

            stats = await load_movie(
                conn,
                store,
                movie_id,
                after=cursor.get(movie_id, ""),
                limit=args.limit,
                min_chars=args.min_chars,
                dry_run=args.dry_run,
                on_page=remember,
            )
            print(
                f"{movie_id}: đọc {stats['seen']}, nạp {stats['loaded']} trong {time.monotonic() - started:.0f}s",
                flush=True,
            )
        print(f"collection {COLLECTION}: {(await client.get_collection(COLLECTION)).points_count} điểm", flush=True)
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--movie-id")
    p.add_argument("--limit", type=int, help="Số vector tối đa đọc mỗi phim trong lần chạy này")
    p.add_argument("--min-chars", type=int, default=25)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--reset-cursor", action="store_true")
    asyncio.run(main(p.parse_args()))
