"""Trạng thái gom chủ đề trên máy chạy job (SQLite, mặc định data/topics.sqlite): comment nào thuộc chủ đề nào,
cache embedding, và mốc 'đã xử lý tới đâu' của từng phim. Mất file này thì job tính lại từ đầu - chậm nhưng không hỏng
dữ liệu D1 (chủ đề cũ vẫn giữ, comment được gán lại vào chúng)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np

DB_PATH = Path(__file__).resolve().parents[3] / "data" / "topics.sqlite"


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS comment_topics (
            comment_id TEXT PRIMARY KEY,
            movie_id TEXT NOT NULL,
            topic_id TEXT,              -- NULL = đã xét, chưa thuộc chủ đề nào (chờ đủ nhiều để thành chủ đề mới)
            similarity REAL,
            sentiment TEXT,
            first_seen_at TEXT NOT NULL,
            embedding BLOB NOT NULL     -- float16, để lần sau gom lại phần dư không phải tính lại
        );
        CREATE INDEX IF NOT EXISTS idx_ct_movie_topic ON comment_topics(movie_id, topic_id);
        CREATE INDEX IF NOT EXISTS idx_ct_movie_seen ON comment_topics(movie_id, first_seen_at);
        CREATE TABLE IF NOT EXISTS movie_cursor (
            movie_id TEXT PRIMARY KEY,
            last_first_seen_at TEXT NOT NULL   -- đã xử lý comment có first_seen_at tới mốc này
        );
        """
    )
    return conn


def get_cursor(conn: sqlite3.Connection, movie_id: str) -> str:
    row = conn.execute("SELECT last_first_seen_at FROM movie_cursor WHERE movie_id = ?", (movie_id,)).fetchone()
    return row[0] if row else ""


def set_cursor(conn: sqlite3.Connection, movie_id: str, value: str) -> None:
    conn.execute(
        "INSERT INTO movie_cursor VALUES (?, ?) ON CONFLICT(movie_id) DO UPDATE SET last_first_seen_at = excluded.last_first_seen_at",
        (movie_id, value),
    )
    conn.commit()


def save_assignments(conn: sqlite3.Connection, rows: list[dict], vectors: np.ndarray) -> None:
    """rows: [{id, movie_id, topic_id, similarity, sentiment, first_seen_at}] cùng thứ tự với vectors."""
    conn.executemany(
        """INSERT INTO comment_topics VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(comment_id) DO UPDATE SET topic_id = excluded.topic_id, similarity = excluded.similarity""",
        [
            (r["id"], r["movie_id"], r["topic_id"], r["similarity"], r.get("sentiment"), r["first_seen_at"],
             v.astype(np.float16).tobytes())
            for r, v in zip(rows, vectors, strict=True)
        ],
    )
    conn.commit()


def unassigned(conn: sqlite3.Connection, movie_id: str) -> tuple[list[str], np.ndarray]:
    """Comment chưa thuộc chủ đề nào (phần dư) + embedding - để gom thành chủ đề mới khi đủ nhiều."""
    rows = conn.execute(
        "SELECT comment_id, embedding FROM comment_topics WHERE movie_id = ? AND topic_id IS NULL", (movie_id,)
    ).fetchall()
    if not rows:
        return [], np.zeros((0, 0), dtype=np.float32)
    return [r[0] for r in rows], np.stack([np.frombuffer(r[1], dtype=np.float16).astype(np.float32) for r in rows])


def topic_counts(conn: sqlite3.Connection, movie_id: str, until: str, since: str = "") -> dict[str, dict[str, int]]:
    """Số comment theo chủ đề có first_seen_at trong (since, until] - dùng cho snapshot từng mốc."""
    out: dict[str, dict[str, int]] = {}
    for topic_id, sentiment, n in conn.execute(
        """SELECT topic_id, sentiment, COUNT(*) FROM comment_topics
           WHERE movie_id = ? AND topic_id IS NOT NULL AND first_seen_at > ? AND first_seen_at <= ?
           GROUP BY topic_id, sentiment""",
        (movie_id, since, until),
    ):
        c = out.setdefault(topic_id, {"comments": 0, "positive": 0, "negative": 0})
        c["comments"] += n
        if sentiment in ("positive", "negative"):
            c[sentiment] += n
    return out
