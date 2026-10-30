"""Chủ đề riêng từng phim (Social Topic bước 4) trên D1 - chỉ phần NHỎ mà Worker cinemark cần đọc:
danh sách chủ đề và số liệu theo mốc đo. Gán comment -> chủ đề nằm ở SQLite cục bộ
(app/repositories/local/topic_state.py) để không ghi hàng chục nghìn dòng lên D1."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from app.clients.d1 import d1_query
from app.repositories.d1.posts import RELEVANT_POST_SQL

_ready = False

from dataclasses import dataclass


@dataclass
class MovieTopic:
    id: str
    movie_id: str
    name: str
    summary: str | None
    centroid: list[float]
    comment_count: int
    samples: list[str]
    status: str
    name_locked: bool
    first_seen_slot: str | None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> MovieTopic:
        return cls(
            id=r["id"], movie_id=r["movie_id"], name=r["name"], summary=r.get("summary"),
            centroid=json.loads(r["centroid"]), comment_count=int(r["comment_count"] or 0),
            samples=json.loads(r.get("samples") or "[]"), status=r["status"],
            name_locked=bool(r.get("name_locked")), first_seen_slot=r.get("first_seen_slot"),
        )

async def ensure_topic_tables() -> None:
    """Tạo bảng chủ đề trên D1 nếu chưa có."""
    global _ready
    if _ready:
        return
    for sql in (
        """
        CREATE TABLE IF NOT EXISTS movie_topics (
            id TEXT PRIMARY KEY,
            movie_id TEXT NOT NULL,
            name TEXT NOT NULL,
            summary TEXT,
            centroid TEXT NOT NULL,
            comment_count INTEGER NOT NULL DEFAULT 0,
            samples TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            marged_info TEXT,
            name_locked INTEGER NOT NULL DEFAULT 0,
            first_seen_slot TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_movie_topics_movie_id ON movie_topics (movie_id, status)",
        """
        CREATE TABLE IF NOT EXISTS topics_snapshots (
            movie_id TEXT NOT NULL,
            measured_for TEXT NOT NULL,
            topic_id TEXT NOT NULL,
            comments INTEGER NOT NULL,
            new_comments INTEGER NOT NULL,
            positive INTEGER NOT NULL,
            negative INTEGER NOT NULL,
            PRIMARY KEY (movie_id, measured_for, topic_id)
        )
        """
    ):
        await d1_query(sql)
    _ready = True
    
async def list_topics(movie_id: str, *, include_hidden: bool = False) -> list[dict[str, Any]]:
    """Danh sách chủ đề của phim, kèm số liệu đo mới nhất."""
    await ensure_topic_tables()
    where = "" if include_hidden else "AND status = 'active'"
    rows = await d1_query(f"SELECT * FROM movie_topics WHERE movie_id = ? {where} ORDER BY comment_count DESC", [movie_id])
    for r in rows:
        r["centroid"] = json.loads(r["centroid"])
        r["samples"] = json.loads(r["samples"]) if r["samples"] else []
        
    return [MovieTopic.from_row(r) for r in rows or []]

async def create_topic(movie_id: str, name: str, summary: str, centroid: list[float], count: int, samples: list[str], slot: str | None) -> str:
    await ensure_topic_tables()
    topic_id = f"topic_{uuid.uuid4()}"
    now = datetime.now(UTC).isoformat()
    await d1_query(
        """
        INSERT INTO movies_topics (id, movie_id, name, summary, centroid, comment_count, ssmples, first_seen_slot, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
        """,
        [topic_id, movie_id, name, summary, json.dumps([round(v, 5) for v in centroid]), count, json.dumps(samples,ensure_ascii=False), slot, now, now],
    )
    
    return topic_id

async def update_topic_stats(topic_id: str, centroid: list[float], count: int, samples: list[str]) -> None:
    await d1_query(
        "UPDATE movie_topics SET centroid = ?, comment_count = ?, samples = ?, updated_at = ? WHERE id = ?",
        [json.dumps([round(v, 5) for v in centroid]), count, json.dumps(samples, ensure_ascii=False),
         datetime.now(UTC).isoformat(), topic_id],
    )
    
async def save_snapshot(movie_id: str, measured_for: str, rows: list[dict[str, Any]]):
    """rows: [{topic_id, comments, new_comments, positive, negative}] - ghi đè nếu chạy lại cùng mốc."""
    await ensure_topic_tables()
    for r in rows:
        await d1_query(
            """INSERT INTO topic_snapshots (movie_id, measured_for, topic_id, comments, new_comments, positive, negative)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (movie_id, measured_for, topic_id) DO UPDATE SET comments = excluded.comments,
                 new_comments = excluded.new_comments, positive = excluded.positive, negative = excluded.negative""",
            [movie_id, measured_for, r["topic_id"], r["comments"], r["new_comments"], r["positive"], r["negative"]],
        )
        
async def fetch_movie_comments(movie_id: str, *, seen_after: str = "", page_size: int = 2000) -> list[dict[str, Any]]:
    """Comment của bài liên quan tới phim, xuất hiện SAU `seen_after` (rỗng = tất cả). `seen_at` = first_seen_at,
    dòng cũ hơn cột đó (trước 28/10/2026) lấy posted_at - đủ gần để chia comment vào đúng mốc đo."""
    seen_at = "COALESCE(c.first_seen_at, c.posted_at, c.scraped_at)"
    rows: list[dict[str, Any]] = []
    last_id = ""
    while True:
        page = await d1_query(
            f"""SELECT c.id, c.message, c.sentiment, c.author_id, {seen_at} AS seen_at
                FROM comments c JOIN posts p ON p.id = c.post_id
                WHERE p.movie_id = ?1 AND {RELEVANT_POST_SQL} AND c.id > ?2 AND {seen_at} > ?3
                  AND length(c.message) >= 15
                ORDER BY c.id LIMIT {page_size}""",
            [movie_id, last_id, seen_after],
            timeout=120.0,
        )
        if not page:
            return rows
        rows += page
        last_id = page[-1]["id"]
