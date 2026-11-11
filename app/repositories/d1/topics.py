"""Chủ đề riêng từng phim (Social Topic bước 4) trên D1 - chỉ phần NHỎ mà Worker cinemark cần đọc:
danh sách chủ đề và số liệu theo mốc đo. Gán comment -> chủ đề nằm ở SQLite cục bộ
(app/repositories/local/topic_state.py) để không ghi hàng chục nghìn dòng lên D1."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Column, Index, Integer, Table, Text, func, select, text, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateIndex, CreateTable

from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec, metadata
from app.repositories.d1.tables import comments, posts

topics = Table(
    "movie_topics",
    metadata,
    Column("id", Text, primary_key=True),
    Column("movie_id", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("summary", Text),
    Column("centroid", Text, nullable=False),
    Column("comment_count", Integer, nullable=False, server_default=text("0")),
    Column("samples", Text),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("merged_info", Text),
    Column("name_locked", Integer, nullable=False, server_default=text("0")),
    Column("first_seen_slot", Text),
    Column("created_at", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    Index("idx_movie_topics_movie_id", "movie_id", "status"),
)

snapshots = Table(
    "topic_snapshots",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("measured_for", Text, primary_key=True),
    Column("topic_id", Text, primary_key=True),
    Column("comments", Integer, nullable=False),
    Column("new_comments", Integer, nullable=False),
    Column("positive", Integer, nullable=False),
    Column("negative", Integer, nullable=False),
)

_SNAPSHOT_STATS = ("comments", "new_comments", "positive", "negative")
# D1 nhận tối đa 100 tham số mỗi câu SQL; snapshot có 7 cột -> 14 dòng mỗi lô.
_SNAPSHOT_BATCH = 100 // len(snapshots.columns)
_ready = False


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
            id=r["id"],
            movie_id=r["movie_id"],
            name=r["name"],
            summary=r.get("summary"),
            centroid=json.loads(r["centroid"]),
            comment_count=int(r["comment_count"] or 0),
            samples=json.loads(r.get("samples") or "[]"),
            status=r["status"],
            name_locked=bool(r.get("name_locked")),
            first_seen_slot=r.get("first_seen_slot"),
        )


def _vector(values: list[float]) -> str:
    return json.dumps([round(v, 5) for v in values])


async def ensure_topic_tables() -> None:
    """Tạo bảng chủ đề trên D1 nếu chưa có."""
    global _ready
    if _ready:
        return
    for ddl in (
        CreateTable(topics, if_not_exists=True),
        *(CreateIndex(index, if_not_exists=True) for index in topics.indexes),
        CreateTable(snapshots, if_not_exists=True),
    ):
        await d1_exec(ddl)
    _ready = True


async def list_topics(movie_id: str, *, include_hidden: bool = False) -> list[MovieTopic]:
    """Danh sách chủ đề của phim, nhiều comment trước."""
    await ensure_topic_tables()
    stmt = select(topics).where(topics.c.movie_id == movie_id).order_by(topics.c.comment_count.desc())
    if not include_hidden:
        stmt = stmt.where(topics.c.status == "active")
    return [MovieTopic.from_row(r) for r in await d1_exec(stmt) or []]


async def create_topic(
    movie_id: str, name: str, summary: str, centroid: list[float], count: int, samples: list[str], slot: str | None
) -> str:
    await ensure_topic_tables()
    topic_id = f"topic_{uuid.uuid4()}"
    now = datetime.now(UTC).isoformat()
    await d1_exec(
        insert(topics).values(
            id=topic_id,
            movie_id=movie_id,
            name=name,
            summary=summary,
            centroid=_vector(centroid),
            comment_count=count,
            samples=json.dumps(samples, ensure_ascii=False),
            first_seen_slot=slot,
            created_at=now,
            updated_at=now,
        )
    )
    return topic_id


async def update_topic_stats(topic_id: str, centroid: list[float], count: int, samples: list[str]) -> None:
    await ensure_topic_tables()
    await d1_exec(
        update(topics)
        .where(topics.c.id == topic_id)
        .values(
            centroid=_vector(centroid),
            comment_count=count,
            samples=json.dumps(samples, ensure_ascii=False),
            updated_at=datetime.now(UTC).isoformat(),
        )
    )


async def save_snapshot(movie_id: str, measured_for: str, rows: list[dict[str, Any]]) -> None:
    """rows: [{topic_id, comments, new_comments, positive, negative}] - ghi đè nếu chạy lại cùng mốc."""
    await ensure_topic_tables()
    for start in range(0, len(rows), _SNAPSHOT_BATCH):
        values = [
            {"movie_id": movie_id, "measured_for": measured_for, "topic_id": r["topic_id"]}
            | {key: r[key] for key in _SNAPSHOT_STATS}
            for r in rows[start : start + _SNAPSHOT_BATCH]
        ]
        stmt = insert(snapshots).values(values)
        await d1_exec(
            stmt.on_conflict_do_update(
                index_elements=[snapshots.c.movie_id, snapshots.c.measured_for, snapshots.c.topic_id],
                set_={key: stmt.excluded[key] for key in _SNAPSHOT_STATS},
            )
        )


async def fetch_movie_comments(movie_id: str, *, seen_after: str = "", page_size: int = 2000) -> list[dict[str, Any]]:
    """Comment của bài liên quan tới phim, xuất hiện SAU `seen_after` (rỗng = tất cả). `seen_at` = first_seen_at,
    dòng cũ hơn cột đó lấy posted_at - đủ gần để chia comment vào đúng mốc đo."""
    c, p = comments.alias("c"), posts.alias("p")
    seen_at = func.coalesce(c.c.first_seen_at, c.c.posted_at, c.c.scraped_at)
    query = (
        select(c.c.id, c.c.message, c.c.sentiment, c.c.author_id, seen_at.label("seen_at"))
        .select_from(c.join(p, p.c.id == c.c.post_id))
        .where(p.c.movie_id == movie_id, relevant_post(p), seen_at > seen_after, func.length(c.c.message) >= 15)
        .order_by(c.c.id)
        .limit(page_size)
    )
    rows: list[dict[str, Any]] = []
    last_id = ""
    while True:
        page = await d1_exec(query.where(c.c.id > last_id), timeout=120.0)
        if not page:
            return rows
        rows += page
        last_id = page[-1]["id"]
