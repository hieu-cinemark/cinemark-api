"""Đọc dữ liệu cho Attack Detection: số theo giờ (movie_comment_hourly), mốc sự kiện, bình luận chê gần đây (D1) và
embedding (file SQLite do scripts/jobs/embed_comments.py ghi trên cùng máy - VPS: timer cinemark-embed)."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy import func, select

from app.repositories.d1.comment_hourly import movie_comment_hourly, vn_hour
from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import movie_measure_settings, movies
from app.repositories.d1.tables import posts as posts_table

# Cùng file với scripts/jobs/embed_comments.py (DB_PATH).
EMBEDDINGS_PATH = Path(__file__).resolve().parents[3] / "data" / "comment_embeddings.sqlite"
EMBED_DIM = 384
# Trần số bình luận chê đọc mỗi lượt (48 giờ của phim đang nóng có thể vài nghìn).
NEGATIVE_LIMIT = 6000


async def load_hours(movie_id: str, now: datetime, days: int = 16) -> list[dict[str, Any]]:
    """[{"hour", "total", "negative"}] liên tục từng giờ VN tới hết giờ của `now`, giờ trống = 0."""
    first = now - timedelta(days=days)
    h = movie_comment_hourly
    rows = await d1_exec(
        select(h.c.hour, h.c.total, h.c.negative).where(h.c.movie_id == movie_id, h.c.hour >= vn_hour(first)),
        timeout=30.0,
    )
    by_hour = {r["hour"]: r for r in rows or []}
    out, cursor = [], first.replace(minute=0, second=0, microsecond=0)
    while cursor <= now:
        key = vn_hour(cursor)
        row = by_hour.get(key) or {}
        out.append({"hour": key, "total": int(row.get("total") or 0), "negative": int(row.get("negative") or 0)})
        cursor += timedelta(hours=1)
    return out


async def load_events(movie_id: str) -> list[str]:
    """Mốc sự kiện của phim (giờ VN 'YYYY-MM-DDT00'): ngày chiếu sớm, ngày khởi chiếu. Mốc tay chưa có (docs mục 2.9)."""
    m, s = movies.alias("m"), movie_measure_settings.alias("s")
    rows = await d1_exec(
        select(m.c.released_at, s.c.premiere_date)
        .select_from(m.outerjoin(s, s.c.movie_id == m.c.id))
        .where(m.c.id == movie_id)
        .limit(1)
    )
    row = (rows or [{}])[0]
    days = {str(v)[:10] for v in (row.get("premiere_date"), row.get("released_at")) if v}
    return sorted(f"{day}T00" for day in days)


async def load_negative_comments(movie_id: str, now: datetime, hours: int = 48) -> list[dict[str, Any]]:
    """Bình luận chê có nội dung (bỏ nhãn 'rule') của bài liên quan tới phim, đăng trong `hours` giờ trước `now`."""
    c, p = comments_table.alias("c"), posts_table.alias("p")
    since = (now - timedelta(hours=hours)).astimezone(UTC).isoformat(timespec="seconds")
    until = now.astimezone(UTC).isoformat(timespec="seconds")
    rows = await d1_exec(
        select(
            c.c.id,
            c.c.author_id,
            c.c.author_name,
            c.c.post_id,
            c.c.platform,
            c.c.posted_at,
            func.substr(c.c.message, 1, 600).label("message"),
            c.c.reactions_count.label("likes"),
            p.c.url.label("post_url"),
        )
        .select_from(c.join(p, p.c.id == c.c.post_id))
        .where(
            c.c.posted_at >= since,
            c.c.posted_at < until,
            p.c.movie_id == movie_id,
            relevant_post(p),
            c.c.sentiment == "negative",
            func.coalesce(c.c.sentiment_source, "") != "rule",
        )
        .order_by(c.c.posted_at.desc())
        .limit(NEGATIVE_LIMIT),
        timeout=60.0,
    )
    return rows or []


def load_vectors(comment_ids: list[str], path: Path = EMBEDDINGS_PATH) -> np.ndarray | None:
    """(n, EMBED_DIM) cùng thứ tự comment_ids, dòng chưa có embedding là vector 0. Chưa có file thì None (chỉ gom trùng chữ)."""
    if not comment_ids or not path.exists():
        return None
    out = np.zeros((len(comment_ids), EMBED_DIM), dtype=np.float32)
    index = {cid: i for i, cid in enumerate(comment_ids)}
    conn = sqlite3.connect(path)
    try:
        for start in range(0, len(comment_ids), 500):  # SQLite giới hạn số tham số
            chunk = comment_ids[start : start + 500]
            marks = ",".join("?" * len(chunk))
            sql = f"SELECT comment_id, embedding FROM comment_embeddings WHERE comment_id IN ({marks})"
            for cid, blob in conn.execute(sql, chunk):
                vector = np.frombuffer(blob, dtype=np.float16).astype(np.float32)
                if vector.shape[0] == EMBED_DIM:
                    out[index[cid]] = vector
    finally:
        conn.close()
    return out


def coverage(hours: list[dict[str, Any]], negatives: list[dict[str, Any]]) -> dict[str, int]:
    """Độ phủ cho verdict: bình luận 7 ngày (từ số theo giờ) và số nền tảng có bình luận chê gần đây."""
    return {
        "comments_7d": sum(h["total"] for h in hours[-24 * 7 :]),
        "platforms": len({r.get("platform") for r in negatives if r.get("platform")}),
    }
