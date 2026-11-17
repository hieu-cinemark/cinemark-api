"""Bảng quyết định của AI khám phá nguồn cào (hashtag_ai_decisions): ghi quyết định, danh sách phim/từ khoá đang bật. Dùng chung cho khám phá câu truy vấn (query_discovery).

Tách từ hashtag_discovery.py khi bỏ phần AI duyệt hashtag TikTok (17/11/2026).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Column, Integer, Table, Text, select, text
from sqlalchemy.schema import CreateTable

from app.clients.d1 import d1_query
from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, d1_insert_many, metadata
from app.repositories.d1.tables import keywords, movies

logger = get_logger(__name__)


RECHECK_REJECTED_DAYS = 14

# Quyết định của AI cho từng tag/câu tìm (source: hashtag/query); dùng chung với query_discovery.py.
decisions_table = Table(
    "hashtag_ai_decisions",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("tag", Text, primary_key=True),
    Column("display", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("seen", Integer, nullable=False, server_default=text("0")),
    Column("verdict", Text),
    Column("confidence", Integer),
    Column("reason", Text),
    Column("status", Text, nullable=False),
    Column("keyword_id", Text),
    Column("decided_at", Text, nullable=False),
    # Cột BFS (thêm sau): tag được sinh từ từ khoá nào và ở tầng mấy.
    Column("parent_keyword_id", Text),
    Column("depth", Integer, nullable=False, server_default=text("0")),
)

# Thông tin phim đưa vào prompt.
ALL_KEYWORDS = select(*(keywords.c[name] for name in ("id", "movie_id", "platform", "keyword", "enabled")))

# Phim đang bật kèm thông tin đưa vào prompt.
ENABLED_MOVIES = select(
    *(
        movies.c[name]
        for name in ("id", "title", "slug", "director", "cast", "distributor", "released_at", "description")
    )
).where(movies.c.enabled == 1)


async def _ensure_table() -> None:
    await d1_exec(CreateTable(decisions_table, if_not_exists=True), quiet=True)
    # Bảng cũ thiếu cột BFS: ALTER (lỗi khi cột đã có - bỏ qua).
    for column in ("parent_keyword_id TEXT", "depth INTEGER NOT NULL DEFAULT 0"):
        await d1_query(f"ALTER TABLE hashtag_ai_decisions ADD COLUMN {column}", quiet=True)


async def _record(
    movie_id: str, cand: dict[str, Any], verdict: dict[str, Any] | None, status: str, keyword_id: str | None
) -> None:
    verdict = verdict or {}
    await d1_insert_many(
        decisions_table,
        [
            {
                "movie_id": movie_id,
                "tag": cand["tag"],
                "display": cand["display"],
                "source": cand["source"],
                "seen": cand.get("seen", 0),
                "verdict": verdict.get("verdict"),
                "confidence": verdict.get("confidence"),
                "reason": (verdict.get("reason") or "")[:300] or None,
                "status": status,
                "keyword_id": keyword_id,
                "decided_at": datetime.now(tz=UTC).isoformat(),
                "parent_keyword_id": cand.get("parent_keyword_id"),
                "depth": int(cand.get("depth") or 0),
            }
        ],
        conflict=["movie_id", "tag"],
    )
