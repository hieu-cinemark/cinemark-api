"""Transcript (phụ đề) video - bảng D1 post_transcripts (16/11/2026).

Hiện chỉ YouTube: spider-hub (consumer/jobs/youtube.py) lấy phụ đề cho video mới, mỗi video một lần, gửi kèm trong message
raw_posts (trường "transcript"); persist_post ghi vào đây. Bảng riêng thay vì cột của posts: transcript có thể dài hàng chục
nghìn ký tự (spider-hub cắt ở 30.000), mà posts bị quét nhiều. Nối với posts qua (platform, external_id).

Dùng: lọc liên quan lúc ingest (ingest_consumer.main - phim "chặt" có tín hiệu phim trong transcript thì giữ), phân tích sau
này; bài cũ lấy bù bằng scripts/tools/backfill_youtube_transcripts.py.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Column, Integer, Table, Text, func, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateTable

from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, json_values, metadata

logger = get_logger(__name__)

post_transcripts = Table(
    "post_transcripts",
    metadata,
    Column("platform", Text, primary_key=True),
    Column("external_id", Text, primary_key=True),
    Column("language", Text),
    Column("generated", Integer),
    Column("text", Text, nullable=False),
    Column("char_count", Integer),
    Column("fetched_at", Text, nullable=False),
)

_ready = False
_lock = asyncio.Lock()


async def ensure_table() -> None:
    global _ready
    if _ready:
        return
    async with _lock:
        if not _ready and await d1_exec(CreateTable(post_transcripts, if_not_exists=True), quiet=True) is not None:
            _ready = True


def transcript_text(value: Any) -> str:
    """Nội dung transcript trong message / draft (dict {text, ...}), hoặc chuỗi rỗng."""
    return str(value.get("text") or "").strip() if isinstance(value, dict) else ""


# Đoạn đầu đủ cho lọc liên quan: video về phim nói tên phim sớm; video dài (truyện audio) dễ lẫn chữ "phim" ở đâu đó.
CONTEXT_CHARS = 3000


def transcript_has_film_context(draft: dict[str, Any], movie: dict[str, Any] | None) -> bool:
    """Đoạn đầu transcript của bài có tín hiệu gắn đúng phim (relevance_rules.has_film_context)."""
    from app.services.relevance.relevance_rules import has_film_context

    text = transcript_text(draft.get("transcript"))
    return bool(text) and has_film_context(text[:CONTEXT_CHARS], movie)


async def save_transcript(platform: str, external_id: str, transcript: dict[str, Any]) -> bool:
    text = transcript_text(transcript)
    if not text or not external_id:
        return False
    await ensure_table()
    stmt = insert(post_transcripts).values(
        platform=platform,
        external_id=str(external_id),
        language=transcript.get("language"),
        generated=int(bool(transcript.get("generated"))),
        text=text,
        char_count=int(transcript.get("char_count") or len(text)),
        fetched_at=datetime.now(tz=UTC).isoformat(),
    )
    new = stmt.excluded
    stmt = stmt.on_conflict_do_update(
        index_elements=[post_transcripts.c.platform, post_transcripts.c.external_id],
        set_={name: new[name] for name in ("language", "generated", "text", "char_count", "fetched_at")},
    )
    if await d1_exec(stmt) is None:
        logger.warning("transcript_save_failed", platform=platform, external_id=external_id)
        return False
    return True


async def load_texts(platform: str, external_ids: list[str], chars: int) -> dict[str, str]:
    """{external_id: `chars` ký tự đầu của transcript}. Lỗi / chưa có bảng thì rỗng (chỗ gọi vẫn đọc tiêu đề)."""
    if not external_ids:
        return {}
    t = post_transcripts
    rows = await d1_exec(
        select(t.c.external_id, func.substr(t.c.text, 1, chars).label("text")).where(
            t.c.platform == platform, t.c.external_id.in_(json_values(external_ids))
        ),
        quiet=True,
    )
    return {row["external_id"]: row["text"] for row in rows or [] if row.get("text")}
