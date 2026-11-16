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

from app.clients.d1 import d1_query
from app.core.logging import get_logger

logger = get_logger(__name__)

TABLE_SQL = """CREATE TABLE IF NOT EXISTS post_transcripts (
  platform TEXT NOT NULL,
  external_id TEXT NOT NULL,
  language TEXT,
  generated INTEGER,
  text TEXT NOT NULL,
  char_count INTEGER,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (platform, external_id)
)"""

_ready = False
_lock = asyncio.Lock()


async def ensure_table() -> None:
    global _ready
    if _ready:
        return
    async with _lock:
        if not _ready and await d1_query(TABLE_SQL, quiet=True) is not None:
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
    result = await d1_query(
        "INSERT INTO post_transcripts (platform, external_id, language, generated, text, char_count, fetched_at)"
        " VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
        " ON CONFLICT(platform, external_id) DO UPDATE SET language = excluded.language,"
        " generated = excluded.generated, text = excluded.text, char_count = excluded.char_count,"
        " fetched_at = excluded.fetched_at",
        [
            platform,
            str(external_id),
            transcript.get("language"),
            int(bool(transcript.get("generated"))),
            text,
            int(transcript.get("char_count") or len(text)),
            datetime.now(tz=UTC).isoformat(),
        ],
    )
    if result is None:
        logger.warning("transcript_save_failed", platform=platform, external_id=external_id)
        return False
    return True
