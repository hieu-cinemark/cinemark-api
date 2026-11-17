"""Bảng attack_signals (D1 scraper): mỗi lần tính một dòng, giữ KEEP_DAYS ngày (docs mục 2.6). Worker cinemark đọc thẳng
bảng này cho tab Attack Detection (apps/api/src/services/socialTopic/attack.ts) - đổi cột thì sửa cả hai bên."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Column, Index, Table, Text, delete, insert
from sqlalchemy.schema import CreateIndex, CreateTable

from app.repositories.d1.sql import d1_exec, metadata

KEEP_DAYS = 60

attack_signals = Table(
    "attack_signals",
    metadata,
    Column("id", Text, primary_key=True),
    Column("movie_id", Text, nullable=False),
    Column("computed_at", Text, nullable=False),
    Column("window_start", Text, nullable=False),
    Column("window_end", Text, nullable=False),
    Column("status", Text, nullable=False),  # insufficient | clear | suspicious | coordinated
    Column("confidence", Text, nullable=False),  # high | medium | low
    Column("reasons_json", Text, nullable=False),  # ["câu giải thích", ...]
    Column("signals_json", Text, nullable=False),  # [Signal.as_dict(), ...]
    Column("summary", Text),  # câu tóm tắt do AI viết (dấu hiệu E) - chưa làm, luôn NULL
)
_index = Index("idx_attack_signals_movie", attack_signals.c.movie_id, attack_signals.c.computed_at)

_ready = False


async def ensure_table() -> bool:
    global _ready
    if not _ready:
        ok = await d1_exec(CreateTable(attack_signals, if_not_exists=True), quiet=True) is not None
        ok = ok and await d1_exec(CreateIndex(_index, if_not_exists=True), quiet=True) is not None
        _ready = ok
    return _ready


async def save(result: dict[str, Any]) -> bool:
    """result: {movie_id, computed_at, window_start, window_end, status, confidence, reasons, signals}."""
    if not await ensure_table():
        return False
    row = {
        "id": f"atk_{uuid.uuid4().hex}",
        "movie_id": result["movie_id"],
        "computed_at": result["computed_at"],
        "window_start": result["window_start"],
        "window_end": result["window_end"],
        "status": result["status"],
        "confidence": result["confidence"],
        "reasons_json": json.dumps(result["reasons"], ensure_ascii=False),
        "signals_json": json.dumps(result["signals"], ensure_ascii=False),
        "summary": None,
    }
    return await d1_exec(insert(attack_signals).values(**row)) is not None


async def prune(now: datetime | None = None) -> None:
    cutoff = ((now or datetime.now(tz=UTC)) - timedelta(days=KEEP_DAYS)).isoformat(timespec="seconds")
    await d1_exec(delete(attack_signals).where(attack_signals.c.computed_at < cutoff), quiet=True)
