"""Kênh ưu tiên: kênh trong danh sách booking (trang "Xuất thông tin theo kênh" › Nghiệm thu booking của
cinemark) mà D1 chưa có bài nào - Worker ghi vào bảng D1 priority_channels (api/lib/bookingReport.ts), bên này:

- lấy ngay (mỗi vòng scheduler, tối đa BATCH kênh) follower của kênh TikTok qua Omar /profile. Facebook / Threads không tự
  crawl (spider-hub lo). Video của kênh không còn cào riêng qua Omar (chỉ hashtag + bình luận, 18/11/2026).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Column, Table, Text, select, update
from sqlalchemy.schema import CreateTable

from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, metadata

logger = get_logger(__name__)

# Giống PRIORITY_CHANNELS_SQL ở api/lib/bookingReport.ts (cinemark).
priority_channels = Table(
    "priority_channels",
    metadata,
    Column("platform", Text, primary_key=True),
    Column("handle", Text, primary_key=True),
    Column("movie_id", Text),
    Column("source", Text),
    Column("requested_at", Text, nullable=False),
    Column("crawled_at", Text),
    Column("result", Text),
)
BATCH = 5
KEEP_DAYS = 30

_running = False
_table_ready = False


def is_running() -> bool:
    return _running


async def _ensure_table() -> None:
    global _table_ready
    if not _table_ready:
        await d1_exec(CreateTable(priority_channels, if_not_exists=True))
        _table_ready = True


async def process_pending() -> dict[str, int]:
    """Crawl các kênh ưu tiên chưa crawl (cũ nhất trước, tối đa BATCH), ghi crawled_at + kết quả."""
    global _running
    if _running:
        return {"skipped": 1}
    _running = True
    try:
        await _ensure_table()
        pc = priority_channels.c
        rows = await d1_exec(
            select(pc.platform, pc.handle)
            .where(pc.platform == "tiktok", pc.crawled_at.is_(None))
            .order_by(pc.requested_at)
            .limit(BATCH),
            quiet=True,
        )
        if not rows:
            return {"pending": 0}
        from app.services.crawl import channel_profiles

        by_platform: dict[str, list[str]] = {}
        for row in rows:
            by_platform.setdefault(str(row["platform"]), []).append(str(row["handle"]))
        results: dict[tuple[str, str], dict[str, Any]] = {}
        if by_platform.get("tiktok"):
            found = await channel_profiles.refresh_tiktok_handles(by_platform["tiktok"])
            for handle in by_platform["tiktok"]:
                results[("tiktok", handle)] = {"followers_found": found}
        now = datetime.now(tz=UTC).isoformat()
        for (platform, handle), result in results.items():
            await d1_exec(
                update(priority_channels)
                .where(pc.platform == platform, pc.handle == handle)
                .values(crawled_at=now, result=json.dumps(result, ensure_ascii=False)[:500])
            )
        logger.info(
            "priority_channels_processed",
            channels=len(results),
            by_platform={k: len(v) for k, v in by_platform.items()},
        )
        return {"processed": len(results)}
    except Exception as exc:  # noqa: BLE001 - chạy nền từ scheduler; vòng sau thử lại
        logger.error("priority_channels_failed", error=str(exc)[:300])
        return {"failed": 1}
    finally:
        _running = False
