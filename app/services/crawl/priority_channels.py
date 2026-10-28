"""Kênh ưu tiên (từ 20/10/2026): kênh trong danh sách booking (trang "Xuất thông tin theo kênh" › Nghiệm thu booking của
cinemark) mà D1 chưa có bài nào - Worker ghi vào bảng D1 priority_channels (api/lib/bookingReport.ts), bên này:

- crawl ngay (mỗi vòng scheduler, tối đa BATCH kênh) kênh TikTok: video của kênh qua Omar /posts (chỉ giữ video nhắc tới
  phim đang theo dõi, như omar_channels) + follower qua Omar /profile. Facebook / Threads không tự crawl (spider-hub lo).
- kênh TikTok yêu cầu trong KEEP_DAYS ngày luôn đứng đầu hàng cào kênh của các lượt Omar (omar_channels.load_channels).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.logging import get_logger
from app.repositories.d1.store import d1_query

logger = get_logger(__name__)

# Giống PRIORITY_CHANNELS_SQL ở api/lib/bookingReport.ts (cinemark).
TABLE_SQL = """CREATE TABLE IF NOT EXISTS priority_channels (
  platform TEXT NOT NULL,
  handle TEXT NOT NULL,
  movie_id TEXT,
  source TEXT,
  requested_at TEXT NOT NULL,
  crawled_at TEXT,
  result TEXT,
  PRIMARY KEY (platform, handle)
)"""
BATCH = 5
KEEP_DAYS = 30

_running = False
_table_ready = False


def is_running() -> bool:
    return _running


async def _ensure_table() -> None:
    global _table_ready
    if not _table_ready:
        await d1_query(TABLE_SQL)
        _table_ready = True


async def priority_tiktok_handles() -> list[str]:
    """Handle TikTok được yêu cầu trong KEEP_DAYS ngày, mới yêu cầu trước."""
    try:
        await _ensure_table()
        since = (datetime.now(tz=UTC) - timedelta(days=KEEP_DAYS)).isoformat()
        rows = await d1_query(
            "SELECT handle FROM priority_channels WHERE platform = 'tiktok' AND requested_at >= ? ORDER BY requested_at DESC",
            [since],
            quiet=True,
        )
    except Exception as exc:  # noqa: BLE001 - lỗi bảng ưu tiên không được chặn lượt cào kênh
        logger.warning("priority_channels_load_failed", error=str(exc)[:200])
        return []
    return [str(row["handle"]) for row in rows or []]


async def process_pending() -> dict[str, int]:
    """Crawl các kênh ưu tiên chưa crawl (cũ nhất trước, tối đa BATCH), ghi crawled_at + kết quả."""
    global _running
    if _running:
        return {"skipped": 1}
    _running = True
    try:
        await _ensure_table()
        rows = await d1_query(
            "SELECT platform, handle FROM priority_channels WHERE platform = 'tiktok' AND crawled_at IS NULL ORDER BY requested_at LIMIT ?",
            [BATCH],
            quiet=True,
        )
        if not rows:
            return {"pending": 0}
        from app.services.crawl import channel_profiles
        from app.services.omar.omar_channels import crawl_channel_once

        by_platform: dict[str, list[str]] = {}
        for row in rows:
            by_platform.setdefault(str(row["platform"]), []).append(str(row["handle"]))
        results: dict[tuple[str, str], dict[str, Any]] = {}
        for handle in by_platform.get("tiktok", []):
            stats = await crawl_channel_once(handle)
            results[("tiktok", handle)] = {
                "videos": int(stats.get("channel_videos") or 0),
                "matched": int(stats.get("channel_matched") or 0),
                **({"error": stats["error"]} if stats.get("error") else {}),
            }
        if by_platform.get("tiktok"):
            await channel_profiles.refresh_tiktok_handles(by_platform["tiktok"])
        now = datetime.now(tz=UTC).isoformat()
        for (platform, handle), result in results.items():
            await d1_query(
                "UPDATE priority_channels SET crawled_at = ?, result = ? WHERE platform = ? AND handle = ?",
                [now, json.dumps(result, ensure_ascii=False)[:500], platform, handle],
            )
        logger.info("priority_channels_processed", channels=len(results), by_platform={k: len(v) for k, v in by_platform.items()})
        return {"processed": len(results)}
    except Exception as exc:  # noqa: BLE001 - chạy nền từ scheduler; vòng sau thử lại
        logger.error("priority_channels_failed", error=str(exc)[:300])
        return {"failed": 1}
    finally:
        _running = False
