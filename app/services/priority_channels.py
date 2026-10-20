"""Kênh ưu tiên (từ 20/10/2026): kênh trong danh sách booking (trang "Xuất thông tin theo kênh" › Nghiệm thu booking của
cinemark) mà D1 chưa có bài nào - Worker ghi vào bảng D1 priority_channels (api/lib/bookingReport.ts), bên này:

- crawl ngay (mỗi vòng scheduler, tối đa BATCH kênh): TikTok - video của kênh qua Omar /posts (chỉ giữ video nhắc tới phim
  đang theo dõi, như omar_channels) + follower qua Omar /profile; Facebook / Threads - follower, tên kênh qua trang công
  khai (channel_profiles.refresh_public_followers). Threads còn xếp thêm một job spider-hub type=channel_posts
  (threads_channel_posts - bài gần đây của kênh nhắc tới phim, vào D1 posts qua ingest với từ khoá Threads của phim).
  Chưa có crawler bài theo page cho Facebook.
- kênh TikTok yêu cầu trong KEEP_DAYS ngày luôn đứng đầu hàng cào kênh của các lượt Omar (omar_channels.load_channels).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.logging import get_logger
from app.services.d1 import d1_query

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
# Job channel_posts của Threads: số trang (10 thread/trang) và số ngày gần nhất - tài khoản Threads hiếm, chỉ cần bài quảng
# bá phim vừa đăng.
THREADS_POSTS_MAX_PAGES = 3
THREADS_POSTS_DAYS = 60

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


async def _threads_posts_request(movie_id: str) -> dict[str, Any] | None:
    """Tham số job channel_posts cho phim: từ khoá Threads của phim (ưu tiên đang bật, không phải hashtag - ingest chỉ nhận
    keyword_id đúng nền tảng, kể cả từ khoá đã tắt), tên phim và mọi hashtag đang bật của phim (mọi nền tảng) để spider
    chỉ giữ bài nhắc tới phim. None khi phim không có từ khoá Threads nào / phim đã tắt."""
    from app.clients.kafka import _match_terms_for_keyword

    rows = await d1_query(
        """
        SELECT k.id, k.keyword FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.movie_id = ? AND k.platform = 'threads' AND m.enabled = 1
        ORDER BY k.enabled DESC, (k.keyword LIKE '#%') ASC, k.created_at
        LIMIT 1
        """,
        [movie_id],
        quiet=True,
    )
    if not rows:
        return None
    keyword_id, keyword = str(rows[0]["id"]), str(rows[0]["keyword"] or "")
    tag_rows = await d1_query(
        "SELECT DISTINCT keyword FROM keywords WHERE movie_id = ? AND enabled = 1 AND keyword LIKE '#%'",
        [movie_id],
        quiet=True,
    )
    hashtags = sorted({tag for row in tag_rows or [] if (tag := str(row["keyword"] or "").strip().lstrip("#"))})
    return {
        "keyword_id": keyword_id,
        # Từ khoá dạng hashtag đã nằm trong match_hashtags (khớp nguyên hashtag) - không so chuỗi con thêm.
        "keyword": None if keyword.startswith("#") else keyword,
        "match_terms": await _match_terms_for_keyword(keyword_id),
        "match_hashtags": hashtags,
    }


async def _queue_threads_posts(handle: str, movie_id: str | None) -> str:
    """Xếp job channel_posts cho một kênh Threads. Trả trạng thái ghi vào result: queued / no_movie / no_threads_keyword /
    publish_failed / error."""
    if not movie_id:
        return "no_movie"
    try:
        from app.clients.kafka import publish_threads_channel_posts_request

        request = await _threads_posts_request(movie_id)
        if request is None:
            logger.info("priority_threads_posts_no_keyword", handle=handle, movie_id=movie_id)
            return "no_threads_keyword"
        ok = await publish_threads_channel_posts_request(
            username=handle, max_pages=THREADS_POSTS_MAX_PAGES, days=THREADS_POSTS_DAYS, **request
        )
    except Exception as exc:  # noqa: BLE001 - lỗi xếp job không được chặn phần follower đã làm
        logger.warning("priority_threads_posts_failed", handle=handle, error=str(exc)[:200])
        return "error"
    logger.info("priority_threads_posts_queued" if ok else "priority_threads_posts_publish_failed", handle=handle)
    return "queued" if ok else "publish_failed"


async def process_pending() -> dict[str, int]:
    """Crawl các kênh ưu tiên chưa crawl (cũ nhất trước, tối đa BATCH), ghi crawled_at + kết quả."""
    global _running
    if _running:
        return {"skipped": 1}
    _running = True
    try:
        await _ensure_table()
        rows = await d1_query(
            "SELECT platform, handle, movie_id FROM priority_channels WHERE crawled_at IS NULL ORDER BY requested_at LIMIT ?",
            [BATCH],
            quiet=True,
        )
        if not rows:
            return {"pending": 0}
        from app.services import channel_profiles
        from app.services.omar_channels import crawl_channel_once

        by_platform: dict[str, list[str]] = {}
        movie_of: dict[tuple[str, str], str | None] = {}
        for row in rows:
            by_platform.setdefault(str(row["platform"]), []).append(str(row["handle"]))
            movie_of[(str(row["platform"]), str(row["handle"]))] = row.get("movie_id") or None
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
        for platform in ("facebook", "threads"):
            handles = by_platform.get(platform) or []
            if handles:
                summary = await channel_profiles.refresh_public_followers(platform, len(handles), handles=handles)
                for handle in handles:
                    results[(platform, handle)] = {"profile": summary}
        for handle in by_platform.get("threads", []):
            # crawled_at vẫn được ghi ngay bên dưới: "đã crawl" ở đây nghĩa là đã xếp job - spider-hub chạy nó sau (hàng
            # Threads từng-cái-một), bài về D1 qua ingest. Job lỗi (session chết...) không tự xếp lại: xoá crawled_at của
            # dòng này để vòng sau xếp lại.
            results[("threads", handle)]["posts_job"] = await _queue_threads_posts(
                handle, movie_of.get(("threads", handle))
            )
        now = datetime.now(tz=UTC).isoformat()
        for (platform, handle), result in results.items():
            await d1_query(
                "UPDATE priority_channels SET crawled_at = ?, result = ? WHERE platform = ? AND handle = ?",
                [now, json.dumps(result, ensure_ascii=False)[:500], platform, handle],
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
