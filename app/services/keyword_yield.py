"""Hiệu quả từng từ khoá Facebook/Threads và tự điều chỉnh (2026-10-15).

Đo 01-15/10/2026: một số từ khoá gần như chỉ đem về rác (Threads #QuyAnTang4 0%, Facebook "Mật Mã Đông Dương" 4%,
"Loạn Thế" 11%) trong khi từ khoá khác trên 90% - nhưng mọi từ khoá được cào như nhau, tốn tài khoản/proxy như nhau.

Hiệu quả = (bài khớp từ khoá / tên phim trong lúc cào) x (bài liên quan / bài đã lưu):
- vế đầu: bộ đếm spider-hub ghi ở Redis (spiders/keyword_filter.py save_yield) - bài lạc đề bị bỏ ngay trong spider nên
  không có trong D1;
- vế sau: bài lưu trong WINDOW_DAYS ngày qua có RELEVANT_POST_SQL (Kira/quy tắc có thể vẫn chấm không liên quan).

Mỗi lượt cào theo lịch (scheduler._trigger_platform) gọi plan_crawl:
- từ khoá có từ MIN_SAMPLE bài mà hiệu quả < PAUSE_BELOW -> tắt (enabled = 0), ghi lý do ở PAUSED_KEY (tab từ khoá
  thấy được, bật lại tay được). Không tắt từ khoá cuối cùng còn bật của phim trên nền tảng đó - chỉ giảm trang;
- hiệu quả < LOW_BELOW -> chỉ cào LOW_PAGES trang (trang đầu của tìm kiếm là phần tốt nhất);
- còn lại / chưa đủ mẫu -> mặc định của spider.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services.d1 import d1_query

logger = get_logger(__name__)

PLATFORMS = ("facebook", "threads")
WINDOW_DAYS = 14
MIN_SAMPLE = 150
PAUSE_BELOW = 0.10
LOW_BELOW = 0.40
LOW_PAGES = 10
# Từ khoá mới tạo chưa đủ ngày thì chưa đánh giá.
MIN_AGE = timedelta(days=3)

YIELD_KEY_TMPL = f"{REDIS_KEY_PREFIX}keyword_yield:{{keyword_id}}:{{kind}}"
PAUSED_KEY = f"{REDIS_KEY_PREFIX}keyword_yield:paused"


async def _spider_counts(keyword_ids: list[str]) -> dict[str, tuple[int, int]]:
    if not keyword_ids:
        return {}
    redis = get_redis_client()
    pipe = redis.pipeline()
    for keyword_id in keyword_ids:
        pipe.get(YIELD_KEY_TMPL.format(keyword_id=keyword_id, kind="matched"))
        pipe.get(YIELD_KEY_TMPL.format(keyword_id=keyword_id, kind="dropped"))
    values = await pipe.execute()
    out: dict[str, tuple[int, int]] = {}
    for index, keyword_id in enumerate(keyword_ids):
        matched, dropped = int(values[2 * index] or 0), int(values[2 * index + 1] or 0)
        if matched or dropped:
            out[keyword_id] = (matched, dropped)
    return out


async def keyword_yields(platform: str) -> dict[str, dict[str, Any]]:
    """keyword_id -> {stored, relevant, matched, dropped, sample, ratio} cho các từ khoá của nền tảng (cả từ khoá đã tắt)."""
    since = (datetime.now(tz=UTC) - timedelta(days=WINDOW_DAYS)).isoformat()
    rows = (
        await d1_query(
            f"""
            SELECT p.keyword_id AS keyword_id, COUNT(*) AS stored,
                   SUM(CASE WHEN {RELEVANT_POST_SQL} THEN 1 ELSE 0 END) AS relevant
            FROM posts p
            WHERE p.platform = ? AND p.scraped_at >= ? AND p.keyword_id IS NOT NULL
            GROUP BY p.keyword_id
            """,
            [platform, since],
            quiet=True,
            timeout=60.0,
        )
        or []
    )
    keyword_rows = await d1_query("SELECT id FROM keywords WHERE platform = ?", [platform], quiet=True) or []
    ids = [str(row["id"]) for row in keyword_rows]
    spider = await _spider_counts(ids)
    stored = {str(row["keyword_id"]): (int(row["stored"] or 0), int(row["relevant"] or 0)) for row in rows}
    out: dict[str, dict[str, Any]] = {}
    for keyword_id in ids:
        saved, relevant = stored.get(keyword_id, (0, 0))
        matched, dropped = spider.get(keyword_id, (0, 0))
        stored_ratio = relevant / saved if saved else 1.0
        if matched + dropped:
            # Có bộ đếm spider: phần bị bỏ ở spider là rác, phần giữ lại còn bị Kira/quy tắc lọc tiếp theo tỉ lệ ở D1.
            sample, ratio = matched + dropped, (matched / (matched + dropped)) * stored_ratio
        elif saved:
            sample, ratio = saved, stored_ratio  # lượt cào trước bản spider có bộ đếm - chỉ dựa vào D1
        else:
            continue
        out[keyword_id] = {
            "stored": saved,
            "relevant": relevant,
            "matched": matched,
            "dropped": dropped,
            "sample": sample,
            "ratio": round(ratio, 3),
        }
    return out


async def paused_keywords() -> dict[str, dict[str, Any]]:
    raw = await get_redis_client().hgetall(PAUSED_KEY) or {}
    out = {}
    for key, value in raw.items():
        try:
            out[key.decode() if isinstance(key, bytes) else key] = json.loads(value)
        except TypeError, ValueError:
            continue
    return out


async def plan_crawl(
    platform: str, keywords: list[dict[str, Any]], *, dry_run: bool = False
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """(từ khoá còn cào, keyword_id -> max_pages cho từ khoá kém). Tắt luôn từ khoá rác trong D1. Lỗi đo -> cào như cũ."""
    if platform not in PLATFORMS or not keywords:
        return keywords, {}
    try:
        yields = await keyword_yields(platform)
        created = {
            str(row["id"]): str(row.get("created_at") or "")
            for row in await d1_query(
                "SELECT id, created_at FROM keywords WHERE platform = ? AND enabled = 1", [platform], quiet=True
            )
            or []
        }
    except Exception as exc:  # noqa: BLE001 - không đo được thì cào như cũ, không chặn lịch
        logger.warning("keyword_yield_failed", platform=platform, error=str(exc)[:200])
        return keywords, {}

    now = datetime.now(tz=UTC)
    per_movie: dict[str, int] = {}
    for keyword in keywords:
        per_movie[keyword["movie_id"]] = per_movie.get(keyword["movie_id"], 0) + 1

    keep: list[dict[str, Any]] = []
    pages: dict[str, int] = {}
    redis = get_redis_client()
    for keyword in keywords:
        stats = yields.get(str(keyword["id"]))
        try:
            made = datetime.fromisoformat(created.get(str(keyword["id"]), "").replace("Z", "+00:00"))
            old_enough = now - (made if made.tzinfo else made.replace(tzinfo=UTC)) >= MIN_AGE
        except ValueError:
            old_enough = True
        if not stats or stats["sample"] < MIN_SAMPLE or not old_enough:
            keep.append(keyword)
            continue
        if stats["ratio"] < PAUSE_BELOW and per_movie[keyword["movie_id"]] > 1:
            per_movie[keyword["movie_id"]] -= 1
            entry = {"keyword": keyword["keyword"], "platform": platform, "at": now.isoformat(), **stats}
            if not dry_run:
                await d1_query("UPDATE keywords SET enabled = 0 WHERE id = ?", [keyword["id"]], quiet=True)
                await redis.hset(PAUSED_KEY, str(keyword["id"]), json.dumps(entry))
            logger.info("keyword_auto_paused", telegram=not dry_run, dry_run=dry_run, keyword_id=keyword["id"], **entry)
            continue
        if stats["ratio"] < LOW_BELOW:
            pages[str(keyword["id"])] = LOW_PAGES
        keep.append(keyword)
    return keep, pages
