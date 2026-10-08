"""Chọn bài để crawl comment - chạy MỖI GIỜ (scheduler._comments_tick), thay cho lượt "top 100 theo tương tác
của từng từ khoá, mỗi bài một lần duy nhất" chạy mỗi ngày một lần trước 2026-10-08.

Cách cũ có hai lỗi (đo trên D1 ngày 08/10/2026): chỉ ~4% bài liên quan có comment (TikTok 1,6%), và mỗi bài chỉ
được chụp một lần nên mọi comment đến sau lần quét đầu bị mất - đường xu hướng chỉ phản ánh lịch crawl. Nó cũng
chỉ nhìn bài tương tác cao nhất, phần lớn là clip quảng bá - không phải tiếng nói chung của khán giả.

Giờ chia hai lớp, theo TỪNG PHIM đang theo dõi (không theo từ khoá):

1. Theo dõi bài nóng (mỗi giờ): trong bài 72 giờ gần đây, xếp theo TỐC ĐỘ tương tác (tương tác / số giờ từ lúc
   đăng) và lấy `hot_per_movie` bài đầu của mỗi phim. Bài chưa quét thì quét (HOT_FIRST_PAGES trang); bài đã quét
   thì quét lại theo nhịp giảm dần (REFRESH_EVERY theo tuổi bài) khi số comment nền tảng báo đã tăng đáng kể - mỗi
   lần chỉ vài trang (HOT_REFRESH_PAGES), comment đã có bị spider bỏ qua nhờ dedupe.
2. Mẫu phân tầng (2 lượt mỗi ngày, ở run_time và run_time + 12 giờ): từ TOÀN BỘ bài 24 giờ của phim, chia ô theo
   khung 6 giờ x loại nguồn (chính chủ / trang tin / cá nhân) và lấy ngẫu nhiên SAMPLE_PER_CELL bài chưa quét mỗi
   ô - để phần "khán giả nói gì" không bị bài quảng bá chi phối. Phía hiển thị cân lại theo tỷ trọng thật.

MAX_PUBLISH_PER_ROUND giới hạn số request mỗi nền tảng mỗi giờ để không vắt kiệt pool tài khoản/proxy.
"""

from __future__ import annotations

import random
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from app.clients.kafka import publish_comments_crawl_request
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL, ensure_post_voice_columns, reputable_authors
from app.services.d1 import d1_query

logger = get_logger(__name__)

HOT_WINDOW = timedelta(hours=72)
SAMPLE_WINDOW = timedelta(hours=24)
HOT_FIRST_PAGES = 5
HOT_REFRESH_PAGES = 2
SAMPLE_PAGES = 2
SAMPLE_PER_CELL = 2
SAMPLE_SLOT_HOURS = 6
MAX_PUBLISH_PER_ROUND = 150
# Quét lại bài nóng: (tuổi bài tối đa, khoảng cách tối thiểu giữa hai lần quét).
REFRESH_EVERY = ((timedelta(hours=24), timedelta(hours=3)), (timedelta(hours=72), timedelta(hours=12)))
# Số comment nền tảng báo phải tăng ít nhất chừng này (tuyệt đối hoặc tỷ lệ) mới đáng quét lại.
MIN_NEW_REPLIES = 5
MIN_NEW_REPLIES_RATIO = 0.1
# reply_count của bài chỉ được cập nhật khi lượt crawl BÀI tìm lại đúng bài đó (lượt crawl comment không cập nhật nó),
# nên không thể chỉ dựa vào nó: bài dưới YOUNG_POST_AGE thì cứ YOUNG_REFRESH_EVERY lại quét lại dù reply_count chưa đổi.
YOUNG_POST_AGE = timedelta(hours=24)
YOUNG_REFRESH_EVERY = timedelta(hours=6)
# Phim "đang theo dõi": ra rạp trong khoảng này quanh hôm nay (cùng định nghĩa "đang chiếu" với trang Social Topic).
SHOWING_BEFORE = timedelta(days=60)
SHOWING_AFTER = timedelta(days=30)

SOURCE_OFFICIAL = "chinh_chu"
SOURCE_MEDIA = "trang_tin"
SOURCE_PERSONAL = "ca_nhan"


@dataclass(frozen=True)
class CrawlPick:
    post_id: str
    external_id: str
    url: str
    platform: str
    movie_id: str
    max_pages: int
    reason: str
    reply_count: int


def _plain(text: str | None) -> str:
    folded = unicodedata.normalize("NFD", text or "").replace("đ", "d").replace("Đ", "D")
    return re.sub(r"[^a-z0-9]", "", "".join(ch for ch in folded if unicodedata.category(ch) != "Mn").lower())


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _num(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def velocity(row: dict[str, Any], now: datetime) -> float:
    """Tương tác mỗi giờ kể từ lúc đăng (comment tính gấp 3 như ENGAGEMENT_SCORE_SQL; tối thiểu 1 giờ để bài vừa
    đăng không thành vô hạn)."""
    posted = _parse_time(row.get("posted_at")) or now
    hours = max((now - posted).total_seconds() / 3600, 1.0)
    interactions = _num(row.get("like_count")) + 3 * _num(row.get("reply_count")) + _num(row.get("shares"))
    return interactions / hours


def refresh_due(row: dict[str, Any], now: datetime) -> bool:
    """Bài nóng đã từng quét: đến lượt quét lại chưa? Theo tuổi bài (REFRESH_EVERY) và khi số comment nền tảng báo đã
    tăng đáng kể so với lần quét trước - riêng bài còn trẻ (YOUNG_POST_AGE) thì cứ YOUNG_REFRESH_EVERY quét lại."""
    crawled_at = _parse_time(row.get("comments_crawled_at"))
    if crawled_at is None:
        return True
    posted = _parse_time(row.get("posted_at")) or crawled_at
    age = now - posted
    gap = next((every for max_age, every in REFRESH_EVERY if age <= max_age), None)
    if gap is None or now - crawled_at < gap:
        return False
    if age <= YOUNG_POST_AGE and now - crawled_at >= YOUNG_REFRESH_EVERY:
        return True
    before = _num(row.get("comments_crawled_replies"))
    grown = _num(row.get("reply_count")) - before
    return grown >= max(MIN_NEW_REPLIES, before * MIN_NEW_REPLIES_RATIO)


def source_type(row: dict[str, Any], reputable: set[tuple[str, str]]) -> str:
    """Chính chủ: tên tài khoản chứa tên phim (vd. "traibuonnguoimovie"); trang tin: tác giả đã đăng về nhiều phim
    (author_reputation); còn lại: cá nhân."""
    title = _plain(row.get("movie_title"))
    author = row.get("author") or ""
    if len(title) >= 6 and title in _plain(author):
        return SOURCE_OFFICIAL
    if (row.get("platform"), author) in reputable:
        return SOURCE_MEDIA
    return SOURCE_PERSONAL


def pick_hot(rows: list[dict[str, Any]], *, per_movie: int, now: datetime) -> list[CrawlPick]:
    """Lớp 1: top `per_movie` bài theo tốc độ của mỗi phim, chỉ giữ bài chưa quét hoặc đến hạn quét lại."""
    by_movie: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_movie.setdefault(row["movie_id"], []).append(row)
    picks: list[CrawlPick] = []
    for movie_rows in by_movie.values():
        top = sorted(movie_rows, key=lambda r: velocity(r, now), reverse=True)[:per_movie]
        for row in top:
            if _num(row.get("reply_count")) <= 0 or not refresh_due(row, now):
                continue
            first = not row.get("comments_crawled_at")
            picks.append(
                CrawlPick(
                    post_id=row["id"],
                    external_id=row["external_id"],
                    url=row["url"],
                    platform=row["platform"],
                    movie_id=row["movie_id"],
                    max_pages=HOT_FIRST_PAGES if first else HOT_REFRESH_PAGES,
                    reason="hot" if first else "hot_refresh",
                    reply_count=_num(row.get("reply_count")),
                )
            )
    return picks


def pick_sample(
    rows: list[dict[str, Any]], *, now: datetime, reputable: set[tuple[str, str]], rng: random.Random | None = None
) -> list[CrawlPick]:
    """Lớp 2: mỗi phim, chia bài 24 giờ (có comment, chưa từng quét) theo khung SAMPLE_SLOT_HOURS giờ x loại nguồn,
    lấy ngẫu nhiên SAMPLE_PER_CELL bài mỗi ô."""
    rng = rng or random.Random()
    cells: dict[tuple[str, int, str], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("comments_crawled_at") or _num(row.get("reply_count")) <= 0:
            continue
        posted = _parse_time(row.get("posted_at"))
        if posted is None:
            continue
        slot = int((now - posted).total_seconds() // (SAMPLE_SLOT_HOURS * 3600))
        cells.setdefault((row["movie_id"], slot, source_type(row, reputable)), []).append(row)
    picks: list[CrawlPick] = []
    for key in sorted(cells):
        for row in rng.sample(cells[key], min(SAMPLE_PER_CELL, len(cells[key]))):
            picks.append(
                CrawlPick(
                    post_id=row["id"],
                    external_id=row["external_id"],
                    url=row["url"],
                    platform=row["platform"],
                    movie_id=row["movie_id"],
                    max_pages=SAMPLE_PAGES,
                    reason="sample",
                    reply_count=_num(row.get("reply_count")),
                )
            )
    return picks


async def _candidates(platform: str, window: timedelta, now: datetime) -> list[dict[str, Any]]:
    """Bài liên quan, có URL, của các phim đang theo dõi, đăng trong `window`."""
    await ensure_post_voice_columns()
    today = now.date()
    rows = await d1_query(
        f"""
        SELECT p.id, p.external_id, p.url, p.platform, p.movie_id, p.author, p.posted_at,
               p.like_count, p.reply_count,
               COALESCE(p.repost_count, 0) + COALESCE(p.reshare_count, 0) + COALESCE(p.quote_count, 0) AS shares,
               p.comments_crawled_at, p.comments_crawled_replies, m.title AS movie_title
        FROM posts p JOIN movies m ON m.id = p.movie_id
        WHERE p.platform = ? AND m.enabled = 1 AND m.released_at BETWEEN ? AND ?
          AND p.posted_at >= ? AND p.url IS NOT NULL AND {RELEVANT_POST_SQL}
        """,
        [
            platform,
            (today - SHOWING_BEFORE).isoformat(),
            (today + SHOWING_AFTER).isoformat(),
            (now - window).isoformat(),
        ],
    )
    if rows is None:
        raise RuntimeError("comment_planner_select_failed")
    return rows


async def _mark(picks: list[CrawlPick], now: datetime) -> None:
    """Ghi lần quét + reply_count lúc đó, để lượt sau không xếp hàng lại cùng bài trước hạn."""
    for pick in picks:
        await d1_query(
            "UPDATE posts SET comments_crawled_at = ?, comments_crawled_replies = ?, comments_crawl_reason = ? WHERE id = ?",
            [now.isoformat(), pick.reply_count, pick.reason, pick.post_id],
            quiet=True,
        )


async def run_comment_round(platform: str, *, hot_per_movie: int, with_sample: bool) -> dict[str, int]:
    """Một lượt mỗi giờ cho một nền tảng: lớp bài nóng, và lớp mẫu nếu `with_sample`. Trả về thống kê."""
    now = datetime.now(tz=UTC)
    hot_rows = await _candidates(platform, HOT_WINDOW, now)
    picks = pick_hot(hot_rows, per_movie=hot_per_movie, now=now)
    if with_sample:
        chosen = {pick.post_id for pick in picks}
        sample_rows = [row for row in hot_rows if (_parse_time(row.get("posted_at")) or now) >= now - SAMPLE_WINDOW]
        picks += [pick for pick in pick_sample(sample_rows, now=now, reputable=await reputable_authors()) if pick.post_id not in chosen]
    picks = picks[:MAX_PUBLISH_PER_ROUND]
    published: list[CrawlPick] = []
    for pick in picks:
        ok = await publish_comments_crawl_request(
            platform=platform, post_external_id=pick.external_id, post_url=pick.url, max_pages=pick.max_pages, bypass_drain=False
        )
        if ok:
            published.append(pick)
    await _mark(published, now)
    stats = {
        "candidates": len(hot_rows),
        "published": len(published),
        "hot": sum(1 for p in published if p.reason == "hot"),
        "hot_refresh": sum(1 for p in published if p.reason == "hot_refresh"),
        "sample": sum(1 for p in published if p.reason == "sample"),
    }
    logger.info("comment_round_finished", platform=platform, with_sample=with_sample, **stats)
    return stats
