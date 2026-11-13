"""Chọn bài để cào bình luận, chạy mỗi giờ theo từng phim đang theo dõi.

1. Bài nóng: bài 72 giờ gần đây xếp theo tốc độ tương tác, lấy `hot_per_movie` bài mỗi phim. Bài chưa quét thì quét
   `HOT_FIRST_PAGES` trang; bài đã quét được quét lại theo nhịp giảm dần khi số bình luận tăng đáng kể.
2. Mẫu phân tầng (2 lượt/ngày): chia bài 24 giờ theo khung 6 giờ x loại nguồn, lấy ngẫu nhiên `SAMPLE_PER_CELL` bài
   mỗi ô, để phần "khán giả nói gì" không bị bài quảng bá chi phối.
3. AI chọn: bài nhiều bình luận được chấm điểm (`comment_value`); điểm cao thì quét sâu, điểm thấp (minigame, quảng cáo)
   bị loại khỏi hai lớp trên.

`MAX_PUBLISH_PER_ROUND` giới hạn số request mỗi nền tảng mỗi giờ. Trước khi đẩy job chỉ ghi lease
`comments_queued_at`; `comments_crawled_at` chỉ ghi khi spider-hub báo job xong, để job hỏng được thử lại.
"""

from __future__ import annotations

import json
import random
import re
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import case, func, select, update

from app.clients.kafka import publish_comments_crawl_request
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.services.accounts.platform_config_db import crawl_setting
from app.core.logging import get_logger
from app.repositories.d1.posts import ensure_post_voice_columns, relevant_post, reputable_authors
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import movies, posts
from app.services.crawl.comment_value import judge_new_posts, load_scores

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
# Lớp 3 (AI chọn): xét bài trong VALUE_WINDOW, quét bài điểm cao, bỏ bài điểm thấp khỏi mọi lớp.
VALUE_WINDOW = timedelta(days=7)
VALUE_PICK_SCORE = 70
VALUE_SKIP_SCORE = 20
VALUE_PER_MOVIE = 8
VALUE_PAGES = 5
# Nhân số trang theo nền tảng: Facebook Comet ~10 comment/trang, Threads cũng ít - cùng số trang với TikTok
# (20-50 comment/trang) thì bài FB/Threads chỉ được vài chục comment. Vừa phải vì tài khoản Facebook còn ít.
PAGE_MULTIPLIER = {"facebook": 2, "threads": 2}
# Lease xếp hàng: job thường báo kết thúc rồi nhả; chỉ khi mất luôn tín hiệu đó (consumer chết, message mất) mới phải chờ hết
# hạn này. Rộng hơn hàng tồn comment Facebook lâu nhất.
QUEUE_LEASE = timedelta(hours=24)
# Giữ đúng tên key của spider-hub (runtime/task_queue.py); mỗi nhịp scheduler xử lý tối đa từng này tín hiệu.
COMMENT_COMPLETIONS_KEY = f"{REDIS_KEY_PREFIX}comment_crawl_completions"
COMPLETIONS_PER_TICK = 100

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
    except TypeError, ValueError:
        return 0


def velocity(row: dict[str, Any], now: datetime) -> float:
    """Tương tác mỗi giờ kể từ lúc đăng (comment tính gấp 3 như ENGAGEMENT_SCORE_SQL; tối thiểu 1 giờ để bài vừa
    đăng không thành vô hạn)."""
    posted = _parse_time(row.get("posted_at")) or now
    hours = max((now - posted).total_seconds() / 3600, 1.0)
    interactions = _num(row.get("like_count")) + 3 * _num(row.get("reply_count")) + _num(row.get("shares"))
    return interactions / hours


def queued_recently(row: dict[str, Any], now: datetime) -> bool:
    """Bài đã có job comment đang xếp hàng/đang chạy (lease còn hạn) - lượt này không xếp trùng."""
    queued_at = _parse_time(row.get("comments_queued_at"))
    return queued_at is not None and now - queued_at < QUEUE_LEASE


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


def pick_valuable(
    rows: list[dict[str, Any]], scores: dict[str, int], *, per_movie: int, now: datetime
) -> list[CrawlPick]:
    """Lớp 3: mỗi phim, các bài AI chấm >= VALUE_PICK_SCORE - chưa quét thì quét sâu, đã quét thì quét lại khi đến hạn
    (refresh_due). Điểm cao trước, rồi nhiều comment trước."""
    by_movie: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if scores.get(row["id"], -1) >= VALUE_PICK_SCORE and _num(row.get("reply_count")) > 0:
            by_movie.setdefault(row["movie_id"], []).append(row)
    picks: list[CrawlPick] = []
    for movie_rows in by_movie.values():
        ranked = sorted(movie_rows, key=lambda r: (scores[r["id"]], _num(r.get("reply_count"))), reverse=True)
        chosen = 0
        for row in ranked:
            if chosen >= per_movie:
                break
            first = not row.get("comments_crawled_at")
            if not first and not refresh_due(row, now):
                continue
            chosen += 1
            picks.append(
                CrawlPick(
                    post_id=row["id"],
                    external_id=row["external_id"],
                    url=row["url"],
                    platform=row["platform"],
                    movie_id=row["movie_id"],
                    max_pages=VALUE_PAGES if first else HOT_REFRESH_PAGES,
                    reason="ai_value",
                    reply_count=_num(row.get("reply_count")),
                )
            )
    return picks


async def _candidates(platform: str, window: timedelta, now: datetime) -> list[dict[str, Any]]:
    """Bài liên quan, có URL, của các phim đang theo dõi, đăng trong `window`."""
    await ensure_post_voice_columns()
    today = now.date()
    p = posts.alias("p")
    shares = (
        func.coalesce(p.c.repost_count, 0) + func.coalesce(p.c.reshare_count, 0) + func.coalesce(p.c.quote_count, 0)
    )
    rows = await d1_exec(
        select(
            *(p.c[name] for name in ("id", "external_id", "url", "platform", "movie_id", "author", "posted_at")),
            p.c.like_count,
            p.c.reply_count,
            shares.label("shares"),
            p.c.comments_crawled_at,
            p.c.comments_crawled_replies,
            p.c.comments_queued_at,
            movies.c.title.label("movie_title"),
            func.substr(p.c.content, 1, 400).label("content"),
        )
        .select_from(p.join(movies, movies.c.id == p.c.movie_id))
        .where(
            p.c.platform == platform,
            movies.c.enabled == 1,
            movies.c.released_at.between((today - SHOWING_BEFORE).isoformat(), (today + SHOWING_AFTER).isoformat()),
            p.c.posted_at >= (now - window).isoformat(),
            p.c.url.is_not(None),
            relevant_post(p),
        )
    )
    if rows is None:
        raise RuntimeError("comment_planner_select_failed")
    return rows


async def _mark_queued(pick: CrawlPick, now: datetime, run_id: str) -> bool:
    """Ghi lease TRƯỚC khi publish: job chạy nhanh có thể xong trước khi bước ghi này chạy, và ghi sau sẽ đè lên tín hiệu xong -
    bài đã quét lại mang lease 24 giờ. False khi D1 lỗi (không publish: không có lease thì lượt sau xếp trùng)."""
    result = await d1_exec(
        update(posts)
        .where(posts.c.id == pick.post_id)
        .values(
            comments_queued_at=now.isoformat(),
            comments_queued_replies=pick.reply_count,
            comments_queued_run_id=run_id,
            comments_crawl_reason=pick.reason,
        ),
        quiet=True,
    )
    return result is not None


_NO_LEASE = {"comments_queued_at": None, "comments_queued_replies": None, "comments_queued_run_id": None}


async def _release_queued(pick: CrawlPick, run_id: str) -> None:
    await d1_exec(
        update(posts).where(posts.c.id == pick.post_id, posts.c.comments_queued_run_id == run_id).values(**_NO_LEASE),
        quiet=True,
    )


async def _apply_completion(item: dict[str, Any]) -> bool:
    """Job xong thì luôn ghi lần quét; bị bỏ qua / lỗi thì không. Lease chỉ nhả khi đúng run_id: tín hiệu muộn của job cũ, hay của
    một lần bấm tay trên dashboard, không được xoá lease của job khác còn đang xếp hàng. False khi D1 lỗi."""
    run_id = item.get("run_id")
    target = (posts.c.platform == item["platform"], posts.c.external_id == item["post_id"])
    if item.get("status") == "done":
        mine = posts.c.comments_queued_run_id == run_id

        def if_mine(value, otherwise):
            return case((mine, value), else_=otherwise)

        stmt = (
            update(posts)
            .where(*target)
            .values(
                comments_crawled_at=datetime.fromtimestamp(float(item["finished_at"]), tz=UTC).isoformat(),
                comments_crawled_replies=if_mine(
                    func.coalesce(posts.c.comments_queued_replies, posts.c.reply_count), posts.c.reply_count
                ),
                comments_queued_at=if_mine(None, posts.c.comments_queued_at),
                comments_queued_replies=if_mine(None, posts.c.comments_queued_replies),
                comments_queued_run_id=if_mine(None, posts.c.comments_queued_run_id),
            )
        )
    else:
        stmt = update(posts).where(*target, posts.c.comments_queued_run_id == run_id).values(**_NO_LEASE)
    return await d1_exec(stmt, quiet=True) is not None


async def apply_comment_completions() -> int:
    """Áp tín hiệu kết thúc job comment từ spider-hub lên D1, cũ nhất trước. Chỉ gỡ khỏi hàng sau khi D1 ghi xong, nên D1 lỗi
    thì tín hiệu còn nguyên cho nhịp sau. Ghi lại là vô hại."""
    await ensure_post_voice_columns()
    redis = get_redis_client()
    applied = 0
    for _ in range(COMPLETIONS_PER_TICK):
        raw = await redis.lindex(COMMENT_COMPLETIONS_KEY, -1)
        if raw is None:
            break
        try:
            ok = await _apply_completion(json.loads(raw))
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning("comment_completion_malformed", error=str(exc)[:200])
            ok = True
        else:
            if not ok:
                logger.warning("comment_completion_d1_failed", applied=applied)
                break
            applied += 1
        await redis.lrem(COMMENT_COMPLETIONS_KEY, -1, raw)
    if applied:
        logger.info("comment_completions_applied", applied=applied)
    return applied


async def run_comment_round(platform: str, *, hot_per_movie: int, with_sample: bool) -> dict[str, int]:
    """Một lượt mỗi giờ cho một nền tảng: lớp bài nóng, và lớp mẫu nếu `with_sample`. Trả về thống kê."""
    now = datetime.now(tz=UTC)
    all_rows = await _candidates(platform, VALUE_WINDOW, now)
    week_rows = [row for row in all_rows if not queued_recently(row, now)]
    scores: dict[str, int] = {}
    try:
        scores = await load_scores([row["id"] for row in week_rows if _num(row.get("reply_count")) > 0])
        scores.update(await judge_new_posts(week_rows, scores))
    except Exception as exc:  # noqa: BLE001 - không có điểm AI thì chạy như trước
        logger.warning("comment_value_unavailable", platform=platform, error=str(exc)[:300])
    # Bài AI chấm là rác comment (minigame tag bạn, quảng cáo) bị bỏ khỏi mọi lớp.
    usable = [row for row in week_rows if scores.get(row["id"], 100) >= VALUE_SKIP_SCORE]
    hot_rows = [row for row in usable if (_parse_time(row.get("posted_at")) or now) >= now - HOT_WINDOW]
    picks = pick_valuable(usable, scores, per_movie=VALUE_PER_MOVIE, now=now)
    chosen = {pick.post_id for pick in picks}
    picks += [pick for pick in pick_hot(hot_rows, per_movie=hot_per_movie, now=now) if pick.post_id not in chosen]
    if with_sample:
        chosen = {pick.post_id for pick in picks}
        sample_rows = [row for row in hot_rows if (_parse_time(row.get("posted_at")) or now) >= now - SAMPLE_WINDOW]
        picks += [
            pick
            for pick in pick_sample(sample_rows, now=now, reputable=await reputable_authors())
            if pick.post_id not in chosen
        ]
    picks = picks[:MAX_PUBLISH_PER_ROUND]
    published: list[CrawlPick] = []
    for pick in picks:
        run_id = str(uuid.uuid4())
        if not await _mark_queued(pick, now, run_id):
            logger.warning("comment_queue_lease_failed", platform=platform, post_id=pick.post_id)
            continue
        ok = await publish_comments_crawl_request(
            platform=platform,
            post_external_id=pick.external_id,
            post_url=pick.url,
            max_pages=pick.max_pages * PAGE_MULTIPLIER.get(platform, 1),
            bypass_drain=False,
            run_id=run_id,
        )
        if ok:
            published.append(pick)
        else:
            await _release_queued(pick, run_id)
    stats = {
        "candidates": len(hot_rows),
        "published": len(published),
        "hot": sum(1 for p in published if p.reason == "hot"),
        "hot_refresh": sum(1 for p in published if p.reason == "hot_refresh"),
        "sample": sum(1 for p in published if p.reason == "sample"),
        "ai_value": sum(1 for p in published if p.reason == "ai_value"),
        "skipped_low_value": len(week_rows) - len(usable),
        "skipped_queued": len(all_rows) - len(week_rows),
    }
    logger.info("comment_round_finished", platform=platform, with_sample=with_sample, **stats)
    return stats


# YouTube (13/11/2026): cào bình luận + trả lời của MỌI video liên quan (không chọn lọc bài nóng như nền tảng dùng tài khoản) -
# spider-hub đọc không tài khoản theo làn song song. Video chưa cào đi trước; video mới đăng được quét lại để lấy bình luận mới.
YOUTUBE_WINDOW = timedelta(days=90)
YOUTUBE_REFRESH = (
    (timedelta(days=3), timedelta(hours=12)),
    (timedelta(days=14), timedelta(days=1)),
    (YOUTUBE_WINDOW, timedelta(days=7)),
)


def youtube_due(row: dict[str, Any], now: datetime) -> bool:
    crawled = _parse_time(row.get("comments_crawled_at"))
    if crawled is None:
        return True
    age = now - (_parse_time(row.get("posted_at")) or now)
    for max_age, every in YOUTUBE_REFRESH:
        if age <= max_age:
            return now - crawled >= every
    return False


async def run_youtube_comment_round() -> dict[str, int]:
    now = datetime.now(tz=UTC)
    rows = await _candidates("youtube", YOUTUBE_WINDOW, now)
    due = [row for row in rows if not queued_recently(row, now) and youtube_due(row, now)]
    # Chưa cào trước, rồi video nhiều tương tác.
    due.sort(key=lambda row: (row.get("comments_crawled_at") is not None, -_num(row.get("like_count"))))
    published = 0
    for row in due[: int(await crawl_setting("youtube_comments_per_round"))]:
        pick = CrawlPick(
            post_id=row["id"],
            external_id=str(row["external_id"]),
            url=str(row["url"]),
            platform="youtube",
            movie_id=row["movie_id"],
            max_pages=0,
            reason="youtube_all",
            reply_count=_num(row.get("reply_count")),
        )
        run_id = str(uuid.uuid4())
        if not await _mark_queued(pick, now, run_id):
            continue
        if await publish_comments_crawl_request(
            platform="youtube", post_external_id=pick.external_id, post_url=pick.url, bypass_drain=False, run_id=run_id
        ):
            published += 1
        else:
            await _release_queued(pick, run_id)
    stats = {"relevant": len(rows), "due": len(due), "published": published}
    logger.info("youtube_comment_round_finished", **stats)
    return stats
