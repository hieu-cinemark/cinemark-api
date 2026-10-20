"""Bình luận TikTok qua Omar API - MỖI NGÀY (settings.omar_comments_cadence, từ 2026-10-20; mỗi tuần từ 2026-10-12).

Mỗi ngày (hoặc từ thứ settings.omar_comments_weekday) lúc settings.omar_comments_time (giờ VN), enqueue_weekly_job lấy từ D1 cho mỗi
hashtag TikTok đang bật settings.omar_comments_top_per_hashtag video đăng trong 7 ngày qua, tương tác cao nhất (thích +
3 x bình luận + chia sẻ, chỉ bài liên quan và có bình luận), rồi xếp vào hàng đợi Redis (omar:comments:jobs). Mỗi lượt
Omar (omar_tiktok.run_omar_round) xử lý hàng đợi SAU phần video mới (ưu tiên video): mỗi video lật /comments tới hết, bình luận nào có
trả lời thì lật /comment_replies tới hết. Con trỏ lưu sau mỗi trang - hết ngân sách / dừng / restart thì lượt sau đi
tiếp đúng chỗ; xong video thì xoá khỏi hàng đợi và đánh dấu posts.comments_crawled_* (lý do "omar_weekly").

Bình luận đi ĐÚNG đường của spider: topic raw_comments, key "tiktok:<comment_id>", cùng tên trường TikTokCommentItem
(map_comment là bản port của spiders/tiktok/features/comments/extract.py), chống trùng qua tập
tiktok:seen_comment_ids dùng chung với spider - bình luận spider đã gửi thì Omar không gửi lại và ngược lại."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import httpx

from app.clients.kafka import publish_raw_comment
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services import omar_yield
from app.services.d1 import d1_query, get_enabled_keywords

if TYPE_CHECKING:
    from app.services.omar_tiktok import Budget, _Pacer

logger = get_logger(__name__)

JOBS_KEY = f"{REDIS_KEY_PREFIX}omar:comments:jobs"
WEEK_KEY = f"{REDIS_KEY_PREFIX}omar:comments:week"
LAST_JOB_KEY = f"{REDIS_KEY_PREFIX}omar:comments:last_job"
ENQUEUE_LOCK_KEY = f"{REDIS_KEY_PREFIX}omar:comments:enqueuing"
_SPIDER_SEEN_COMMENTS_KEY = f"{REDIS_KEY_PREFIX}tiktok:seen_comment_ids"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
WINDOW = timedelta(days=7)
# Chặn vòng lặp vô tận nếu API cứ trả has_more (30 bình luận/trang -> 60.000 bình luận một video).
_MAX_PAGES_PER_VIDEO = 2000
_MAX_REPLY_PAGES = 200


def map_comment(raw: dict[str, Any], *, parent_comment_id: str | None = None) -> dict[str, Any] | None:
    """Một bình luận dạng gốc của TikTok -> tên trường TikTokCommentItem của spider-hub. None khi thiếu id."""
    cid = raw.get("cid")
    if not cid:
        return None
    user = raw.get("user") or {}
    reply_id = str(raw.get("reply_id") or "")
    parent = parent_comment_id or (reply_id if reply_id and reply_id != "0" else None)
    return {
        "comment_id": str(cid),
        "message": raw.get("text"),
        "timestamp": raw.get("create_time"),
        "like_count": raw.get("digg_count") or 0,
        "reply_count": raw.get("reply_comment_total") or 0,
        "author_id": user.get("uid"),
        "author_username": user.get("unique_id"),
        "author_name": user.get("nickname"),
        "author_avatar_url": ((user.get("avatar_thumb") or {}).get("url_list") or [None])[0],
        "parent_comment_id": parent,
    }


def _daily() -> bool:
    return settings.omar_comments_cadence.strip().lower() == "daily"


def iso_week(now: datetime) -> str:
    """Mã của kỳ xếp hàng: ngày (YYYY-MM-DD) khi chạy mỗi ngày, tuần ISO (YYYY-Www) khi chạy mỗi tuần."""
    if _daily():
        return now.date().isoformat()
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def weekly_due(now: datetime) -> bool:
    """Đã qua mốc giờ của hôm nay (mỗi ngày) / mốc (thứ, giờ) của tuần này (mỗi tuần) chưa - bỏ lỡ mốc (API tắt) thì chạy
    bù khi bật lại trong kỳ."""
    try:
        hour, minute = (int(part) for part in settings.omar_comments_time.split(":", 1))
    except ValueError:
        return False
    if _daily():
        return now.hour * 60 + now.minute >= hour * 60 + minute
    return (now.weekday(), now.hour * 60 + now.minute) >= (settings.omar_comments_weekday, hour * 60 + minute)


# Chạy mỗi ngày: bỏ video Omar đã cào hết mà chưa có thêm đủ bình luận. Chỉ tính lần cào của Omar (lý do "omar_..."):
# spider cũng ghi comments_crawled_* nhưng chỉ quét vài trang đầu (comment_planner), video đó vẫn cần Omar cào hết.
_NEEDS_CRAWL_SQL = """(COALESCE(p.comments_crawl_reason, '') NOT LIKE 'omar%'
  OR (p.reply_count - COALESCE(p.comments_crawled_replies, 0) >= ?
      AND p.reply_count >= COALESCE(p.comments_crawled_replies, 0) * (1 + ?)))"""


async def _top_videos(keyword_ids: list[str], since: str, limit: int) -> dict[str, list[dict[str, Any]]]:
    """Top `limit` video theo tương tác của MỖI từ khoá, một truy vấn cho mọi từ khoá (ROW_NUMBER theo keyword_id).
    Trước 14/10/2026 là một truy vấn mỗi từ khoá chạy lần lượt: 147 hashtag ~140 giây, nút "Cào bình luận ngay" của
    dashboard hết giờ chờ 15 giây. Danh sách id truyền qua json_each (D1 giới hạn 100 tham số mỗi câu)."""
    if not keyword_ids:
        return {}
    needs = f" AND {_NEEDS_CRAWL_SQL}" if _daily() else ""
    regrowth = [settings.omar_comments_regrowth_min, settings.omar_comments_regrowth_ratio] if _daily() else []
    rows = (
        await d1_query(
            f"""
            SELECT keyword_id, id, external_id, reply_count FROM (
              SELECT p.keyword_id, p.id, p.external_id, p.reply_count,
                     ROW_NUMBER() OVER (
                       PARTITION BY p.keyword_id
                       ORDER BY COALESCE(p.like_count, 0) + 3 * COALESCE(p.reply_count, 0)
                                + COALESCE(p.repost_count, 0) + COALESCE(p.reshare_count, 0) DESC
                     ) AS rn
              FROM posts p
              WHERE p.platform = 'tiktok' AND p.keyword_id IN (SELECT value FROM json_each(?))
                AND p.posted_at >= ? AND p.reply_count > 0 AND {RELEVANT_POST_SQL}{needs}
            ) WHERE rn <= ? ORDER BY keyword_id, rn
            """,
            [json.dumps(keyword_ids), since, *regrowth, limit],
            quiet=True,
            timeout=60.0,
        )
        or []
    )
    by_keyword: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_keyword.setdefault(str(row["keyword_id"]), []).append(row)
    return by_keyword


_KEYWORD_TABLE_READY = False


async def _ensure_keyword_table() -> None:
    global _KEYWORD_TABLE_READY
    if _KEYWORD_TABLE_READY:
        return
    await d1_query(
        "CREATE TABLE IF NOT EXISTS omar_comment_keywords (keyword_id TEXT PRIMARY KEY, created_at TEXT NOT NULL)",
        quiet=True,
    )
    _KEYWORD_TABLE_READY = True


_OPTIONS_TABLE_READY = False


async def _ensure_options_table() -> None:
    global _OPTIONS_TABLE_READY
    if _OPTIONS_TABLE_READY:
        return
    await d1_query("CREATE TABLE IF NOT EXISTS omar_comment_options (key TEXT PRIMARY KEY, value TEXT NOT NULL)", quiet=True)
    _OPTIONS_TABLE_READY = True


async def all_videos_enabled() -> bool:
    """Tuỳ chọn "toàn bộ video" ở tab Omar: ngoài top video 7 ngày, xếp hàng MỌI video có bình luận của các từ khoá đã
    chọn (mọi thời điểm) mà Omar chưa cào. Chỉ áp dụng cho từ khoá chọn tay - danh sách trống (mặc định mọi hashtag)
    thì bỏ qua, tránh xếp hàng hàng chục nghìn video một lúc."""
    await _ensure_options_table()
    rows = await d1_query("SELECT value FROM omar_comment_options WHERE key = 'all_videos'", quiet=True) or []
    return bool(rows) and str(rows[0]["value"]) == "1"


async def set_all_videos(enabled: bool) -> bool:
    await _ensure_options_table()
    await d1_query(
        "INSERT INTO omar_comment_options (key, value) VALUES ('all_videos', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        ["1" if enabled else "0"],
        quiet=True,
    )
    logger.info("omar_comment_all_videos_set", enabled=enabled)
    return enabled


async def _all_videos(keyword_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Mọi video TikTok liên quan, có bình luận, của các từ khoá này mà Omar chưa cào xong (comments_crawl_reason không
    bắt đầu bằng "omar") - nhiều tương tác trước. Video Omar đã cào thì lịch tuần (top 7 ngày) lo phần bình luận mới."""
    if not keyword_ids:
        return {}
    rows = (
        await d1_query(
            f"""
            SELECT p.keyword_id, p.id, p.external_id, p.reply_count
            FROM posts p
            WHERE p.platform = 'tiktok' AND p.keyword_id IN (SELECT value FROM json_each(?))
              AND p.reply_count > 0 AND {RELEVANT_POST_SQL}
              AND (p.comments_crawl_reason IS NULL OR p.comments_crawl_reason NOT LIKE 'omar%')
            ORDER BY p.keyword_id,
                     COALESCE(p.like_count, 0) + 3 * COALESCE(p.reply_count, 0)
                     + COALESCE(p.repost_count, 0) + COALESCE(p.reshare_count, 0) DESC
            """,
            [json.dumps(keyword_ids)],
            quiet=True,
            timeout=60.0,
        )
        or []
    )
    by_keyword: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_keyword.setdefault(str(row["keyword_id"]), []).append(row)
    return by_keyword


async def selected_keyword_ids() -> list[str]:
    """Từ khoá người dùng chọn để cào bình luận (tab Omar). Rỗng = mặc định: mọi hashtag TikTok đang bật."""
    await _ensure_keyword_table()
    rows = await d1_query("SELECT keyword_id FROM omar_comment_keywords ORDER BY created_at", quiet=True) or []
    return [str(row["keyword_id"]) for row in rows]


async def set_selected_keyword_ids(keyword_ids: list[str]) -> list[str]:
    """Thay toàn bộ danh sách (chỉ giữ id là từ khoá TikTok đang có). Danh sách rỗng = quay về mặc định."""
    await _ensure_keyword_table()
    known = {str(k["id"]) for k in await get_enabled_keywords(platform="tiktok")}
    wanted = list(dict.fromkeys(str(i) for i in keyword_ids if str(i) in known))
    now = datetime.now(tz=UTC).isoformat()
    await d1_query("DELETE FROM omar_comment_keywords", quiet=True)
    for start in range(0, len(wanted), 40):
        chunk = wanted[start : start + 40]
        await d1_query(
            "INSERT INTO omar_comment_keywords (keyword_id, created_at) VALUES " + ", ".join("(?, ?)" for _ in chunk),
            [value for keyword_id in chunk for value in (keyword_id, now)],
            quiet=True,
        )
    logger.info("omar_comment_keywords_set", count=len(wanted))
    return wanted


async def comment_keywords() -> list[dict[str, Any]]:
    """Từ khoá được cào bình luận ở lượt tới: danh sách đã chọn (cả từ khoá chữ, không chỉ hashtag - từ 13/10/2026 TikTok
    cào được cả tìm kiếm từ khoá) hoặc mặc định mọi hashtag TikTok đang bật. Từ khoá đã chọn mà bị tắt thì bỏ qua."""
    enabled = await get_enabled_keywords(platform="tiktok")
    selected = set(await selected_keyword_ids())
    if selected:
        return [k for k in enabled if str(k["id"]) in selected]
    return [k for k in enabled if str(k["keyword"]).startswith("#")]


async def enqueue_weekly_job(*, week: str | None = None, trigger: str = "schedule") -> dict[str, Any]:
    """Xếp hàng top video tuần của các từ khoá cào bình luận (comment_keywords). Video trùng giữa các từ khoá chỉ xếp
    một lần; video còn trong hàng đợi từ tuần trước (chưa cào xong) được giữ nguyên con trỏ."""
    now = datetime.now(tz=UTC)
    since = (now - WINDOW).isoformat()
    keywords = await comment_keywords()
    redis = get_redis_client()
    existing = set(await redis.hkeys(JOBS_KEY) or [])
    existing = {key.decode() if isinstance(key, bytes) else key for key in existing}
    added, per_hashtag = 0, {}
    jobs: dict[str, str] = {}
    keyword_ids = [str(kw["id"]) for kw in keywords]
    top = await _top_videos(keyword_ids, since, settings.omar_comments_top_per_hashtag)
    all_videos = bool(await selected_keyword_ids()) and await all_videos_enabled()
    rest = await _all_videos(keyword_ids) if all_videos else {}
    for kw in keywords:
        # Top 7 ngày trước (rank nhỏ = cào trước), rồi phần còn lại của tuỳ chọn "toàn bộ video".
        rows = top.get(str(kw["id"]), []) + rest.get(str(kw["id"]), [])
        per_hashtag[kw["keyword"]] = len(rows)
        for rank, row in enumerate(rows):
            video_id = str(row.get("external_id") or "")
            if not video_id or video_id in existing or video_id in jobs:
                continue
            jobs[video_id] = json.dumps(
                {
                    "video_id": video_id,
                    "post_id": row["id"],
                    "hashtag": kw["keyword"],
                    "rank": rank,
                    "reason": "omar_daily" if _daily() else "omar_weekly",
                    "reply_count": int(row.get("reply_count") or 0),
                    "cursor": "0",
                    "pages": 0,
                    "comments": 0,
                }
            )
    if jobs:
        await redis.hset(JOBS_KEY, mapping=jobs)
        added = len(jobs)
    # Chạy tay cũng tính là lượt của tuần này - mốc tuần không xếp hàng lại đúng những video đó.
    week = week or iso_week(now.astimezone(VN_TZ))
    await redis.set(WEEK_KEY, week, ex=14 * 24 * 3600)
    info = {
        "week": week,
        "trigger": trigger,
        "queued_at": now.isoformat(),
        "hashtags": len(keywords),
        "all_videos": all_videos,
        "videos": added,
        "kept_from_before": len(existing),
        "finished_at": None,
        "comments": 0,
        "replies": 0,
        "published": 0,
    }
    await redis.set(LAST_JOB_KEY, json.dumps(info))
    logger.info("omar_comments_enqueued", **{k: v for k, v in info.items() if k != "finished_at"})
    return info


async def weekly_enqueue_due(now_local: datetime) -> bool:
    """Gọi từ scheduler mỗi tick (phải nhanh - tick bị cắt sau 25 giây): tới mốc tuần, tuần này chưa xếp hàng và không có
    lượt xếp hàng nào đang chạy thì giữ khoá và trả True - bên gọi chạy enqueue_weekly_job ở nền (một truy vấn D1 mỗi
    hashtag, lâu hơn một tick nhiều). Khoá tự hết hạn sau 15 phút, nên xếp hàng lỗi thì tick sau thử lại."""
    if not weekly_due(now_local):
        return False
    redis = get_redis_client()
    if await redis.get(WEEK_KEY) == iso_week(now_local):
        return False
    return bool(await redis.set(ENQUEUE_LOCK_KEY, "1", ex=15 * 60, nx=True))


async def enqueue_videos(videos: list[dict[str, Any]], *, reason: str, hashtag: str = "trending") -> int:
    """Xếp thêm từng video ({video_id, post_id, reply_count}) vào CÙNG hàng đợi với job tuần - dùng cho video đang lên
    (app/services/omar_trending.py). Video đã có trong hàng đợi giữ nguyên con trỏ. Không đụng LAST_JOB_KEY / WEEK_KEY nên
    lịch tuần không đổi. Trả số video thêm mới."""
    redis = get_redis_client()
    existing = {key.decode() if isinstance(key, bytes) else key for key in await redis.hkeys(JOBS_KEY) or []}
    jobs: dict[str, str] = {}
    for rank, video in enumerate(videos):
        video_id = str(video.get("video_id") or "")
        if not video_id or video_id in existing or video_id in jobs or not video.get("post_id"):
            continue
        jobs[video_id] = json.dumps(
            {
                "video_id": video_id,
                "post_id": video["post_id"],
                "hashtag": hashtag,
                "rank": rank,
                "reply_count": int(video.get("reply_count") or 0),
                "cursor": "0",
                "pages": 0,
                "comments": 0,
                "reason": reason,
            }
        )
    if jobs:
        await redis.hset(JOBS_KEY, mapping=jobs)
        logger.info("omar_comments_videos_enqueued", reason=reason, videos=len(jobs))
    return len(jobs)


async def pending_jobs() -> list[dict[str, Any]]:
    raw = await get_redis_client().hgetall(JOBS_KEY) or {}
    jobs = []
    for value in raw.values():
        try:
            jobs.append(json.loads(value))
        except TypeError, ValueError:
            continue
    return sorted(jobs, key=lambda job: (job.get("rank", 0), -int(job.get("reply_count") or 0)))


async def job_status() -> dict[str, Any]:
    """Cho tab Omar: lần xếp hàng gần nhất + số video còn trong hàng đợi."""
    redis = get_redis_client()
    raw = await redis.get(LAST_JOB_KEY)
    last = json.loads(raw) if raw else None
    pending = await redis.hlen(JOBS_KEY)
    return {"last": last, "pending_videos": int(pending or 0)}


async def _update_last_job(**delta: int) -> None:
    redis = get_redis_client()
    raw = await redis.get(LAST_JOB_KEY)
    if not raw:
        return
    info = json.loads(raw)
    for key, value in delta.items():
        info[key] = int(info.get(key) or 0) + value
    if not await redis.hlen(JOBS_KEY) and not info.get("finished_at"):
        info["finished_at"] = datetime.now(tz=UTC).isoformat()
    await redis.set(LAST_JOB_KEY, json.dumps(info))


async def _publish(video_id: str, comments: list[dict[str, Any]]) -> int:
    """Gửi các bình luận chưa có trong tập dedupe chung với spider. Trả số đã gửi."""
    if not comments:
        return 0
    redis = get_redis_client()
    pipe = redis.pipeline()
    for comment in comments:
        pipe.sadd(_SPIDER_SEEN_COMMENTS_KEY, comment["comment_id"])
    added = await pipe.execute()
    sent = 0
    for comment, is_new in zip(comments, added, strict=True):
        if not is_new:
            continue
        ok = await publish_raw_comment(
            key=f"tiktok:{comment['comment_id']}",
            value={"platform": "tiktok", "post_id": video_id, "video_id": video_id, **comment},
        )
        if ok:
            sent += 1
        else:
            await redis.srem(_SPIDER_SEEN_COMMENTS_KEY, comment["comment_id"])  # chưa tới Kafka: lần sau gửi lại
    # Bình luận chỉ gửi khi chưa từng thấy, nên số gửi = số mới.
    await omar_yield.add("comments", sent=sent, new=sent)
    return sent


async def process_queue(
    client: httpx.AsyncClient,
    pacer: _Pacer,
    api_key: str,
    budget: Budget,
    stats: dict[str, Any],
    should_stop: Callable[[], bool],
) -> None:
    """Cào bình luận các video trong hàng đợi trên settings.omar_concurrency worker song song (mỗi video một worker),
    trong `budget` dùng chung với lượt. OmarStop được ném ra cho run_omar_round dừng lượt."""
    from app.services.omar_tiktok import OmarStop, next_cursor, omar_get

    jobs = await pending_jobs()
    if not jobs:
        return
    redis = get_redis_client()
    stats.update(comment_videos=0, comment_requests=0, comments=0, comment_replies=0, comments_published=0)
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    for job in jobs:
        queue.put_nowait(job)
    stopped: list[BaseException] = []

    def halted() -> bool:
        return bool(stopped) or should_stop() or budget.exhausted

    async def get(path: str, params: dict[str, str]) -> dict[str, Any] | None:
        if not budget.take():
            return None
        stats["requests"] += 1
        stats["comment_requests"] += 1
        await omar_yield.add("comments", requests=1)
        return await omar_get(client, pacer, api_key, path, params)

    async def replies_of(video_id: str, comment_id: str) -> bool:
        """Mọi trả lời của một bình luận. False khi hết ngân sách giữa chừng."""
        cursor = "0"
        for _ in range(_MAX_REPLY_PAGES):
            if halted():
                return False
            data = await get("comment_replies", {"video": video_id, "comment_id": comment_id, "cursor": cursor})
            if data is None:
                return not budget.exhausted
            mapped = [c for c in (map_comment(r, parent_comment_id=comment_id) for r in data.get("replies") or []) if c]
            stats["comment_replies"] += len(mapped)
            sent = await _publish(video_id, mapped)
            stats["comments_published"] += sent
            await _update_last_job(replies=len(mapped), published=sent)
            cursor = next_cursor(data)
            if cursor is None:
                return True
        return True

    async def crawl(job: dict[str, Any]) -> None:
        video_id = job["video_id"]
        cursor = str(job.get("cursor") or "0")
        for _ in range(_MAX_PAGES_PER_VIDEO - int(job.get("pages") or 0)):
            if halted():
                return
            data = await get("comments", {"video": video_id, "cursor": cursor})
            if data is None and budget.exhausted:
                return
            raw = list((data or {}).get("comments") or [])
            mapped = [c for c in (map_comment(r) for r in raw) if c]
            stats["comments"] += len(mapped)
            sent = await _publish(video_id, mapped)
            stats["comments_published"] += sent
            await _update_last_job(comments=len(mapped), published=sent)
            if settings.omar_comments_replies:
                # Bình luận đã lấy hết trả lời ở trang này - hết ngân sách giữa trang thì lượt sau làm lại trang nhưng
                # bỏ qua chúng (bình luận đã gửi thì dedupe bỏ qua).
                replied = set(job.get("replied") or [])
                for comment in mapped:
                    if int(comment["reply_count"] or 0) <= 0 or comment["comment_id"] in replied:
                        continue
                    if not await replies_of(video_id, comment["comment_id"]):
                        return
                    replied.add(comment["comment_id"])
                    job["replied"] = sorted(replied)
                    await redis.hset(JOBS_KEY, video_id, json.dumps(job))
            following = next_cursor(data or {})
            job.update(
                pages=int(job.get("pages") or 0) + 1, comments=int(job.get("comments") or 0) + len(mapped), replied=[]
            )
            if following is None:
                break
            cursor = following
            job["cursor"] = cursor
            await redis.hset(JOBS_KEY, video_id, json.dumps(job))
        await redis.hdel(JOBS_KEY, video_id)
        stats["comment_videos"] += 1
        await _update_last_job()
        await d1_query(
            "UPDATE posts SET comments_crawled_at = ?, comments_crawled_replies = ?, comments_crawl_reason = ? WHERE id = ?",
            [
                datetime.now(tz=UTC).isoformat(),
                job.get("reply_count") or 0,
                job.get("reason") or "omar_weekly",  # "omar_trending": xếp hàng từ omar_trending (enqueue_videos)
                job["post_id"],
            ],
            quiet=True,
        )

    async def worker() -> None:
        while not queue.empty() and not halted():
            job = queue.get_nowait()
            try:
                await crawl(job)
            except OmarStop as exc:
                stopped.append(exc)
            except Exception as exc:  # noqa: BLE001 - một video lỗi: giữ trong hàng đợi, lượt sau thử lại
                stats["failed"] += 1
                logger.warning("omar_comments_video_failed", video_id=job["video_id"], error=str(exc)[:200])

    await asyncio.gather(*(worker() for _ in range(max(1, min(settings.omar_concurrency, len(jobs))))))
    logger.info(
        "omar_comments_processed",
        videos=stats["comment_videos"],
        requests=stats["comment_requests"],
        comments=stats["comments"],
        replies=stats["comment_replies"],
        published=stats["comments_published"],
        pending=await redis.hlen(JOBS_KEY),
    )
    if stopped:
        raise stopped[0]
