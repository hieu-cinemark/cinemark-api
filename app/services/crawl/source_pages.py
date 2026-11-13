"""Trang nguồn Facebook - cào theo nguồn, tự động hoàn toàn.

Mỗi dòng source_pages là một Page/Profile Facebook, hai loại:
- movie_id có giá trị: trang CỦA một phim (fanpage chính thức) - mọi bài là về phim đó, ingest lưu là liên quan;
- movie_id NULL: trang CHUNG (CGV, Moveek, trang review) - ingest tìm phim đang theo dõi mà bài nêu tên rồi lọc liên quan
  như bài tìm kiếm của phim đó; bài không nêu phim nào thì bỏ.

Tự động: review_suggestions() (mỗi sáng, sau hashtag/query discovery) cho Kira duyệt các trang gợi ý - tác giả Facebook
có nhiều bài liên quan nhất 30 ngày qua (suggestions()) kèm bài mẫu của chính trang - thành "trang của phim X", "trang
chung" hoặc loại; trang được duyệt tự thêm vào danh sách (added_by='ai').

Tần suất: lượt cào Facebook theo lịch gọi queue_enabled_sources(), chỉ xếp trang đã quá hạn - trang của phim mỗi
FILM_INTERVAL_HOURS, trang chung mỗi SHARED_INTERVAL_HOURS; lượt cào trước không có bài mới (spider ghi
page_feed_last:<url> trong Redis) thì giãn gấp đôi, tối đa x MAX_BACKOFF. Spider facebook_page_feed đọc bảng tin mới nhất
trước, dừng ở bài đã có / cũ hơn SINCE_DAYS / MAX_PAGES trang.

Bài cần keyword_id để ingest biết phim: trang của phim dùng từ khoá Facebook là tên phim (get_or_create_keyword).
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import Column, Integer, Table, Text, delete, func, select, update
from sqlalchemy.dialects.sqlite import insert

from app.ai.runtime import call_llm, parse_json_response
from app.clients.kafka import publish_action_request
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.errors import NotFoundError, ValidationError
from app.core.logging import get_logger
from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec, metadata
from app.repositories.d1.store import d1_query, get_or_create_keyword
from app.repositories.d1.tables import movies
from app.repositories.d1.tables import posts as posts_table
from app.services.crawl.task_queue import enqueue_published, list_pending
from app.services.relevance.relevance_rules import normalize_title

logger = get_logger(__name__)

# Schema tạo/bổ sung ở _ensure_table.
source_pages = Table(
    "source_pages",
    metadata,
    Column("id", Text, primary_key=True),
    *(Column(name, Text) for name in ("platform", "url", "page_id", "name", "movie_id")),
    Column("enabled", Integer),
    *(Column(name, Text) for name in ("last_queued_at", "created_at", "added_by", "ai_reason")),
    Column("quiet_runs", Integer),
)
decisions = Table(
    "source_page_decisions",
    metadata,
    Column("url", Text, primary_key=True),
    *(Column(name, Text) for name in ("verdict", "movie_id")),
    Column("confidence", Integer),
    *(Column(name, Text) for name in ("reason", "decided_at")),
)
_ENABLED_SOURCES = (
    select(source_pages)
    .select_from(source_pages.outerjoin(movies, movies.c.id == source_pages.c.movie_id))
    .where(
        source_pages.c.platform == "facebook",
        source_pages.c.enabled == 1,
        (source_pages.c.movie_id.is_(None)) | (movies.c.enabled == 1),
    )
)


async def _set_source(source_id: str, *, quiet: bool = False, **values: Any) -> None:
    await d1_exec(update(source_pages).where(source_pages.c.id == source_id).values(**values), quiet=quiet)


async def _get_source(source_id: str) -> dict[str, Any] | None:
    rows = await d1_exec(select(source_pages).where(source_pages.c.id == source_id))
    return rows[0] if rows else None


async def _movie_title(movie_id: str) -> str | None:
    rows = await d1_exec(select(movies.c.title).where(movies.c.id == movie_id), quiet=True)
    return rows[0]["title"] if rows else None


# Mỗi trang bảng tin xin 10 bài (spider) - 50 trang ~ 500 bài; dừng sớm ở bài đã có nên lượt sau chỉ tốn vài trang.
MAX_PAGES = 50
SINCE_DAYS = 60
FILM_INTERVAL_HOURS = 6
SHARED_INTERVAL_HOURS = 24
MAX_BACKOFF = 8
# chỉ còn 2 tài khoản FB, mà tìm kiếm theo từ khoá phim vẫn phải đi trước - trang nguồn chỉ được lấp chỗ
# trống: mỗi lượt xếp tối đa MAX_QUEUE_PER_ROUND trang (trang của phim / trang hay có bài trước), và bỏ hẳn lượt khi còn
# từ MAX_PENDING_PAGE_FEED job page_feed của lượt trước chưa chạy (hàng đợi đang dồn).
MAX_QUEUE_PER_ROUND = 40
MAX_PENDING_PAGE_FEED = 10
# AI duyệt trang gợi ý
REVIEW_TASK = "source_page_review"
REVIEW_BATCH = 10
REVIEW_MIN_CONFIDENCE = 75
MAX_ADDS_PER_RUN = 20
RECHECK_REJECTED_DAYS = 14
SAMPLE_POSTS_PER_PAGE = 3
SUGGEST_WINDOW_DAYS = 30
SUGGEST_PER_MOVIE = 5
SUGGEST_MIN_POSTS = 5
SUGGEST_SHARED = 30

_TABLE_READY = False
# Đường dẫn không phải trang (facebook.com/groups/..., /watch, /reel...) - không gợi ý.
_NOT_A_PAGE = {
    "groups",
    "watch",
    "reel",
    "reels",
    "photo",
    "photo.php",
    "story.php",
    "permalink.php",
    "share",
    "events",
    "stories",
}


async def _ensure_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    await d1_query(
        """CREATE TABLE IF NOT EXISTS source_pages (
            id TEXT PRIMARY KEY, platform TEXT NOT NULL, url TEXT NOT NULL, page_id TEXT, name TEXT,
            movie_id TEXT, enabled INTEGER NOT NULL DEFAULT 1, last_queued_at TEXT, created_at TEXT NOT NULL
        )""",
        quiet=True,
    )
    cols = {row.get("name") for row in await d1_query("PRAGMA table_info(source_pages)", quiet=True) or []}
    for column, kind in (("added_by", "TEXT"), ("ai_reason", "TEXT"), ("quiet_runs", "INTEGER NOT NULL DEFAULT 0")):
        if column not in cols:
            await d1_query(f"ALTER TABLE source_pages ADD COLUMN {column} {kind}", quiet=True)
    await d1_query(
        """CREATE TABLE IF NOT EXISTS source_page_decisions (
            url TEXT PRIMARY KEY, verdict TEXT NOT NULL, movie_id TEXT, confidence INTEGER, reason TEXT, decided_at TEXT NOT NULL
        )""",
        quiet=True,
    )
    _TABLE_READY = True


_THREADS_HANDLE = re.compile(r"threads\.(?:com|net)/@([A-Za-z0-9._]{1,30})", re.IGNORECASE)


def is_threads_url(url: str) -> bool:
    return bool(_THREADS_HANDLE.search(url or ""))


def normalize_threads_url(url: str) -> str:
    """https://www.threads.com/@<handle> (chữ thường, bỏ /post/... và query)."""
    match = _THREADS_HANDLE.search(url or "")
    if not match:
        raise ValidationError("Đường dẫn kênh Threads phải có dạng threads.com/@ten_kenh")
    return f"https://www.threads.com/@{match.group(1).lower()}"


def normalize_page_url(url: str) -> str:
    """https://www.facebook.com/<trang> (bỏ query/đường dẫn con, giữ profile.php?id=...)."""
    raw = url.strip()
    if not raw:
        raise ValidationError("Thiếu đường dẫn trang")
    if not raw.startswith("http"):
        raw = f"https://www.facebook.com/{raw.lstrip('@/')}"
    parsed = urlparse(raw)
    if "facebook.com" not in parsed.netloc:
        raise ValidationError("Chỉ nhận trang Facebook (facebook.com/...)")
    if parsed.path.rstrip("/") == "/profile.php":
        match = re.search(r"(?:^|&)id=(\d+)", parsed.query)
        if not match:
            raise ValidationError("Đường dẫn profile.php thiếu id")
        return f"https://www.facebook.com/profile.php?id={match.group(1)}"
    slug = parsed.path.strip("/").split("/")[0]
    if not slug or slug in _NOT_A_PAGE:
        raise ValidationError("Đường dẫn không phải một trang/Profile")
    return f"https://www.facebook.com/{slug}"


async def list_sources() -> list[dict[str, Any]]:
    await _ensure_table()
    rows = await d1_exec(
        select(source_pages, movies.c.title.label("movie_title"))
        .select_from(source_pages.outerjoin(movies, movies.c.id == source_pages.c.movie_id))
        .order_by(movies.c.title, source_pages.c.created_at),
        quiet=True,
    )
    return rows or []


async def add_source(
    *,
    url: str,
    movie_id: str | None = None,
    name: str | None = None,
    added_by: str = "manual",
    reason: str | None = None,
) -> dict[str, Any]:
    """movie_id None = trang chung (nhiều phim). Đường dẫn threads.com/@... là kênh Threads (đọc không đăng nhập)."""
    await _ensure_table()
    platform = "threads" if is_threads_url(url) else "facebook"
    page_url = normalize_threads_url(url) if platform == "threads" else normalize_page_url(url)
    movie_id = (movie_id or "").strip() or None
    if movie_id and await _movie_title(movie_id) is None:
        raise NotFoundError("Không tìm thấy phim")
    existing = await d1_exec(
        select(source_pages.c.id).where(
            source_pages.c.platform == platform,
            source_pages.c.url == page_url,
            func.coalesce(source_pages.c.movie_id, "") == (movie_id or ""),
        )
    )
    if existing:
        raise ValidationError("Trang này đã có trong danh sách")
    source_id = f"src_{uuid.uuid4()}"
    await d1_exec(
        insert(source_pages).values(
            id=source_id,
            platform=platform,
            url=page_url,
            name=(name or "").strip() or None,
            movie_id=movie_id,
            enabled=1,
            created_at=datetime.now(tz=UTC).isoformat(),
            added_by=added_by,
            ai_reason=reason,
        )
    )
    logger.info("source_page_added", platform=platform, url=page_url, movie_id=movie_id)
    row = await _get_source(source_id)
    if not row:
        raise NotFoundError(f"Source page {source_id} not found after insert")
    return row


async def set_enabled(source_id: str, enabled: bool) -> None:
    await _ensure_table()
    await _set_source(source_id, enabled=1 if enabled else 0)


async def delete_source(source_id: str) -> None:
    await _ensure_table()
    await d1_exec(delete(source_pages).where(source_pages.c.id == source_id))


async def queue_source(
    source: dict[str, Any], *, bypass_drain: bool = False, overrides: dict[str, Any] | None = None
) -> bool:
    """Xếp một yêu cầu page_feed cho trang (từ khoá Facebook là tên phim làm keyword_id). `overrides`: tham số job riêng của
    chỗ gọi, vd. lượt quét lại chỉ đọc trang mới nhất (app/services/crawl/watch_pages.py)."""
    keyword_id = None
    if source.get("movie_id"):
        title = await _movie_title(source["movie_id"])
        if title is None:
            return False
        keyword = await get_or_create_keyword(source["movie_id"], "facebook", title)
        if keyword is None:
            return False
        keyword_id = keyword["id"]
    ok = await publish_action_request(
        "facebook",
        "page_feed",
        {
            "page_url": source["url"],
            "page_id": source.get("page_id") or _page_id_from_url(source["url"]),
            # None với trang chung - ingest tự tìm phim theo nội dung bài.
            "keyword_id": keyword_id,
            "max_pages": MAX_PAGES,
            "since_days": SINCE_DAYS,
            "run_id": str(uuid.uuid4()),
            **(overrides or {}),
        },
        bypass_drain=bypass_drain,
    )
    if ok:
        await _set_source(source["id"], last_queued_at=datetime.now(tz=UTC).isoformat())
    return ok


def _page_id_from_url(url: str) -> str | None:
    """Trang kiểu mới (facebook.com/profile.php?id=615...) mang sẵn id trong URL - không phải tốn một request dò id."""
    match = re.search(r"[?&]id=(\d{6,})", url or "")
    return match.group(1) if match else None


def _due(source: dict[str, Any], now: datetime) -> bool:
    base = FILM_INTERVAL_HOURS if source.get("movie_id") else SHARED_INTERVAL_HOURS
    interval = base * min(MAX_BACKOFF, 2 ** int(source.get("quiet_runs") or 0))
    last = str(source.get("last_queued_at") or "")
    try:
        return now - datetime.fromisoformat(last) >= timedelta(hours=interval) - timedelta(minutes=30)
    except ValueError:
        return True


async def _update_quiet_runs(source: dict[str, Any]) -> None:
    """Lượt cào trước của trang không có bài mới (spider ghi page_feed_last:<url>) -> tăng quiet_runs (giãn lịch), có
    bài -> về 0."""
    try:
        raw = await get_redis_client().get(f"{REDIS_KEY_PREFIX}page_feed_last:{source['url']}")
    except Exception:  # noqa: BLE001 - không đọc được thì giữ nhịp cũ
        return
    if not raw:
        return
    last = json.loads(raw)
    if last.get("page_id") and not source.get("page_id"):
        # Spider đã dò ra id của trang - lưu lại để các lượt sau không tốn request dò lại.
        await _set_source(source["id"], page_id=str(last["page_id"]), quiet=True)
        source["page_id"] = str(last["page_id"])
    quiet = 0 if int(last.get("new") or 0) > 0 else int(source.get("quiet_runs") or 0) + 1
    if quiet != int(source.get("quiet_runs") or 0):
        await _set_source(source["id"], quiet_runs=quiet, quiet=True)
        source["quiet_runs"] = quiet


async def queue_enabled_sources() -> int:
    """Gọi từ mỗi lượt cào Facebook theo lịch: xếp các trang đang bật đã tới hạn (xem docstring module)."""
    await _ensure_table()
    rows = await d1_exec(_ENABLED_SOURCES, quiet=True)
    now = datetime.now(tz=UTC)
    queued = 0
    due = []
    for row in rows or []:
        await _update_quiet_runs(row)
        if _due(row, now):
            due.append(row)
    # hạn mức request FB/ngày - trang hay có bài mới (quiet_runs thấp) và trang của phim xếp trước.
    due.sort(key=lambda row: (int(row.get("quiet_runs") or 0), 0 if row.get("movie_id") else 1))
    pending = [item for item in await list_pending("facebook") if item.get("type") == "page_feed"]
    if len(pending) >= MAX_PENDING_PAGE_FEED:
        logger.info("source_pages_skipped_backlog", pending_page_feed=len(pending), due=len(due))
        return 0
    for row in due[:MAX_QUEUE_PER_ROUND]:
        if await queue_source(row):
            queued += 1
    logger.info("source_pages_queued", queued=queued, total=len(rows or []))
    return queued


# Đọc page không đăng nhập: chạy liên tục kể cả khi không có tài khoản (spider-hub job public_page, worker song song).
_PUBLIC_QUEUE_KEY = f"{REDIS_KEY_PREFIX}crawl_public_pages:facebook"
_PUBLIC_PAUSED_KEY = f"{REDIS_KEY_PREFIX}facebook:public_pages:paused"  # spider-hub đặt khi hết proxy dùng được
_PUBLIC_PLAN_REFRESH = timedelta(minutes=10)
_public_plan: list[dict[str, Any]] = []
_public_plan_at: datetime | None = None


async def _refresh_public_plan(now: datetime) -> None:
    """Page cần đọc: tới hạn theo lịch trang nguồn trước, rồi mọi page đã xếp quá min_gap (cũ nhất trước) để worker không rảnh.
    Đọc lại tối đa mỗi _PUBLIC_PLAN_REFRESH, không phải mỗi page: D1 tính phí theo dòng đọc."""
    global _public_plan, _public_plan_at
    if _public_plan or (_public_plan_at and now - _public_plan_at < _PUBLIC_PLAN_REFRESH):
        return
    await _ensure_table()
    rows = await d1_exec(_ENABLED_SOURCES, quiet=True)
    min_gap = timedelta(minutes=settings.facebook_public_pages_min_gap_minutes)
    plan = []
    for row in rows or []:
        await _update_quiet_runs(row)
        last = str(row.get("last_queued_at") or "")
        try:
            ready = now - datetime.fromisoformat(last) >= min_gap
        except ValueError:
            ready = True
        if ready or _due(row, now):
            plan.append(row)
    plan.sort(
        key=lambda row: (
            0 if _due(row, now) else 1,
            int(row.get("quiet_runs") or 0),
            0 if row.get("movie_id") else 1,
            str(row.get("last_queued_at") or ""),
        )
    )
    _public_plan, _public_plan_at = plan, now
    logger.info("public_pages_plan", ready=len(plan), total=len(rows or []))


async def _public_since_days(url: str, now: datetime) -> int | None:
    """Lần đọc trước đã đi hết khoảng ngày thì lần này chỉ cần phần mới (cộng 1 ngày phòng bài đăng trễ). None: để job dùng
    khoảng mặc định, gồm cả khi lần trước dừng giữa chừng vì bị chặn. Bài đã thấy không được gửi lại nên đi lại toàn bộ là phí."""
    try:
        raw = await get_redis_client().get(f"{REDIS_KEY_PREFIX}page_feed_last:{url}")
        last = json.loads(raw) if raw else {}
    except Exception:  # noqa: BLE001 - không đọc được thì đọc đủ khoảng ngày
        return None
    if last.get("pagination_status") or last.get("stop_reason") not in ("exhausted", "since_days", "no_new_posts"):
        return None
    if not last.get("at"):
        return None
    days = (now.timestamp() - float(last["at"])) / 86400
    return min(SINCE_DAYS, int(days) + 2)


async def fill_public_pages_queue(now: datetime | None = None) -> int:
    """Gọi từ mỗi nhịp scheduler: thấy hàng đợi vơi là nạp đủ facebook_public_pages_queue_target, trả số page đã xếp. Spider-hub
    không chạy thì hàng đợi đầy và không xếp thêm; last_queued_at chỉ ghi khi đã xếp nên page không bị tính là đã đọc."""
    now = now or datetime.now(tz=UTC)
    if not settings.facebook_public_pages_enabled:
        return 0
    redis = get_redis_client()
    if await redis.exists(_PUBLIC_PAUSED_KEY):
        return 0
    room = settings.facebook_public_pages_queue_target - await redis.llen(_PUBLIC_QUEUE_KEY)
    if room <= 0:
        return 0
    await _refresh_public_plan(now)
    queued = 0
    while queued < room and _public_plan:
        source = _public_plan.pop(0)
        keyword_id = None
        if source.get("movie_id"):
            title = await _movie_title(source["movie_id"])
            keyword = await get_or_create_keyword(source["movie_id"], "facebook", title) if title else None
            if keyword is None:
                continue
            keyword_id = keyword["id"]
        request: dict[str, Any] = {
            "type": "public_page",
            "platform": "facebook",
            "page_url": source["url"],
            "keyword_id": keyword_id,
            "run_id": str(uuid.uuid4()),
            "published_at": now.timestamp(),
        }
        since_days = await _public_since_days(source["url"], now)
        if since_days:
            request["since_days"] = since_days
        await enqueue_published(request)  # hiện trên trang Hàng đợi
        await redis.lpush(_PUBLIC_QUEUE_KEY, json.dumps(request, ensure_ascii=False))
        await _set_source(source["id"], last_queued_at=now.isoformat(), quiet=True)
        queued += 1
    return queued


async def run_source(source_id: str) -> bool:
    await _ensure_table()
    source = await _get_source(source_id)
    if not source:
        raise NotFoundError("Không tìm thấy trang nguồn")
    if source.get("platform") == "threads":
        from app.services.crawl.threads_channels import queue_channel

        return await queue_channel(source, datetime.now(tz=UTC))
    return await queue_source(source, bypass_drain=True)


def _page_from_post_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return normalize_page_url(url)
    except ValidationError:
        return None


async def suggestions() -> list[dict[str, Any]]:
    """Trang Facebook có nhiều bài liên quan nhất của từng phim trong SUGGEST_WINDOW_DAYS ngày, chưa có trong danh sách."""
    await _ensure_table()
    since = (datetime.now(tz=UTC) - timedelta(days=SUGGEST_WINDOW_DAYS)).isoformat()
    p, count = posts_table.alias("p"), func.count()
    rows = (
        await d1_exec(
            select(
                p.c.movie_id,
                movies.c.title.label("movie_title"),
                p.c.author,
                p.c.url,
                count.label("posts"),
                func.sum(
                    func.coalesce(p.c.like_count, 0)
                    + func.coalesce(p.c.reply_count, 0)
                    + func.coalesce(p.c.repost_count, 0)
                ).label("engagement"),
            )
            .select_from(p.join(movies, movies.c.id == p.c.movie_id))
            .where(
                p.c.platform == "facebook",
                movies.c.enabled == 1,
                p.c.posted_at >= since,
                relevant_post(p),
                p.c.author.is_not(None),
                p.c.author != "",
            )
            .group_by(p.c.movie_id, p.c.author)
            .having(count >= SUGGEST_MIN_POSTS)
            .order_by(p.c.movie_id, count.desc()),
            quiet=True,
            timeout=60.0,
        )
        or []
    )
    sources = await list_sources()
    taken_film = {(row["movie_id"], row["url"]) for row in sources if row.get("movie_id")}
    taken_shared = {row["url"] for row in sources if not row.get("movie_id")}
    film_pages: list[dict[str, Any]] = []
    shared: dict[str, dict[str, Any]] = {}
    per_movie: dict[str, int] = {}
    for row in rows:
        page_url = _page_from_post_url(row.get("url"))
        if not page_url:
            continue
        # Tên trang chứa tên phim -> trang của phim đó; còn lại (CGV, trang review, cá nhân) -> trang chung, gộp mọi phim.
        own_page = normalize_title(row["movie_title"]).replace(" ", "") in normalize_title(row["author"]).replace(
            " ", ""
        )
        posts, engagement = int(row["posts"] or 0), int(row["engagement"] or 0)
        if own_page:
            if (row["movie_id"], page_url) in taken_film or per_movie.get(row["movie_id"], 0) >= SUGGEST_PER_MOVIE:
                continue
            per_movie[row["movie_id"]] = per_movie.get(row["movie_id"], 0) + 1
            film_pages.append(
                {
                    "kind": "film",
                    "movie_id": row["movie_id"],
                    "movie_title": row["movie_title"],
                    "name": row["author"],
                    "url": page_url,
                    "posts": posts,
                    "engagement": engagement,
                    "films": [row["movie_title"]],
                }
            )
            continue
        if page_url in taken_shared:
            continue
        entry = shared.setdefault(
            page_url,
            {
                "kind": "shared",
                "movie_id": None,
                "movie_title": None,
                "name": row["author"],
                "url": page_url,
                "posts": 0,
                "engagement": 0,
                "films": [],
            },
        )
        entry["posts"] += posts
        entry["engagement"] += engagement
        entry["films"].append(row["movie_title"])
    # Trang chung đáng cào là trang nói về nhiều phim đang theo dõi.
    shared_pages = sorted((e for e in shared.values() if len(e["films"]) >= 2), key=lambda e: -e["posts"])[
        :SUGGEST_SHARED
    ]
    out = film_pages + shared_pages
    return out


REVIEW_PROMPT = """Bạn duyệt Trang Facebook làm NGUỒN CÀO bài cho hệ thống theo dõi thảo luận phim chiếu rạp tại Việt Nam.
Mỗi trang có: tên, đường dẫn, các phim đang theo dõi mà trang đã đăng bài, và vài bài mẫu của chính trang.
Phân loại từng trang:
- "film": trang dành riêng cho MỘT phim (fanpage chính thức, trang phim của nhà phát hành) - ghi movie là đúng tên phim đó
  trong danh sách phim của trang. Chỉ chọn khi gần như mọi bài của trang đều về phim đó.
- "shared": trang đăng về NHIỀU phim chiếu rạp (chuỗi rạp, nhà phát hành nhiều phim, trang tin/review/meme điện ảnh).
- "reject": trang không phải về phim chiếu rạp (thương hiệu/sản phẩm trùng tên phim, game, ca sĩ, trang bán hàng), trang cá
  nhân ít bài, hoặc trang không nói tiếng Việt.
Không chắc thì "reject". Trả JSON hợp lệ, không bọc code block:
{"results":[{"url":"<đúng như được đưa>","verdict":"film|shared|reject","movie":"<tên phim khi film, ngược lại null>",
"confidence":0-100,"reason":"một câu ngắn tiếng Việt"}]}"""


async def _sample_posts(name: str) -> list[str]:
    rows = (
        await d1_exec(
            select(func.substr(posts_table.c.content, 1, 220).label("content"))
            .where(
                posts_table.c.platform == "facebook",
                posts_table.c.author == name,
                func.length(posts_table.c.content) >= 20,
            )
            .order_by(posts_table.c.posted_at.desc())
            .limit(SAMPLE_POSTS_PER_PAGE),
            quiet=True,
        )
        or []
    )
    return [" ".join(str(row["content"]).split()) for row in rows]


async def _decided_urls() -> set[str]:
    recheck = (datetime.now(tz=UTC) - timedelta(days=RECHECK_REJECTED_DAYS)).isoformat()
    rows = await d1_exec(select(decisions.c.url, decisions.c.verdict, decisions.c.decided_at), quiet=True) or []
    return {row["url"] for row in rows if row["verdict"] != "reject" or str(row["decided_at"]) >= recheck}


async def pending_suggestions() -> list[dict[str, Any]]:
    """Gợi ý mà Kira chưa quyết định (cho dashboard)."""
    await _ensure_table()
    decided = await _decided_urls()
    return [s for s in await suggestions() if s["url"] not in decided]


async def review_suggestions(*, dry_run: bool = False, limit: int | None = None) -> dict[str, int]:
    """Kira duyệt các trang gợi ý chưa quyết định; trang film/shared đủ tin cậy tự thêm vào danh sách (xem docstring)."""
    await _ensure_table()
    decided = await _decided_urls()
    candidates = [s for s in await suggestions() if s["url"] not in decided][:limit]
    stats = {"candidates": len(candidates), "asked": 0, "added": 0, "rejected": 0, "failed": 0}
    titles = {
        normalize_title(row["title"]): row["id"]
        for row in await d1_exec(select(movies.c.id, movies.c.title).where(movies.c.enabled == 1), quiet=True) or []
    }
    for start in range(0, len(candidates), REVIEW_BATCH):
        if stats["added"] >= MAX_ADDS_PER_RUN:
            break
        batch = candidates[start : start + REVIEW_BATCH]
        blocks = []
        for cand in batch:
            samples = await _sample_posts(cand["name"])
            blocks.append(
                f"- url: {cand['url']}\n  name: {cand['name']}\n  films: {', '.join(cand['films'])}\n"
                f"  on-topic posts (30 days): {cand['posts']}\n  sample posts:\n"
                + "\n".join(f"    * {text}" for text in samples)
            )
        try:
            raw = await call_llm(
                task=REVIEW_TASK, system_prompt=REVIEW_PROMPT, user_prompt="\n".join(blocks), max_tokens=6000
            )
        except Exception as exc:  # noqa: BLE001 - Kira tắt/lỗi: lượt sau xét lại
            stats["failed"] += 1
            logger.warning("source_page_review_failed", error=str(exc)[:300])
            continue
        stats["asked"] += 1
        parsed = parse_json_response(raw)
        results = parsed.get("results") if isinstance(parsed, dict) else None
        by_url = {str(r.get("url")): r for r in results if isinstance(r, dict)} if isinstance(results, list) else {}
        for cand in batch:
            verdict = by_url.get(cand["url"])
            if not verdict:
                continue
            kind = str(verdict.get("verdict") or "").lower()
            try:
                confidence = int(float(verdict.get("confidence") or 0))
            except TypeError, ValueError:
                confidence = 0
            movie_id = titles.get(normalize_title(str(verdict.get("movie") or ""))) if kind == "film" else None
            accept = (
                kind in ("film", "shared")
                and confidence >= REVIEW_MIN_CONFIDENCE
                and (kind == "shared" or bool(movie_id))
            )
            reason = str(verdict.get("reason") or "")[:300]
            logger.info(
                "source_page_ai_decision",
                url=cand["url"],
                name=cand["name"],
                verdict=kind,
                confidence=confidence,
                movie_id=movie_id,
                accept=accept,
                dry_run=dry_run,
                reason=reason,
            )
            if dry_run:
                stats["added" if accept else "rejected"] += 1
                continue
            stmt = insert(decisions).values(
                url=cand["url"],
                verdict=kind if accept else "reject",
                movie_id=movie_id,
                confidence=confidence,
                reason=reason,
                decided_at=datetime.now(tz=UTC).isoformat(),
            )
            await d1_exec(
                stmt.on_conflict_do_update(
                    index_elements=[decisions.c.url],
                    set_={c.name: stmt.excluded[c.name] for c in decisions.columns if c.name != "url"},
                ),
                quiet=True,
            )
            if not accept or stats["added"] >= MAX_ADDS_PER_RUN:
                stats["rejected"] += 0 if accept else 1
                continue
            try:
                await add_source(url=cand["url"], movie_id=movie_id, name=cand["name"], added_by="ai", reason=reason)
                stats["added"] += 1
            except (ValidationError, NotFoundError) as exc:
                logger.info("source_page_ai_add_skipped", url=cand["url"], error=str(exc))
    logger.info("source_page_review_finished", telegram=bool(stats["added"]) and not dry_run, dry_run=dry_run, **stats)
    return stats
