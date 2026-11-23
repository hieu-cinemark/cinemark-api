"""Review Angles: đọc dần bài đăng từ Premiere và gom thành các góc nhìn về phim.

Mỗi lô 24 bài của một phim; AI gom vào góc đã có hoặc tạo góc mới. Phim ưu tiên (Redis `REVIEW_FOCUS_KEY`) đọc liên tục;
còn lại xoay vòng các phim đã qua Premiere trong 60 ngày, mỗi lượt đọc liên tiếp tới TICK_SECONDS, tối đa
settings.review_angles_daily_batches lô/ngày.

Thứ tự đọc (16/11/2026 - trước đó một con trỏ đi từ Premiere tới nay; bài mới về nhanh hơn tốc độ đọc nên sau 3 tuần
Chị Chị Em Em 3 mới đọc tới 2 ngày sau Premiere). Theo `posted_at`, state giữ ba mốc:
  - `head`: bài đăng từ đây trở đi là bài mới - đọc TRƯỚC, tăng dần; mỗi giờ quét lại 48 giờ dưới `head` (bài về muộn);
  - `cursor` .. `tail`: khoảng chưa đọc (cursor là con trỏ cũ) - đọc lùi từ `tail` về `cursor` khi hết bài mới, để các
    tuần gần đây có trước. `tail` <= `cursor` là đã đọc hết. Trang hiện khoảng này (Worker filmReviewAngles.ts).

YouTube (16/11/2026): đọc tiêu đề + mô tả + đoạn đầu transcript (bảng post_transcripts).

Kết quả ghi R2 `analysis/review-angles/v2/<movieId>/state.json` và `page-N.json` (khoá camelCase vì Worker đọc trực
tiếp).
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

from botocore.exceptions import ClientError
from sqlalchemy import func, literal_column, select

from app.ai.prompts.review_angles import REVIEW_ANGLES_SYSTEM, REVIEW_ASPECTS
from app.ai.runtime import call_llm_json
from app.ai.tasks.review_angles import angle_id_of, has_text, js_json, parse_batch
from app.clients import lake
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.post_filters import relevant_post
from app.repositories.d1.sql import d1_exec, json_values
from app.repositories.d1.tables import movie_measure_settings, movies
from app.repositories.d1.tables import posts as posts_table
from app.repositories.d1.transcripts import load_texts as load_transcripts
from app.services.ai_jobs.channels import channel_of_post, channel_url

logger = get_logger(__name__)

BATCH_SIZE = 24
CAPTION_CHARS = 4000
NEW_SCAN_LIMIT = 200
LATE_WINDOW = timedelta(hours=48)
LATE_SCAN_EVERY_MS = 3_600_000
LATE_SCAN_LIMIT = 5000
# Lần đầu chạy bản ba mốc: bài trong chừng này giờ gần nhất tính là bài mới, phần trước đó là khoảng đọc lùi.
NEW_WINDOW = timedelta(hours=24)
ACTIVE_DAYS = 60
# YouTube: caption gửi AI = tiêu đề + mô tả (cắt) + đoạn đầu transcript.
YOUTUBE_TEXT_CHARS = 1500
TRANSCRIPT_CHARS = 2500
PLATFORMS = ("facebook", "tiktok", "threads", "youtube")
MAX_BATCH_FAILURES = 3
# Góc nhìn tối đa mỗi khía cạnh (prompt nói 8, ở đây chừa dư): quá số này thì bài được gán vào góc gần nhất thay vì thêm góc mới -
# chặn việc danh sách góc phình ra từng lô và vụn thành hàng chục góc gần nghĩa.
MAX_ANGLES_PER_ASPECT = 10
FOCUS_TICK_SECONDS = 4 * 60
# Lượt thường (scheduler/ai_jobs.py gọi mỗi ~200 giây, không chồng lượt): đọc liên tiếp tới chừng này giây.
TICK_SECONDS = 170
VN = timedelta(hours=7)

_PREFIX = f"{REDIS_KEY_PREFIX}review_angles:"
REVIEW_FOCUS_KEY = f"{_PREFIX}focus"


# --- hàm thuần ---


def vn_day(at: datetime | None = None) -> str:
    return ((at or datetime.now(tz=UTC)) + VN).date().isoformat()


def premiere_start(day: str) -> str:
    """Đầu ngày VN theo UTC, dạng posted_at đã lưu (giây, +00:00)."""
    start = datetime.fromisoformat(f"{day}T00:00:00+07:00").astimezone(UTC)
    return start.isoformat(timespec="seconds")


def _valid_day(value: Any) -> str | None:
    day = str(value or "")[:10]
    try:
        return date.fromisoformat(day).isoformat() if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day) else None
    except ValueError:
        return None


# --- lưu trữ ---


def angles_dir(movie_id: str) -> str:
    return f"analysis/review-angles/v2/{movie_id}"


async def read_json(key: str) -> Any:
    try:
        return json.loads(await lake.get_object(key))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


async def write_json(key: str, value: Any) -> None:
    await lake.put_object(key, json.dumps(value, ensure_ascii=False).encode(), "application/json")


def empty_state() -> dict[str, Any]:
    return {
        "version": 2,
        "cursor": None,
        "head": None,
        "tail": None,
        "tailIds": [],
        "recent": {},
        "lateScanAt": 0,
        "pages": 0,
        "analyzed": {},
        "sources": {},
        "updatedAt": None,
        "model": None,
        "angles": [],
    }


_movies: dict[str, tuple[float, dict[str, Any] | None]] = {}
_M, _S, _P = movies.alias("m"), movie_measure_settings.alias("s"), posts_table.alias("p")
_MOVIE_DATES = _M.outerjoin(_S, _S.c.movie_id == _M.c.id)
# Bài của phim trên các nền tảng Review Angles đọc, theo thời gian đăng.
_SCAN_BASE = select(_P.c.id, _P.c.platform, _P.c.posted_at).where(_P.c.platform.in_(PLATFORMS), relevant_post(_P))
_SCAN = _SCAN_BASE.order_by(_P.c.posted_at, _P.c.id)
_SCAN_DESC = _SCAN_BASE.order_by(_P.c.posted_at.desc(), _P.c.id.desc())


async def load_movie(movie_id: str) -> dict[str, Any] | None:
    cached = _movies.get(movie_id)
    if cached and time.monotonic() - cached[0] < 3600:
        return cached[1]
    rows = await d1_exec(
        select(_M.c.title, _M.c.released_at, _S.c.premiere_date)
        .select_from(_MOVIE_DATES)
        .where(_M.c.id == movie_id)
        .limit(1)
    )
    if rows is None:
        return cached[1] if cached else None
    movie = None
    if rows:
        premiere = _valid_day(rows[0].get("premiere_date"))
        release = _valid_day(rows[0].get("released_at"))
        movie = {"title": rows[0]["title"], "premiereDate": premiere or release}
    _movies[movie_id] = (time.monotonic(), movie)
    return movie


def _available(movie: dict[str, Any] | None) -> bool:
    return bool(movie and movie["premiereDate"] and movie["premiereDate"] <= vn_day())


# --- một lô ---


def _iso_utc(at: datetime) -> str:
    return at.astimezone(UTC).isoformat(timespec="seconds")


def init_marks(state: dict[str, Any], start: str, now: datetime | None = None) -> None:
    """Đặt head/tail/cursor cho state chưa có (state mới, hoặc state của bản một con trỏ trước 16/11/2026)."""
    if state.get("head"):
        return
    # cursor = cận dưới (không tính) của khoảng chưa đọc; state mới thì lùi 1 giây để bài đúng giờ Premiere vẫn được đọc.
    cursor = state.get("cursor") or _iso_utc(datetime.fromisoformat(start) - timedelta(seconds=1))
    head = max(cursor, start, _iso_utc((now or datetime.now(tz=UTC)) - NEW_WINDOW))
    state.update(cursor=cursor, head=head, tail=head, tailIds=[])


async def _next_batch(movie_id: str, start: str, state: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """(bài của lô, "new" | "gap"): bài mới từ head trước, rồi bài về muộn, rồi đọc lùi khoảng cursor..tail."""
    init_marks(state, start)
    head, tail, cursor = state["head"], state["tail"], state["cursor"]
    recent, tail_ids = state["recent"], set(state.get("tailIds") or [])
    fresh = await d1_exec(_SCAN.where(_P.c.movie_id == movie_id, _P.c.posted_at >= head).limit(NEW_SCAN_LIMIT))
    if fresh is None:
        raise RuntimeError("posts unavailable")
    pending = [post for post in fresh if post["id"] not in recent]
    if pending:
        return pending[:BATCH_SIZE], "new"
    if time.time() * 1000 - state.get("lateScanAt", 0) > LATE_SCAN_EVERY_MS:
        state["lateScanAt"] = int(time.time() * 1000)
        # Dưới tail là phần đọc lùi lo; từ tail tới head đã đọc - bài nào chưa có là bài về muộn.
        late_from = max(tail, start, _iso_utc(datetime.fromisoformat(head) - LATE_WINDOW))
        late = await d1_exec(
            _SCAN.where(_P.c.movie_id == movie_id, _P.c.posted_at >= late_from, _P.c.posted_at < head).limit(
                LATE_SCAN_LIMIT
            ),
            timeout=60.0,
        )
        pending = [post for post in late or [] if post["id"] not in recent and post["id"] not in tail_ids]
        if pending:
            return pending[:BATCH_SIZE], "new"
    if tail <= cursor:
        return [], "new"
    gap = await d1_exec(
        _SCAN_DESC.where(_P.c.movie_id == movie_id, _P.c.posted_at > cursor, _P.c.posted_at <= tail).limit(
            NEW_SCAN_LIMIT
        )
    )
    if gap is None:
        raise RuntimeError("posts unavailable")
    pending = [post for post in gap if post["id"] not in tail_ids and post["id"] not in recent]
    if not pending:
        state.update(tail=cursor, tailIds=[])  # đã đọc hết khoảng thiếu
    return pending[:BATCH_SIZE], "gap"


def mark_read(state: dict[str, Any], metas: list[dict[str, Any]], kind: str) -> None:
    """Dời mốc sau một lô: bài mới đẩy head lên; lô đọc lùi kéo tail xuống (nhớ id các bài đúng giờ tail để không đọc lại)."""
    if kind == "gap":
        low = min(meta["posted_at"] for meta in metas)
        same = [meta["id"] for meta in metas if meta["posted_at"] == low]
        state["tailIds"] = (state.get("tailIds") or []) + same if low == state["tail"] else same
        state["tail"] = low
    else:
        for meta in metas:
            state["recent"][meta["id"]] = meta["posted_at"]
            state["head"] = max(state["head"], meta["posted_at"])
    keep_after = _iso_utc(datetime.fromisoformat(state["head"]) - LATE_WINDOW - timedelta(hours=1))
    state["recent"] = {pid: at for pid, at in state["recent"].items() if not at or at >= keep_after}


def _words(text: str) -> set[str]:
    return {w for w in re.findall(r"\w+", text.lower()) if len(w) > 1}


def nearest_angle(angles: list[dict[str, Any]], aspect: str, name: str) -> dict[str, Any] | None:
    """Góc cùng khía cạnh gần tên nhất (trùng nhiều từ nhất, hoà thì góc đã có nhiều bài nhất)."""
    same = [a for a in angles if a["aspect"] == aspect]
    if not same:
        return None
    target = _words(name)
    return max(
        same, key=lambda a: (len(target & _words(a["name"])) / max(1, len(target | _words(a["name"]))), len(a["refs"]))
    )


def _num(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except TypeError, ValueError:
        return 0


def youtube_caption(content: str, transcript: str | None) -> str:
    """Video YouTube: tiêu đề + mô tả (cắt) + đoạn đầu transcript - phần người nói mới là nội dung review."""
    text = content[:YOUTUBE_TEXT_CHARS].strip()
    if transcript:
        text = f"{text}\n\nTranscript: {transcript[:TRANSCRIPT_CHARS].strip()}"
    return text


async def _transcripts(external_ids: list[str]) -> dict[str, str]:
    """{external_id: transcript} của video YouTube (post_transcripts). Lỗi / chưa có bảng thì rỗng - vẫn đọc tiêu đề."""
    return await load_transcripts("youtube", external_ids, TRANSCRIPT_CHARS)


async def _load_posts(movie_id: str, ids: list[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    p = posts_table.alias("p")
    # Danh sách id qua MỘT tham số JSON (giới hạn 100 tham số của D1), dạng `id IN (SELECT value FROM json_each(?))` để SQLite
    # tra theo khoá chính. 14/11/2026: trước đó là JOIN json_each ... WHERE movie_id = ? - planner duyệt mọi bài của phim cho
    # từng id, 114 triệu dòng đọc/ngày (truy vấn nặng nhất D1).
    rows = await d1_exec(
        select(
            p.c.id,
            p.c.external_id,
            p.c.platform,
            p.c.author,
            p.c.url,
            p.c.posted_at,
            # Cắt nội dung ngay trong SQL để không kéo cả caption dài về Python.
            literal_column(f"substr({p.name}.content, 1, {CAPTION_CHARS + 1})").label("content"),
            p.c.like_count,
            p.c.reply_count,
            p.c.repost_count,
            p.c.reshare_count,
            p.c.quote_count,
            p.c.view_count,
        )
        # "+" tắt index movie_id cho điều kiện này: SQLite tra từng id theo khoá chính rồi mới lọc phim (đo EXPLAIN 14/11/2026).
        .where(p.c.id.in_(json_values(ids)), literal_column(f"+{p.name}.movie_id") == movie_id)
    )
    if rows is None:
        raise RuntimeError("posts unavailable")
    transcripts = await _transcripts([row["external_id"] for row in rows if row["platform"] == "youtube"])
    result = []
    for row in rows:
        channel = channel_of_post(row)
        likes, comments = _num(row.get("like_count")), _num(row.get("reply_count"))
        shares = _num(row.get("repost_count")) + _num(row.get("reshare_count")) + _num(row.get("quote_count"))
        content = row.get("content") or ""
        if row["platform"] == "youtube":
            content = youtube_caption(content, transcripts.get(row["external_id"]))
        views = _num(row.get("view_count"))
        result.append(
            {
                "id": row["id"],
                "platform": row["platform"],
                "source": {
                    "key": f"{row['platform']}|{channel['handle']}" if channel else "",
                    "name": (row.get("author") or "").strip() or "Chưa rõ tác giả",
                    "url": channel_url(row["platform"], channel["kind"], channel["handle"])
                    if channel
                    else row.get("url") or "",
                },
                "url": row.get("url") or "",
                "postedAt": row.get("posted_at"),
                "caption": content[:CAPTION_CHARS],
                "captionTruncated": len(content) > CAPTION_CHARS,
                "imageUrl": "/api/social-topic/review-angles/image?"
                + urlencode({"movieId": movie_id, "postId": row["id"]}),
                "presentation": {"priority": 0, "label": None, "signals": []},
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "views": views if row["platform"] == "tiktok" or views > 0 else None,
                "interactions": likes + comments + shares,
            }
        )
    return sorted(result, key=lambda p: p.get("postedAt") or "")


async def analyze_next_batch(movie_id: str) -> dict[str, Any]:
    """Đọc tiếp MỘT lô bài Premiere của phim (khoá Redis chống chạy trùng): lấy lô kế tiếp theo thứ tự đầu/khe, hỏi AI
    gom vào góc nhìn đã có hoặc tạo góc mới (có cache theo nội dung lô), cập nhật `state.json` trên R2 (góc, bài đã
    đọc, nguồn). Trả `{read, analyzed, ai_called, tokens}` hoặc lý do bỏ qua (`not_available`, `busy`)."""
    movie = await load_movie(movie_id)
    if not _available(movie):
        return {"movie_id": movie_id, "read": 0, "reason": "not_available"}
    redis = get_redis_client()
    lock_key = f"{_PREFIX}lock:{movie_id}"
    if not await redis.set(lock_key, "1", nx=True, ex=600):
        return {"movie_id": movie_id, "read": 0, "reason": "busy"}
    try:
        prefix = angles_dir(movie_id)
        state = await read_json(f"{prefix}/state.json") or empty_state()
        metas, kind = await _next_batch(movie_id, premiere_start(movie["premiereDate"]), state)
        if not metas:
            await write_json(f"{prefix}/state.json", state)
            return {"movie_id": movie_id, "read": 0}
        posts = await _load_posts(movie_id, [m["id"] for m in metas])
        readable = [p for p in posts if has_text(p["caption"])]
        answer: dict[str, Any] = {"summary": "", "angles": []}
        ai_called = False
        tokens = 0
        if readable:
            # Gửi kèm mô tả (cắt ngắn) và số bài để model biết góc đã có nói về ý gì mà tái sử dụng, thay vì chỉ thấy tên.
            catalog = [
                {
                    "id": a["id"],
                    "name": a["name"],
                    "aspect": a["aspect"],
                    "description": (a.get("description") or "")[:120],
                    "posts": len(a["refs"]),
                }
                for a in state["angles"]
            ]
            cache_key = (
                f"{_PREFIX}batch:"
                + hashlib.sha256(
                    js_json(
                        {
                            "movieId": movie_id,
                            "catalog": catalog,
                            "posts": [{"id": p["id"], "caption": p["caption"]} for p in readable],
                        }
                    ).encode()
                ).hexdigest()
            )
            cached = await redis.get(cache_key)
            parsed = parse_batch(json.loads(cached), readable, state["angles"]) if cached else None
            if parsed:
                answer = parsed
            else:
                user = json.dumps(
                    {
                        "movieTitle": movie["title"],
                        "premiereDate": movie["premiereDate"],
                        "aspects": REVIEW_ASPECTS,
                        "existingAngles": catalog,
                        "posts": [
                            {
                                "id": p["id"],
                                "platform": p["platform"],
                                "source": p["source"]["name"],
                                "caption": p["caption"],
                                "coverage": "Caption chỉ có phần đầu đã lưu cho phân tích."
                                if p["captionTruncated"]
                                else "Caption đã lưu.",
                            }
                            for p in readable
                        ],
                    },
                    ensure_ascii=False,
                )
                raw, content = await call_llm_json(
                    system_prompt=REVIEW_ANGLES_SYSTEM,
                    user_prompt=user,
                    max_tokens=8000,
                    task="review_angles",
                    temperature=0.2,
                )
                ai_called = True
                # Bee không trả số token từng lượt - ước lượng ~3 ký tự/token (tiếng Việt).
                tokens = round((len(REVIEW_ANGLES_SYSTEM) + len(user) + len(content)) / 3)
                parsed = parse_batch(raw, readable, state["angles"])
                if parsed:
                    answer = parsed
                    await redis.set(cache_key, json.dumps(parsed, ensure_ascii=False), ex=30 * 86400)
                    state["model"] = "bee"
                else:
                    fail_key = metas[0]["id"]
                    count = (state.get("failures") or {}).get(fail_key, 0) + 1
                    state["failures"] = {fail_key: count}
                    if count < MAX_BATCH_FAILURES:
                        await write_json(f"{prefix}/state.json", state)
                        return {
                            "movie_id": movie_id,
                            "read": 0,
                            "reason": "ai_unparsed",
                            "ai_called": True,
                            "tokens": tokens,
                        }
                    logger.warning("review_angles_batch_skipped", movie_id=movie_id, first_post=fail_key)
        page = state["pages"]
        if readable:
            await write_json(f"{prefix}/page-{page}.json", readable)
            state["pages"] += 1
        by_id = {p["id"]: p for p in readable}
        for item in answer["angles"]:
            angle_id = item["id"] or angle_id_of(item["aspect"], item["name"])
            angle = next((a for a in state["angles"] if a["id"] == angle_id), None)
            if not angle and sum(1 for a in state["angles"] if a["aspect"] == item["aspect"]) >= MAX_ANGLES_PER_ASPECT:
                angle = nearest_angle(state["angles"], item["aspect"], item["name"])
            if not angle:
                angle = {
                    "id": angle_id,
                    "name": item["name"],
                    "description": item["description"],
                    "aspect": item["aspect"],
                    "refs": [],
                }
                state["angles"].append(angle)
            for ref in item["evidence"]:
                post = by_id[ref["postId"]]
                if not any(r["id"] == post["id"] for r in angle["refs"]):
                    angle["refs"].append(
                        {
                            "id": post["id"],
                            "page": page,
                            "platform": post["platform"],
                            "source": post["source"]["key"],
                            "interactions": post["interactions"],
                            "views": post["views"],
                            "postedAt": post["postedAt"],
                            "quote": ref["quote"],
                            "stance": ref.get("stance"),
                        }
                    )
        mark_read(state, metas, kind)
        for post in posts:
            state["analyzed"][post["platform"]] = state["analyzed"].get(post["platform"], 0) + 1
            if post["source"]["key"]:
                sources = state["sources"].setdefault(post["platform"], [])
                if post["source"]["key"] not in sources:
                    sources.append(post["source"]["key"])
        state["updatedAt"] = datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        state["failures"] = {}
        await write_json(f"{prefix}/state.json", state)
        return {
            "movie_id": movie_id,
            "read": len(metas),
            "analyzed": len(readable),
            "ai_called": ai_called,
            "tokens": tokens,
        }
    finally:
        await redis.delete(lock_key)


# --- lượt chạy ---


async def _add_tokens(movie_id: str, tokens: int) -> int:
    if not tokens:
        return int(await get_redis_client().get(f"{_PREFIX}tokens:{movie_id}") or 0)
    return int(await get_redis_client().incrby(f"{_PREFIX}tokens:{movie_id}", tokens))


async def _active_movies() -> list[str]:
    redis = get_redis_client()
    cached = await redis.get(f"{_PREFIX}active")
    if cached:
        return json.loads(cached)
    today = vn_day()
    since = vn_day(datetime.now(tz=UTC) - timedelta(days=ACTIVE_DAYS))
    opening = func.coalesce(_S.c.premiere_date, _M.c.released_at)
    rows = await d1_exec(
        select(_M.c.id)
        .select_from(_MOVIE_DATES)
        .where(_M.c.enabled == 1, func.substr(opening, 1, 10).between(since, today))
        .order_by(opening.desc())
    )
    ids = [row["id"] for row in rows or []]
    if rows is not None:
        await redis.set(f"{_PREFIX}active", json.dumps(ids), ex=1800)
    return ids


async def run_focus(focus: dict[str, Any]) -> dict[str, Any]:
    movie_id = focus["movieId"]
    budget = int(focus.get("tokenBudget") or 0)
    started = time.monotonic()
    batches = read = 0
    used = await _add_tokens(movie_id, 0)
    while time.monotonic() - started < FOCUS_TICK_SECONDS:
        if budget and used >= budget:
            await get_redis_client().delete(REVIEW_FOCUS_KEY)
            return {"focus": movie_id, "stopped": "token_budget", "used": used, "batches": batches, "read": read}
        result = await analyze_next_batch(movie_id)
        used = await _add_tokens(movie_id, result.get("tokens", 0))
        if result.get("reason") == "busy":
            break
        if not result["read"] and not result.get("ai_called"):
            if result.get("reason") in (None, "not_available"):
                await get_redis_client().delete(REVIEW_FOCUS_KEY)
                return {"focus": movie_id, "stopped": "done", "used": used, "batches": batches, "read": read}
            break
        batches += 1
        read += result["read"]
    return {"focus": movie_id, "used": used, "batches": batches, "read": read}


async def run_tick() -> dict[str, Any] | None:
    """Một lượt: phim ưu tiên thì đọc liên tục tới FOCUS_TICK_SECONDS; không thì một lô của phim kế tiếp (xoay vòng)."""
    redis = get_redis_client()
    raw_focus = await redis.get(REVIEW_FOCUS_KEY)
    if raw_focus:
        try:
            focus = json.loads(raw_focus)
        except ValueError:
            focus = None
        if isinstance(focus, dict) and focus.get("movieId"):
            return await run_focus(focus)
    # 16/11/2026: trước đây mỗi lượt (~200 giây) chỉ một lô - tối đa ~430 lô/ngày dù trần cao hơn, không theo kịp bài mới.
    # Giờ đọc liên tiếp các phim (xoay vòng) tới TICK_SECONDS hoặc hết trần ngày.
    started = time.monotonic()
    done: list[dict[str, Any]] = []
    while time.monotonic() - started < TICK_SECONDS:
        result = await _one_rotation_step(redis)
        if result is None:
            break
        done.append(result)
        if result.get("skipped"):
            break
    if not done:
        return None
    return {
        "batches": sum(1 for r in done if r.get("ai_called")),
        "read": sum(r.get("read", 0) for r in done),
        "movies": sorted({r["movie_id"] for r in done if r.get("movie_id")}),
        **({"skipped": done[-1]["skipped"]} if done[-1].get("skipped") else {}),
    }


async def _one_rotation_step(redis: Any) -> dict[str, Any] | None:
    """Một lô của phim kế tiếp có bài chưa đọc (thử tối đa 4 phim). None khi không phim nào có việc."""
    cap_key = f"{_PREFIX}batches:{vn_day()}"
    used = int(await redis.get(cap_key) or 0)
    if used >= settings.review_angles_daily_batches:
        return {"skipped": "daily_cap", "used": used}
    movie_ids = await _active_movies()
    if not movie_ids:
        return None
    pointer_key = f"{_PREFIX}pointer"
    start = int(await redis.get(pointer_key) or 0)
    for step in range(min(len(movie_ids), 4)):
        index = (start + step) % len(movie_ids)
        try:
            result = await analyze_next_batch(movie_ids[index])
        except Exception as exc:  # noqa: BLE001 - một phim lỗi không chặn phim khác
            logger.warning("review_angles_batch_failed", movie_id=movie_ids[index], error=exc)
            result = {"movie_id": movie_ids[index], "read": 0}
        await _add_tokens(movie_ids[index], result.get("tokens", 0))
        if result.get("ai_called"):
            await redis.incr(cap_key)
            await redis.expire(cap_key, 2 * 86400)
        if result["read"] or result.get("ai_called"):
            await redis.set(pointer_key, (index + 1) % len(movie_ids))
            return result
    await redis.set(pointer_key, (start + 4) % len(movie_ids))
    return None
