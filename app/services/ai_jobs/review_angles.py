"""Review Angles: đọc dần bài đăng từ Premiere và gom thành các góc nhìn về phim.

Mỗi lượt đọc một lô 24 bài của một phim; AI gom vào góc đã có hoặc tạo góc mới. Con trỏ theo `posted_at`, mỗi giờ
quét lại 48 giờ trước con trỏ để nhặt bài về muộn. Phim ưu tiên (Redis `REVIEW_FOCUS_KEY`) đọc liên tục; còn lại xoay
vòng các phim đã qua Premiere trong 60 ngày, tối đa `DAILY_BATCH_CAP` lô/ngày.

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

from app.ai.prompts.review_angles import REVIEW_ANGLES_SYSTEM, REVIEW_ASPECTS
from app.ai.runtime import call_llm_json
from app.ai.tasks.review_angles import angle_id_of, has_text, js_json, parse_batch
from app.clients import lake
from app.clients.d1 import d1_query
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.services.ai_jobs.channels import RELEVANT_POST_FILTER, channel_of_post, channel_url

logger = get_logger(__name__)

BATCH_SIZE = 24
CAPTION_CHARS = 4000
NEW_SCAN_LIMIT = 200
LATE_WINDOW = timedelta(hours=48)
LATE_SCAN_EVERY_MS = 3_600_000
LATE_SCAN_LIMIT = 5000
ACTIVE_DAYS = 60
DAILY_BATCH_CAP = 300
MAX_BATCH_FAILURES = 3
FOCUS_TICK_SECONDS = 4 * 60
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


def _root(movie_id: str) -> str:
    return f"analysis/review-angles/v2/{movie_id}"


async def _read_json(key: str) -> Any:
    try:
        return json.loads(await lake.get_object(key))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


async def _write_json(key: str, value: Any) -> None:
    await lake.put_object(key, json.dumps(value, ensure_ascii=False).encode(), "application/json")


def empty_state() -> dict[str, Any]:
    return {
        "version": 2,
        "cursor": None,
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


async def load_movie(movie_id: str) -> dict[str, Any] | None:
    cached = _movies.get(movie_id)
    if cached and time.monotonic() - cached[0] < 3600:
        return cached[1]
    rows = await d1_query(
        """SELECT m.title, m.released_at, s.premiere_date FROM movies m
           LEFT JOIN movie_measure_settings s ON s.movie_id = m.id WHERE m.id = ? LIMIT 1""",
        [movie_id],
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


async def _next_batch(movie_id: str, start: str, state: dict[str, Any]) -> list[dict[str, Any]]:
    cursor = state.get("cursor")
    since = cursor if cursor and cursor > start else start
    fresh = await d1_query(
        f"""SELECT p.id, p.platform, p.posted_at FROM posts p
            WHERE p.movie_id = ? AND p.posted_at >= ? AND p.platform IN ('facebook', 'tiktok', 'threads') AND {RELEVANT_POST_FILTER}
            ORDER BY p.posted_at, p.id LIMIT {NEW_SCAN_LIMIT}""",
        [movie_id, since],
    )
    if fresh is None:
        raise RuntimeError("posts unavailable")
    recent = state["recent"]
    pending = [post for post in fresh if post["id"] not in recent]
    if not pending and cursor and time.time() * 1000 - state.get("lateScanAt", 0) > LATE_SCAN_EVERY_MS:
        state["lateScanAt"] = int(time.time() * 1000)
        late_from = max(datetime.fromisoformat(start), datetime.fromisoformat(cursor) - LATE_WINDOW)
        late = await d1_query(
            f"""SELECT p.id, p.platform, p.posted_at FROM posts p
                WHERE p.movie_id = ? AND p.posted_at >= ? AND p.posted_at < ?
                  AND p.platform IN ('facebook', 'tiktok', 'threads') AND {RELEVANT_POST_FILTER}
                ORDER BY p.posted_at, p.id LIMIT {LATE_SCAN_LIMIT}""",
            [movie_id, _iso_utc(late_from), cursor],
            timeout=60.0,
        )
        pending = [post for post in late or [] if post["id"] not in recent]
    return pending[:BATCH_SIZE]


def _num(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except TypeError, ValueError:
        return 0


async def _load_posts(movie_id: str, ids: list[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    rows = await d1_query(
        f"""SELECT p.id, p.platform, p.author, p.url, p.posted_at, substr(p.content, 1, {CAPTION_CHARS + 1}) AS content,
              p.like_count, p.reply_count, p.repost_count, p.reshare_count, p.quote_count, p.view_count
            FROM posts p JOIN json_each(?) ids ON ids.value = p.id WHERE p.movie_id = ?""",
        [json.dumps(ids), movie_id],
    )
    if rows is None:
        raise RuntimeError("posts unavailable")
    posts = []
    for row in rows:
        channel = channel_of_post(row)
        likes, comments = _num(row.get("like_count")), _num(row.get("reply_count"))
        shares = _num(row.get("repost_count")) + _num(row.get("reshare_count")) + _num(row.get("quote_count"))
        content = row.get("content") or ""
        views = _num(row.get("view_count"))
        posts.append(
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
    return sorted(posts, key=lambda p: p.get("postedAt") or "")


async def analyze_next_batch(movie_id: str) -> dict[str, Any]:
    movie = await load_movie(movie_id)
    if not _available(movie):
        return {"movie_id": movie_id, "read": 0, "reason": "not_available"}
    redis = get_redis_client()
    lock_key = f"{_PREFIX}lock:{movie_id}"
    if not await redis.set(lock_key, "1", nx=True, ex=600):
        return {"movie_id": movie_id, "read": 0, "reason": "busy"}
    try:
        prefix = _root(movie_id)
        state = await _read_json(f"{prefix}/state.json") or empty_state()
        metas = await _next_batch(movie_id, premiere_start(movie["premiereDate"]), state)
        if not metas:
            await _write_json(f"{prefix}/state.json", state)
            return {"movie_id": movie_id, "read": 0}
        posts = await _load_posts(movie_id, [m["id"] for m in metas])
        readable = [p for p in posts if has_text(p["caption"])]
        answer: dict[str, Any] = {"summary": "", "angles": []}
        ai_called = False
        tokens = 0
        if readable:
            catalog = [{"id": a["id"], "name": a["name"], "aspect": a["aspect"]} for a in state["angles"]]
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
                        await _write_json(f"{prefix}/state.json", state)
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
            await _write_json(f"{prefix}/page-{page}.json", readable)
            state["pages"] += 1
        by_id = {p["id"]: p for p in readable}
        for item in answer["angles"]:
            angle_id = item["id"] or angle_id_of(item["aspect"], item["name"])
            angle = next((a for a in state["angles"] if a["id"] == angle_id), None)
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
                            "quote": ref["quote"],
                        }
                    )
        for meta in metas:
            state["recent"][meta["id"]] = meta["posted_at"]
            if not state.get("cursor") or meta["posted_at"] > state["cursor"]:
                state["cursor"] = meta["posted_at"]
        for post in posts:
            state["analyzed"][post["platform"]] = state["analyzed"].get(post["platform"], 0) + 1
            if post["source"]["key"]:
                sources = state["sources"].setdefault(post["platform"], [])
                if post["source"]["key"] not in sources:
                    sources.append(post["source"]["key"])
        keep_after = _iso_utc(datetime.fromisoformat(state["cursor"]) - LATE_WINDOW - timedelta(hours=1))
        state["recent"] = {pid: at for pid, at in state["recent"].items() if not at or at >= keep_after}
        state["updatedAt"] = datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        state["failures"] = {}
        await _write_json(f"{prefix}/state.json", state)
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
    rows = await d1_query(
        """SELECT m.id FROM movies m LEFT JOIN movie_measure_settings s ON s.movie_id = m.id
           WHERE m.enabled = 1 AND substr(COALESCE(s.premiere_date, m.released_at), 1, 10) BETWEEN ? AND ?
           ORDER BY COALESCE(s.premiere_date, m.released_at) DESC""",
        [since, today],
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
    cap_key = f"{_PREFIX}batches:{vn_day()}"
    used = int(await redis.get(cap_key) or 0)
    if used >= DAILY_BATCH_CAP:
        return {"skipped": "daily_cap", "used": used}
    movies = await _active_movies()
    if not movies:
        return None
    pointer_key = f"{_PREFIX}pointer"
    start = int(await redis.get(pointer_key) or 0)
    for step in range(min(len(movies), 4)):
        index = (start + step) % len(movies)
        try:
            result = await analyze_next_batch(movies[index])
        except Exception as exc:  # noqa: BLE001 - một phim lỗi không chặn phim khác
            logger.warning("review_angles_batch_failed", movie_id=movies[index], error=str(exc)[:300])
            result = {"movie_id": movies[index], "read": 0}
        await _add_tokens(movies[index], result.get("tokens", 0))
        if result.get("ai_called"):
            await redis.incr(cap_key)
            await redis.expire(cap_key, 2 * 86400)
        if result["read"] or result.get("ai_called"):
            await redis.set(pointer_key, (index + 1) % len(movies))
            return result
    await redis.set(pointer_key, (start + 4) % len(movies))
    return None
