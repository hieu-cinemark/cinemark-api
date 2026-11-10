"""Tự gắn nhãn tác giả chưa phân loại bằng AI - bản Python của processAuthorAutoClassifyQueue (cinemark
api/services/authors/autoClassify.ts, chuyển sang 10/11/2026).

Mỗi lượt nhận một phim đến hạn (author_ai_state.next_at cũ nhất, nhận bằng UPDATE có điều kiện), lấy các tác giả chưa có
nhãn ảnh hưởng nhất 60 ngày qua mà AI chưa xem, nhờ AI gợi ý tầng và LƯU nhãn đủ tự tin vào author_classifications (không
bao giờ ghi đè nhãn đã có - nhãn tay luôn thắng). Mọi gợi ý nằm trong author_ai_review để không hỏi lại. Bảng nằm ở
cinemark-db (database="app"); bài đọc từ cinemark-scraper.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.ai.prompts.author_tiers import AUTHOR_TIERS_SYSTEM
from app.ai.runtime import call_llm_json
from app.clients.d1 import d1_query
from app.core.logging import get_logger
from app.services.ai_jobs.channels import RELEVANT_POST_FILTER, extract_handle

logger = get_logger(__name__)

SUGGEST_BATCH = 15
AUTO_APPLY_CONFIDENCE = 75
LOOKBACK_DAYS = 60
BATCHES_PER_RUN = 2
RECHECK_AFTER = timedelta(hours=6)
CONTINUE_AFTER = timedelta(minutes=10)
REASK_AFTER = timedelta(days=14)
# Bảng nhãn ~56 nghìn dòng: đọc lại mỗi lượt là ~40 triệu dòng/ngày trên cinemark-db - giữ trong bộ nhớ 10 phút.
_CACHE_SECONDS = 600
TIERS = ("owned", "pro", "media", "outsider")


_tiers_cache: tuple[float, set[str]] | None = None
_tables_ready = False


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


async def _ensure_tables() -> None:
    global _tables_ready
    if _tables_ready:
        return
    await d1_query(
        """CREATE TABLE IF NOT EXISTS author_ai_review (handle TEXT PRIMARY KEY, platform TEXT, movie_id TEXT, tier TEXT,
           confidence INTEGER, reason TEXT, model TEXT, applied INTEGER NOT NULL DEFAULT 0, reviewed_at TEXT NOT NULL)""",
        database="app",
    )
    await d1_query(
        "CREATE TABLE IF NOT EXISTS author_ai_state (movie_id TEXT PRIMARY KEY, next_at TEXT NOT NULL, error TEXT)",
        database="app",
    )
    _tables_ready = True


async def _classified_handles() -> set[str]:
    global _tiers_cache
    if _tiers_cache and time.monotonic() - _tiers_cache[0] < _CACHE_SECONDS:
        return _tiers_cache[1]
    rows = await d1_query("SELECT handle FROM author_classifications", database="app", timeout=60.0)
    if rows is None:
        raise RuntimeError("author_classifications unavailable")
    handles = {row["handle"] for row in rows}
    _tiers_cache = (time.monotonic(), handles)
    return handles


async def _unclassified_authors(movie_id: str) -> list[dict[str, Any]]:
    since = _iso(_now() - timedelta(days=LOOKBACK_DAYS))
    posts = await d1_query(
        f"""SELECT p.platform, p.author, p.url,
              COALESCE(p.like_count, 0) + COALESCE(p.reply_count, 0) + COALESCE(p.repost_count, 0)
              + COALESCE(p.reshare_count, 0) + COALESCE(p.quote_count, 0) AS interactions
            FROM posts p WHERE p.movie_id = ? AND p.posted_at >= ? AND {RELEVANT_POST_FILTER}""",
        [movie_id, since],
        timeout=60.0,
    )
    if posts is None:
        raise RuntimeError("posts unavailable")
    tiers = await _classified_handles()
    by_handle: dict[str, dict[str, Any]] = {}
    for post in posts:
        handle = extract_handle(post["platform"], post.get("author"), post.get("url"))
        if not handle or handle in tiers:
            continue
        entry = by_handle.setdefault(
            handle, {"platform": post["platform"], "handle": handle, "posts": 0, "interactions": 0}
        )
        entry["posts"] += 1
        entry["interactions"] += int(post.get("interactions") or 0)
    return sorted(by_handle.values(), key=lambda a: (-a["interactions"], -a["posts"]))


async def suggest_author_tiers(movie_id: str, wanted: list[dict[str, Any]]) -> list[dict[str, Any]]:
    platforms = sorted({a["platform"] for a in wanted if a.get("platform")})
    placeholders = ",".join("?" for _ in platforms)
    platform_sql = f"AND p.platform IN ({placeholders})" if platforms else ""
    posts = (
        await d1_query(
            f"""SELECT p.platform, p.author, p.url, substr(p.content, 1, 400) AS content FROM posts p
                WHERE p.movie_id = ? AND {RELEVANT_POST_FILTER} {platform_sql}
                ORDER BY p.posted_at DESC LIMIT 3000""",
            [movie_id, *platforms],
            timeout=60.0,
        )
        or []
    )
    wanted_keys = {a["handle"] for a in wanted}
    samples: dict[str, dict[str, Any]] = {}
    for post in posts:
        handle = extract_handle(post["platform"], post.get("author"), post.get("url"))
        if handle not in wanted_keys:
            continue
        entry = samples.setdefault(handle, {"name": post.get("author") or handle, "captions": []})
        if len(entry["captions"]) < 5 and post.get("content"):
            entry["captions"].append(" ".join(str(post["content"]).split()))
    movie = (await d1_query("SELECT title, distributor FROM movies WHERE id = ?", [movie_id]) or [{}])[0]
    payload = {
        "movie": {"title": movie.get("title") or "", "distributor": movie.get("distributor")},
        "authors": [
            {
                "handle": a["handle"],
                "platform": a["platform"],
                "display_name": samples.get(a["handle"], {}).get("name", a["handle"]),
                "captions": samples.get(a["handle"], {}).get("captions", []),
            }
            for a in wanted
        ],
    }
    parsed, _ = await call_llm_json(
        system_prompt=AUTHOR_TIERS_SYSTEM,
        user_prompt=json.dumps(payload, ensure_ascii=False),
        max_tokens=4000,
        task="author_tiers",
    )
    items = parsed.get("items") if isinstance(parsed, dict) else None
    out = []
    for item in items or []:
        if not isinstance(item, dict) or item.get("handle") not in wanted_keys or item.get("tier") not in TIERS:
            continue
        try:
            confidence = max(0, min(100, round(float(item.get("confidence") or 0))))
        except TypeError, ValueError:
            confidence = 0
        out.append(
            {
                "handle": item["handle"],
                "tier": item["tier"],
                "confidence": confidence,
                "reason": str(item.get("reason") or "")[:200],
            }
        )
    return out


async def classify_movie_authors(movie_id: str) -> dict[str, Any]:
    await _ensure_tables()
    review_rows = await d1_query(
        "SELECT handle, applied, reviewed_at FROM author_ai_review", database="app", timeout=60.0
    )
    if review_rows is None:
        raise RuntimeError("author_ai_review unavailable")
    cutoff = _now() - REASK_AFTER
    reviewed = set()
    for row in review_rows:
        try:
            at = datetime.fromisoformat(str(row["reviewed_at"]))
        except ValueError:
            at = cutoff
        if row.get("applied") or at > cutoff:
            reviewed.add(row["handle"])
    pending = [a for a in await _unclassified_authors(movie_id) if a["handle"] not in reviewed]
    reviewed_count = applied = 0
    applied_handles: list[str] = []
    for batch in range(BATCHES_PER_RUN):
        wanted = pending[batch * SUGGEST_BATCH : (batch + 1) * SUGGEST_BATCH]
        if not wanted:
            break
        suggestions = {item["handle"]: item for item in await suggest_author_tiers(movie_id, wanted)}
        now = _iso(_now())
        confident = [
            a for a in wanted if suggestions.get(a["handle"], {}).get("confidence", 0) >= AUTO_APPLY_CONFIDENCE
        ]
        if confident:
            # Nhãn tay luôn thắng: handle đã có nhãn thì bỏ qua (UNIQUE handle).
            values = ",".join("(?, ?, ?, ?)" for _ in confident)
            params: list[Any] = []
            for author in confident:
                params += [
                    uuid.uuid4().hex,
                    author["platform"],
                    author["handle"],
                    suggestions[author["handle"]]["tier"],
                ]
            await d1_query(
                f"INSERT INTO author_classifications (id, platform, handle, tier) VALUES {values} ON CONFLICT(handle) DO NOTHING",
                params,
                database="app",
            )
        values = ",".join("(?, ?, ?, ?, ?, ?, ?, ?, ?)" for _ in wanted)
        params = []
        for author in wanted:
            item = suggestions.get(author["handle"]) or {}
            params += [
                author["handle"],
                author["platform"],
                movie_id,
                item.get("tier"),
                item.get("confidence", 0),
                item.get("reason"),
                "bee",
                1 if item.get("confidence", 0) >= AUTO_APPLY_CONFIDENCE else 0,
                now,
            ]
        await d1_query(
            f"""INSERT INTO author_ai_review (handle, platform, movie_id, tier, confidence, reason, model, applied, reviewed_at)
                VALUES {values}
                ON CONFLICT(handle) DO UPDATE SET platform = excluded.platform, movie_id = excluded.movie_id, tier = excluded.tier,
                  confidence = excluded.confidence, reason = excluded.reason, model = excluded.model, applied = excluded.applied,
                  reviewed_at = excluded.reviewed_at""",
            params,
            database="app",
        )
        reviewed_count += len(wanted)
        applied += len(confident)
        applied_handles += [a["handle"] for a in confident]
    if applied_handles and _tiers_cache:
        _tiers_cache[1].update(applied_handles)
    return {
        "movie_id": movie_id,
        "reviewed": reviewed_count,
        "applied": applied,
        "remaining": len(pending) > reviewed_count,
    }


async def process_author_queue() -> dict[str, Any] | None:
    """Một lượt: nhận phim đến hạn lâu nhất rồi phân loại."""
    await _ensure_tables()
    movies = await d1_query("SELECT id FROM movies WHERE enabled = 1")
    state = await d1_query("SELECT movie_id, next_at FROM author_ai_state", database="app")
    if not movies or state is None:
        return None
    next_at = {row["movie_id"]: row["next_at"] for row in state}
    now = _now()

    def due_at(movie_id: str) -> str:
        return next_at.get(movie_id) or ""

    due = sorted(
        (m["id"] for m in movies if not due_at(m["id"]) or datetime.fromisoformat(due_at(m["id"])) <= now),
        key=due_at,
    )
    if not due:
        return None
    movie_id = due[0]
    claimed = await d1_query(
        """INSERT INTO author_ai_state (movie_id, next_at) VALUES (?, ?)
           ON CONFLICT(movie_id) DO UPDATE SET next_at = excluded.next_at WHERE author_ai_state.next_at = ?
           RETURNING movie_id""",
        [movie_id, _iso(now + timedelta(hours=1)), due_at(movie_id)],
        database="app",
    )
    if not claimed:
        return None

    async def set_next(delta: timedelta, error: str | None = None) -> None:
        await d1_query(
            """INSERT INTO author_ai_state (movie_id, next_at, error) VALUES (?, ?, ?)
               ON CONFLICT(movie_id) DO UPDATE SET next_at = excluded.next_at, error = excluded.error""",
            [movie_id, _iso(_now() + delta), error],
            database="app",
        )

    try:
        result = await classify_movie_authors(movie_id)
    except Exception as exc:
        await set_next(timedelta(hours=1), str(exc)[:300])
        raise
    await set_next(CONTINUE_AFTER if result["remaining"] else RECHECK_AFTER)
    return result
