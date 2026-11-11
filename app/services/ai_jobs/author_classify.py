"""Tự gắn tầng cho tác giả chưa phân loại bằng AI.

Mỗi lượt nhận một phim đến hạn (`author_ai_state.next_at`), lấy các tác giả chưa có nhãn ảnh hưởng nhất 60 ngày qua,
hỏi AI và lưu nhãn đủ tự tin vào `author_classifications` (không ghi đè nhãn có sẵn). Mọi gợi ý ghi vào
`author_ai_review` để không hỏi lại. Bảng nhãn ở cinemark-db (`database="app"`), bài ở cinemark-scraper.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Column, Integer, Table, Text, func, select, text
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateTable

from app.ai.prompts.author_tiers import AUTHOR_TIERS_SYSTEM
from app.ai.runtime import call_llm_json
from app.core.logging import get_logger
from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec, d1_insert_many, metadata
from app.repositories.d1.tables import movies, posts
from app.services.ai_jobs.channels import extract_handle

logger = get_logger(__name__)

SUGGEST_BATCH = 15
AUTO_APPLY_CONFIDENCE = 75
LOOKBACK_DAYS = 60
BATCHES_PER_RUN = 2
RECHECK_AFTER = timedelta(hours=6)
CONTINUE_AFTER = timedelta(minutes=10)
REASK_AFTER = timedelta(days=14)
# Bảng nhãn ~56 nghìn dòng: cache 10 phút thay vì đọc lại mỗi lượt.
_CACHE_SECONDS = 600
TIERS = ("owned", "pro", "media", "outsider")


_tiers_cache: tuple[float, set[str]] | None = None
_tables_ready = False

# Trên database "app" (cinemark-db).
classifications = Table(
    "author_classifications", metadata, *(Column(name, Text) for name in ("id", "platform", "handle", "tier"))
)
review = Table(
    "author_ai_review",
    metadata,
    Column("handle", Text, primary_key=True),
    Column("platform", Text),
    Column("movie_id", Text),
    Column("tier", Text),
    Column("confidence", Integer),
    Column("reason", Text),
    Column("model", Text),
    Column("applied", Integer, nullable=False, server_default=text("0")),
    Column("reviewed_at", Text, nullable=False),
)
state = Table(
    "author_ai_state",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("next_at", Text, nullable=False),
    Column("error", Text),
)
_P = posts.alias("p")


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


async def _ensure_tables() -> None:
    global _tables_ready
    if _tables_ready:
        return
    for table in (review, state):
        await d1_exec(CreateTable(table, if_not_exists=True), database="app")
    _tables_ready = True


async def _classified_handles() -> set[str]:
    global _tiers_cache
    if _tiers_cache and time.monotonic() - _tiers_cache[0] < _CACHE_SECONDS:
        return _tiers_cache[1]
    rows = await d1_exec(select(classifications.c.handle), database="app", timeout=60.0)
    if rows is None:
        raise RuntimeError("author_classifications unavailable")
    handles = {row["handle"] for row in rows}
    _tiers_cache = (time.monotonic(), handles)
    return handles


async def _unclassified_authors(movie_id: str) -> list[dict[str, Any]]:
    since = _iso(_now() - timedelta(days=LOOKBACK_DAYS))
    interactions = sum(
        func.coalesce(_P.c[name], 0)
        for name in ("like_count", "reply_count", "repost_count", "reshare_count", "quote_count")
    )
    rows = await d1_exec(
        select(_P.c.platform, _P.c.author, _P.c.url, interactions.label("interactions")).where(
            _P.c.movie_id == movie_id, _P.c.posted_at >= since, relevant_post(_P)
        ),
        timeout=60.0,
    )
    if rows is None:
        raise RuntimeError("posts unavailable")
    tiers = await _classified_handles()
    by_handle: dict[str, dict[str, Any]] = {}
    for post in rows:
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
    query = (
        select(_P.c.platform, _P.c.author, _P.c.url, func.substr(_P.c.content, 1, 400).label("content"))
        .where(_P.c.movie_id == movie_id, relevant_post(_P))
        .order_by(_P.c.posted_at.desc())
        .limit(3000)
    )
    if platforms:
        query = query.where(_P.c.platform.in_(platforms))
    rows = await d1_exec(query, timeout=60.0) or []
    wanted_keys = {a["handle"] for a in wanted}
    samples: dict[str, dict[str, Any]] = {}
    for post in rows:
        handle = extract_handle(post["platform"], post.get("author"), post.get("url"))
        if handle not in wanted_keys:
            continue
        entry = samples.setdefault(handle, {"name": post.get("author") or handle, "captions": []})
        if len(entry["captions"]) < 5 and post.get("content"):
            entry["captions"].append(" ".join(str(post["content"]).split()))
    movie = (await d1_exec(select(movies.c.title, movies.c.distributor).where(movies.c.id == movie_id)) or [{}])[0]
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
    review_rows = await d1_exec(
        select(review.c.handle, review.c.applied, review.c.reviewed_at), database="app", timeout=60.0
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
        # Nhãn tay luôn thắng: handle đã có nhãn thì bỏ qua (UNIQUE handle).
        await d1_insert_many(
            classifications,
            [
                {
                    "id": uuid.uuid4().hex,
                    "platform": a["platform"],
                    "handle": a["handle"],
                    "tier": suggestions[a["handle"]]["tier"],
                }
                for a in confident
            ],
            conflict=["handle"],
            update=(),
            database="app",
        )
        reviews = []
        for author in wanted:
            item = suggestions.get(author["handle"]) or {}
            reviews.append(
                {
                    "handle": author["handle"],
                    "platform": author["platform"],
                    "movie_id": movie_id,
                    "tier": item.get("tier"),
                    "confidence": item.get("confidence", 0),
                    "reason": item.get("reason"),
                    "model": "bee",
                    "applied": 1 if item.get("confidence", 0) >= AUTO_APPLY_CONFIDENCE else 0,
                    "reviewed_at": now,
                }
            )
        await d1_insert_many(review, reviews, conflict=["handle"], database="app")
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
    enabled = await d1_exec(select(movies.c.id).where(movies.c.enabled == 1))
    state_rows = await d1_exec(select(state.c.movie_id, state.c.next_at), database="app")
    if not enabled or state_rows is None:
        return None
    next_at = {row["movie_id"]: row["next_at"] for row in state_rows}
    now = _now()

    def due_at(movie_id: str) -> str:
        return next_at.get(movie_id) or ""

    due = sorted(
        (m["id"] for m in enabled if not due_at(m["id"]) or datetime.fromisoformat(due_at(m["id"])) <= now),
        key=due_at,
    )
    if not due:
        return None
    movie_id = due[0]
    # Nhận phim: chỉ một tiến trình đổi được next_at từ giá trị vừa đọc.
    claim = insert(state).values(movie_id=movie_id, next_at=_iso(now + timedelta(hours=1)))
    claimed = await d1_exec(
        claim.on_conflict_do_update(
            index_elements=["movie_id"],
            set_={"next_at": claim.excluded.next_at},
            where=state.c.next_at == due_at(movie_id),
        ).returning(state.c.movie_id),
        database="app",
    )
    if not claimed:
        return None

    async def set_next(delta: timedelta, error: str | None = None) -> None:
        await d1_insert_many(
            state,
            [{"movie_id": movie_id, "next_at": _iso(_now() + delta), "error": error}],
            conflict=["movie_id"],
            database="app",
        )

    try:
        result = await classify_movie_authors(movie_id)
    except Exception as exc:
        await set_next(timedelta(hours=1), str(exc)[:300])
        raise
    await set_next(CONTINUE_AFTER if result["remaining"] else RECHECK_AFTER)
    return result
