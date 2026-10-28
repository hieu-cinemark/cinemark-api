"""AI thêm câu truy vấn tìm kiếm Facebook/Threads cho từng phim (từ 2026-10-10), chạy ngay sau hashtag_discovery mỗi
sáng. Mỗi phim trước đó chỉ có 1-2 câu truy vấn, mà ô tìm kiếm của Facebook/Threads chỉ trả một phần nhỏ bài cho mỗi
câu - mỗi câu khác nhau trả về một tập bài khác. Kompa (đo 2026-10-10: ~120 lần số bài của mình cho Trại Buôn Người)
đếm cả bài nhắc tên phim kèm từ ngữ cảnh phim (review, đi xem...) hoặc tên diễn viên.

1. Ứng viên: "phim <tên>", "review <tên>", "đi xem <tên>", "<diễn viên chính> <tên>" (QUERY_TEMPLATES, MAX_CAST).
2. Chỉ phim còn quanh thời gian chiếu (ACTIVE_BEFORE/AFTER_DAYS quanh released_at) - mỗi câu Facebook là một lượt quét
   60 ngày bằng tài khoản thật, nên không thêm cho phim đã hết chiếu.
3. Kira đọc thông tin phim, chấm câu truy vấn: "film" khi kết quả tìm kiếm hầu hết sẽ là bài về đúng phim này.
4. Ràng buộc: "film" + confidence >= MIN_CONFIDENCE, tối đa MAX_ADDS_PER_MOVIE câu mỗi phim và MAX_ADDS_PER_RUN câu
   mỗi lượt; mỗi câu được thêm cho Facebook và Threads (nền tảng phim đang theo dõi). Không xếp crawl ngay - lịch crawl
   hằng ngày lấy chúng ở lượt kế tiếp (tránh dồn thêm hàng chục lượt quét Facebook cùng lúc).
5. Tự dọn: câu đã thêm PRUNE_AFTER_DAYS ngày mà đem về ít hơn MIN_NEW_POSTS bài MỚI (bài chưa có từ câu khác - posts
   chỉ lưu keyword_id của câu tìm ra đầu tiên) thì tắt. Không dọn câu của phim chưa chiếu / mới chiếu chưa đủ
   PRUNE_AFTER_DAYS ngày, và câu đã bị dọn của phim chưa chiếu được bật lại (_revive_upcoming, từ 20/10/2026).

Quyết định nằm chung bảng hashtag_ai_decisions với tag "<nền tảng>:<câu không dấu>" (source "query")."""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from app.ai.movie_context import cast_list, movie_context_block
from app.ai.runtime import call_llm, parse_json_response
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.repositories.d1.store import d1_query
from app.services.crawl.keyword_yield import keyword_yields
from app.services.discovery.hashtag_discovery import RECHECK_REJECTED_DAYS, _ensure_table, _record
from app.services.relevance.relevance_rules import normalize_title

logger = get_logger(__name__)

TASK = "query_discovery"
PLATFORMS = ("facebook", "threads")
# Nền tảng được THÊM câu mới. Threads chỉ nhận câu THEO MẪU (QUERY_TEMPLATES): câu AI tự sinh cho Threads gần như không ra
# bài (19/10/2026, log: "cừu Shaun Halloween", "Shaun quái thú", "Steve Cox Shaun"... 0-1 bài, 9-34 bài lạc đề mỗi câu) nên
# bị tắt hẳn ngày 19/10; bật lại cho câu theo mẫu từ 20/10. QUERY_DISCOVERY_THREADS=0 tắt Threads.
ADD_PLATFORMS = (
    ("facebook",)
    if os.getenv("QUERY_DISCOVERY_THREADS", "").lower() in ("0", "false", "no")
    else ("facebook", "threads")
)
TEMPLATE_ONLY_PLATFORMS = ("threads",)
# "trailer" / "suất chiếu sớm": giai đoạn hóng trước ngày chiếu, khi chưa ai "review" / "đi xem" (20/10/2026).
QUERY_TEMPLATES = ("phim {title}", "review {title}", "đi xem {title}", "{title} trailer", "{title} suất chiếu sớm")
MAX_CAST = 3
MIN_CONFIDENCE = 80
# Nâng 2026-10-16 (2 -> 4, 12 -> 30): câu kém đã tự bị tắt theo tỉ lệ liên quan (keyword_yield), nên thêm nhiều câu hơn
# để lấy nhiều bài nhất có thể.
MAX_ADDS_PER_MOVIE = 4
MAX_ADDS_PER_RUN = 30
# 45 (trước 20/10/2026: 30) - phim sắp chiếu cần câu tìm từ sớm để bắt giai đoạn hóng.
ACTIVE_BEFORE_DAYS = 45
ACTIVE_AFTER_DAYS = 45
# Tăng nhẹ Threads (24/10/2026): quyết định duyệt câu lưu theo (phim, câu) không theo nền tảng, nên câu theo mẫu đã duyệt
# cho Facebook trước khi Threads được nhận câu lại (20/10) không bao giờ tới Threads. Mỗi lượt thêm bù tối đa chừng này câu
# (~20% số từ khoá Threads đang bật), ưu tiên phim gần ngày chiếu; câu kém vẫn bị _prune tắt như thường.
THREADS_BACKFILL_PER_RUN = int(os.getenv("QUERY_THREADS_BACKFILL_PER_RUN", "14"))
THREADS_BACKFILL_PER_MOVIE = 2
PRUNE_AFTER_DAYS = 3
# Câu chạy được chừng này ngày (đã qua vài lượt crawl) mà chưa lưu bài nào và spider chưa thấy bài nào khớp -> tắt luôn.
EARLY_PRUNE_DAYS = 1
MIN_NEW_POSTS = 3

SYSTEM_PROMPT = """Bạn duyệt câu truy vấn TÌM KIẾM trên Facebook/Threads cho hệ thống theo dõi thảo luận về MỘT bộ phim chiếu rạp (TARGET FILM).
Hệ thống sẽ tự động tìm bằng mọi câu bạn chấp nhận và quét kết quả bằng tài khoản thật, nên câu kém sẽ tốn tài nguyên và kéo về bài rác.

Trả "film" khi phần lớn kết quả tìm kiếm của câu đó sẽ là bài nói về ĐÚNG phim này:
- tên phim kèm từ ngữ cảnh phim (phim, review, đi xem, rạp...) với tên phim đủ đặc trưng;
- tên diễn viên/đạo diễn THẬT SỰ có trong phim (đối chiếu Cast/Director) kèm tên phim.
Trả "ambiguous" khi tên phim trùng cụm từ thông dụng, tên bài hát, tiểu thuyết, game, phim/series khác (vd. "review Loạn Thế", "phim Người Được Chọn" có thể ra phim khác) và từ ngữ cảnh không đủ tách.
Trả "wrong" khi câu có người không thuộc phim, hoặc ghép sai.
Không chắc thì KHÔNG trả "film".

Trả về JSON hợp lệ, không bọc code block:
{"results":[{"query":"<đúng như được đưa>","verdict":"film|ambiguous|wrong","confidence":0-100,"reason":"một câu ngắn tiếng Việt"}]}"""


GENERATE_TASK = "query_generation"
MAX_GENERATED = 6
# Mỗi lần sinh câu mất ~150-170 giây và 11-15 nghìn token của Kira (đo 15/10/2026) - giới hạn số phim mỗi lượt, phim gần
# ngày chiếu nhất trước; phim còn lại chỉ xét câu theo mẫu và tới lượt ở các ngày sau.
MAX_GENERATE_PER_RUN = 6
SAMPLE_POSTS = 15
SAMPLE_DAYS = 14
# Câu AI đã thêm mà tỉ lệ bài liên quan dưới mức này (đủ mẫu) thì tắt - xem app/services/crawl/keyword_yield.py.
PRUNE_BELOW_RATIO = 0.20
PRUNE_MIN_SAMPLE = 50

GENERATE_PROMPT = """Bạn đề xuất câu truy vấn TÌM KIẾM trên Facebook và Threads (tiếng Việt) để tìm bài khán giả nói về MỘT bộ phim chiếu rạp.
Bạn có thông tin phim và một số bài liên quan nhiều tương tác gần đây. Hãy đề xuất tối đa 6 câu ngắn (2-6 từ) mà kết quả
tìm kiếm phần lớn sẽ là bài về ĐÚNG phim này, nhưng KHÁC các câu đang có - để bắt thêm bài chưa tìm được:
- tên nhân vật, biệt danh, câu thoại/meme khán giả hay nhắc (lấy từ các bài mẫu), tên phim viết tắt khán giả hay dùng;
- tên đạo diễn/diễn viên chính KÈM tên phim hoặc tên nhân vật;
- tên phim kèm từ ngữ cảnh rạp chiếu (suất chiếu, ra rạp, lịch chiếu, trailer...).
KHÔNG dùng hashtag (#), không dùng cụm quá chung (vd. "phim hay", "phim kinh dị"), không dùng tên người không có trong phim.
Tên phim trùng cụm từ thông dụng thì luôn ghép thêm ngữ cảnh phim hoặc tên diễn viên.

Trả về JSON hợp lệ, không bọc code block: {"queries": ["...", "..."]}"""


async def _sample_posts(movie_id: str) -> list[str]:
    """Bài liên quan nhiều tương tác nhất của phim gần đây (mọi nền tảng) - gợi ý cho AI các cách khán giả gọi phim."""
    since = (datetime.now(tz=UTC) - timedelta(days=SAMPLE_DAYS)).isoformat()
    rows = (
        await d1_query(
            f"""SELECT substr(p.content, 1, 240) AS content FROM posts p
            WHERE p.movie_id = ? AND p.posted_at >= ? AND {RELEVANT_POST_SQL} AND length(p.content) >= 30
            ORDER BY COALESCE(p.like_count, 0) + 3 * COALESCE(p.reply_count, 0) + COALESCE(p.repost_count, 0) DESC
            LIMIT ?""",
            [movie_id, since, SAMPLE_POSTS],
            quiet=True,
            timeout=60.0,
        )
        or []
    )
    return [" ".join(str(row["content"]).split()) for row in rows if row.get("content")]


async def _generate_queries(movie: dict[str, Any], existing: list[str]) -> list[str]:
    """Kira đề xuất câu tìm kiếm mới (dạng chữ) từ thông tin phim + bài mẫu. Lỗi -> [] (chỉ còn câu theo mẫu)."""
    samples = await _sample_posts(movie["id"])
    user = (
        f"{movie_context_block(movie)}\n"
        f"Câu đang có: {', '.join(existing[:20]) or '(chưa có)'}\n\n"
        "Bài mẫu:\n" + "\n".join(f"- {text}" for text in samples)
    )
    raw = await call_llm(task=GENERATE_TASK, system_prompt=GENERATE_PROMPT, user_prompt=user, max_tokens=4000)
    parsed = parse_json_response(raw)
    queries = parsed.get("queries") if isinstance(parsed, dict) else None
    out = []
    for query in queries if isinstance(queries, list) else []:
        text = " ".join(str(query).split()).strip(" .,-")
        if text and not text.startswith("#") and 2 <= len(text.split()) <= 8 and len(text) <= 80:
            out.append(text)
    return out[:MAX_GENERATED]


def _key(value: str) -> str:
    return normalize_title(value).replace(" ", "")


def _search_title(title: str) -> str:
    """Tên phim để gõ vào ô tìm kiếm: bỏ phần trong ngoặc ("QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)")."""
    return re.sub(r"\s*\([^)]*\)", "", title).strip()


def candidate_queries(movie: dict[str, Any]) -> list[str]:
    title = _search_title(str(movie.get("title") or ""))
    if not title:
        return []
    queries = [template.format(title=title) for template in QUERY_TEMPLATES]
    queries += [f"{actor} {title}" for actor in cast_list(movie.get("cast"))[:MAX_CAST]]
    return list(dict.fromkeys(queries))


def _active(movie: dict[str, Any], now: datetime) -> bool:
    released = str(movie.get("released_at") or "")[:10]
    try:
        day = datetime.fromisoformat(released).replace(tzinfo=UTC)
    except ValueError:
        return True  # chưa có ngày chiếu: vẫn xét
    return now - timedelta(days=ACTIVE_AFTER_DAYS) <= day <= now + timedelta(days=ACTIVE_BEFORE_DAYS)


async def _ask_kira(
    movie: dict[str, Any], queries: list[str], other_titles: Iterable[str]
) -> dict[str, dict[str, Any]]:
    others = [t for t in other_titles if t != movie.get("title")][:40]
    user = (
        f"{movie_context_block(movie)}\n"
        f"Other films being tracked: {', '.join(others)}\n\n"
        "Candidate search queries:\n" + "\n".join(f"- {q}" for q in queries)
    )
    raw = await call_llm(task=TASK, system_prompt=SYSTEM_PROMPT, user_prompt=user, max_tokens=6000)
    parsed = parse_json_response(raw)
    results = parsed.get("results") if isinstance(parsed, dict) else None
    out: dict[str, dict[str, Any]] = {}
    for entry in results if isinstance(results, list) else []:
        if not isinstance(entry, dict):
            continue
        key = _key(str(entry.get("query") or ""))
        verdict = str(entry.get("verdict") or "").strip().lower()
        if key and verdict in {"film", "ambiguous", "wrong"}:
            try:
                confidence = max(0, min(100, int(float(entry.get("confidence") or 0))))
            except TypeError, ValueError:
                confidence = 0
            out[key] = {"verdict": verdict, "confidence": confidence, "reason": str(entry.get("reason") or "")}
    return out


async def _prune(dry_run: bool) -> int:
    now = datetime.now(tz=UTC)
    full_cutoff = (now - timedelta(days=PRUNE_AFTER_DAYS)).isoformat()
    cutoff = (now - timedelta(days=EARLY_PRUNE_DAYS)).isoformat()
    rows = await d1_query(
        "SELECT d.movie_id, d.tag, d.display, d.source, d.seen, d.keyword_id, d.decided_at, "
        "(SELECT count(*) FROM posts p WHERE p.keyword_id = d.keyword_id) AS found, "
        "(SELECT m.released_at FROM movies m WHERE m.id = d.movie_id) AS released_at "
        "FROM hashtag_ai_decisions d WHERE d.source = 'query' AND d.status = 'added' AND d.decided_at < ? AND d.keyword_id IS NOT NULL",
        [cutoff],
    )
    yields: dict[str, dict[str, Any]] = {}
    for platform in PLATFORMS:
        try:
            yields.update(await keyword_yields(platform))
        except Exception as exc:  # noqa: BLE001 - không đo được thì chỉ dọn theo số bài mới như trước
            logger.warning("query_ai_yield_failed", platform=platform, error=str(exc)[:200])
    pruned = 0
    # Phim chưa chiếu (hoặc mới chiếu chưa đủ PRUNE_AFTER_DAYS ngày) ít bài là bình thường - không dọn; câu 0 bài đã được
    # keyword_yield cho cào thưa (mỗi ZERO_EVERY_DAYS ngày) nên không tốn nhiều.
    grace_until = (now - timedelta(days=PRUNE_AFTER_DAYS)).date().isoformat()
    for row in rows or []:
        if str(row.get("released_at") or "")[:10] > grace_until:
            continue
        stats = yields.get(str(row["keyword_id"])) or {}
        found = int(row.get("found") or 0)
        if str(row.get("decided_at") or "") >= full_cutoff:
            # Chưa đủ PRUNE_AFTER_DAYS ngày: chỉ tắt sớm câu hoàn toàn không ra gì.
            if found or stats.get("matched", 0):
                continue
            junk = False
            early = True
        else:
            junk = stats.get("sample", 0) >= PRUNE_MIN_SAMPLE and stats.get("ratio", 1) < PRUNE_BELOW_RATIO
            early = False
            if found >= MIN_NEW_POSTS and not junk:
                continue
        pruned += 1
        logger.info(
            "query_ai_pruned", movie_id=row["movie_id"], query=row["display"], found=row["found"], dry_run=dry_run
        )
        if dry_run:
            continue
        await d1_query("UPDATE keywords SET enabled = 0 WHERE id = ?", [row["keyword_id"]])
        reason = (
            f"tỉ lệ bài liên quan chỉ {round(stats['ratio'] * 100)}% trên {stats['sample']} bài"
            if junk
            else f"{EARLY_PRUNE_DAYS} ngày không ra bài nào khớp ({stats.get('dropped', 0)} bài lạc đề)"
            if early
            else f"{PRUNE_AFTER_DAYS} ngày chỉ đem về {row['found']} bài mới"
        )
        await _record(row["movie_id"], row, {"reason": reason}, "pruned", row["keyword_id"])
    return pruned


async def _revive_upcoming(dry_run: bool) -> int:
    """Bật lại câu mà lượt dọn đã tắt (status 'pruned') của phim CHƯA CHIẾU - trước 20/10/2026 câu của phim sắp chiếu bị tắt
    sau 1 ngày vì chưa có bài. Câu người vận hành tắt không có quyết định 'pruned' nên không bị đụng tới."""
    today = datetime.now(tz=UTC).date().isoformat()
    rows = await d1_query(
        "SELECT d.movie_id, d.tag, d.display, d.source, d.seen, d.keyword_id, m.title FROM hashtag_ai_decisions d "
        "JOIN movies m ON m.id = d.movie_id JOIN keywords k ON k.id = d.keyword_id "
        "WHERE d.source = 'query' AND d.status = 'pruned' AND m.enabled = 1 AND m.released_at > ? AND k.enabled = 0",
        [today],
    )
    revived = 0
    for row in rows or []:
        platform = str(row["tag"]).split(":", 1)[0]
        if platform in TEMPLATE_ONLY_PLATFORMS and _key(row["display"]) not in {
            _key(q) for q in candidate_queries({"title": row["title"]})
        }:
            continue
        revived += 1
        logger.info("query_ai_revived", movie_id=row["movie_id"], query=row["display"], dry_run=dry_run)
        if dry_run:
            continue
        await d1_query("UPDATE keywords SET enabled = 1 WHERE id = ?", [row["keyword_id"]])
        await _record(row["movie_id"], row, {"reason": "bật lại: phim chưa chiếu"}, "added", row["keyword_id"])
    return revived


async def _backfill_threads_templates(
    movies: list[dict[str, Any]],
    keywords: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    now: datetime,
    dry_run: bool,
) -> int:
    """Thêm cho Threads các câu theo mẫu (QUERY_TEMPLATES, không gồm câu diễn viên) đã dùng cho Facebook của cùng phim - Kira
    đã chấm "film", hoặc là từ khoá Facebook đang bật (thường "phim <tên>" người vận hành thêm, không qua quyết định nào -
    chính mẫu ra nhiều bài nhất trên Threads) - khi phim đang theo dõi Threads mà chưa có câu đó."""
    if "threads" not in ADD_PLATFORMS or THREADS_BACKFILL_PER_RUN <= 0:
        return 0
    approved = {
        (d["movie_id"], d["tag"].split(":", 1)[-1]): d
        for d in decisions
        if d["tag"].startswith("facebook:") and d.get("verdict") == "film"
    }
    threads_tags = {(d["movie_id"], d["tag"]) for d in decisions if d["tag"].startswith("threads:")}
    added = 0

    def days_from_release(movie: dict[str, Any]) -> float:
        try:
            day = datetime.fromisoformat(str(movie.get("released_at") or "")[:10]).replace(tzinfo=UTC)
        except ValueError:
            return float("inf")
        return abs((day - now).total_seconds())

    for movie in sorted((m for m in movies if _active(m, now)), key=days_from_release):
        if added >= THREADS_BACKFILL_PER_RUN:
            break
        own = {_key(k["keyword"]) for k in keywords if k["movie_id"] == movie["id"] and k["platform"] == "threads"}
        if not own:
            continue  # phim không theo dõi Threads
        facebook = {
            _key(k["keyword"]): k["keyword"]
            for k in keywords
            if k["movie_id"] == movie["id"] and k["platform"] == "facebook" and k.get("enabled", 1)
        }
        title = _search_title(str(movie.get("title") or ""))
        here = 0
        for query in (template.format(title=title) for template in QUERY_TEMPLATES) if title else ():
            key = _key(query)
            decision = approved.get((movie["id"], key))
            if decision is None and key in facebook:
                decision = {"display": facebook[key], "confidence": None}
            if decision is None or key in own or (movie["id"], f"threads:{key}") in threads_tags:
                continue
            if here >= THREADS_BACKFILL_PER_MOVIE or added >= THREADS_BACKFILL_PER_RUN:
                break
            here += 1
            added += 1
            logger.info("query_threads_backfill", movie=movie["title"], query=decision["display"], dry_run=dry_run)
            if dry_run:
                continue
            keyword_id = f"kw_{uuid.uuid4()}"
            await d1_query(
                "INSERT INTO keywords (id, movie_id, platform, keyword, enabled, created_at, related_keywords) VALUES (?, ?, 'threads', ?, 1, ?, '[]')",
                [keyword_id, movie["id"], decision["display"], now.isoformat()],
            )
            cand = {"tag": f"threads:{key}", "display": decision["display"], "source": "query", "seen": 0}
            verdict = {
                "verdict": "film",
                "confidence": decision.get("confidence"),
                "reason": "thêm bù cho Threads (đã duyệt cho Facebook)",
            }
            await _record(movie["id"], cand, verdict, "added", keyword_id)
    return added


async def run_query_discovery(*, dry_run: bool = False, movie_id: str | None = None) -> dict[str, Any]:
    await _ensure_table()
    pruned = await _prune(dry_run)
    revived = await _revive_upcoming(dry_run)
    now = datetime.now(tz=UTC)
    movies = (
        await d1_query(
            'SELECT id, title, director, "cast", distributor, released_at, description FROM movies WHERE enabled = 1',
            quiet=True,
        )
        or []
    )
    keywords = await d1_query("SELECT id, movie_id, platform, keyword, enabled FROM keywords", quiet=True) or []
    decisions = (
        await d1_query(
            "SELECT movie_id, tag, display, status, verdict, confidence, decided_at FROM hashtag_ai_decisions "
            "WHERE source = 'query'",
            quiet=True,
        )
        or []
    )
    recheck_before = (now - timedelta(days=RECHECK_REJECTED_DAYS)).isoformat()
    decided = {
        (d["movie_id"], d["tag"].split(":", 1)[-1])
        for d in decisions
        if d["status"] != "rejected" or str(d["decided_at"]) >= recheck_before
    }
    backfilled = 0 if movie_id else await _backfill_threads_templates(movies, keywords, decisions, now, dry_run)
    titles = [m["title"] for m in movies if m.get("title")]

    stats = {
        "movies": 0,
        "candidates": 0,
        "asked": 0,
        "added": 0,
        "rejected": 0,
        "pruned": pruned,
        "revived": revived,
        "threads_backfilled": backfilled,
        "failed": 0,
    }

    def days_from_release(movie: dict[str, Any]) -> float:
        try:
            day = datetime.fromisoformat(str(movie.get("released_at") or "")[:10]).replace(tzinfo=UTC)
        except ValueError:
            return float("inf")
        return abs((day - now).total_seconds())

    active = [m for m in movies if _active(m, now) and (not movie_id or m["id"] == movie_id)]
    generate_for = {m["id"] for m in sorted(active, key=days_from_release)[:MAX_GENERATE_PER_RUN]}
    for movie in movies:
        if movie_id and movie["id"] != movie_id:
            continue
        if not _active(movie, now) or stats["added"] >= MAX_ADDS_PER_RUN:
            continue
        own = [k for k in keywords if k["movie_id"] == movie["id"]]
        platforms = [p for p in ADD_PLATFORMS if any(k["platform"] == p for k in own)]
        if not platforms:
            continue
        stats["movies"] += 1
        # Đã là từ khoá của phim (mọi trạng thái - người vận hành đã tắt/xoá thì không thêm lại) hoặc đã quyết trước đó.
        # Chỉ từ khoá tìm kiếm FB/Threads: hashtag TikTok "#phimbosuabay" chuẩn hoá trùng "phim Bò Sữa Bay" và trước 24/10/2026
        # chặn câu "phim <tên>" - mẫu ra nhiều bài nhất trên Threads - không bao giờ được xét.
        existing = {_key(k["keyword"]) for k in own if k["platform"] in PLATFORMS}
        generated: list[str] = []
        if movie["id"] in generate_for:
            try:
                generated = await _generate_queries(movie, [k["keyword"] for k in own])
            except Exception as exc:  # noqa: BLE001 - AI sinh câu lỗi: vẫn xét các câu theo mẫu
                logger.warning("query_ai_generate_failed", movie_id=movie["id"], error=str(exc)[:300])
        stats["generated"] = stats.get("generated", 0) + len(generated)
        templated = {_key(q) for q in candidate_queries(movie)}
        queries = [
            q
            for q in dict.fromkeys([*candidate_queries(movie), *generated])
            if _key(q) not in existing and (movie["id"], _key(q)) not in decided
        ]
        if not queries:
            continue
        stats["candidates"] += len(queries)
        try:
            verdicts = await _ask_kira(movie, queries, titles)
        except Exception as exc:  # noqa: BLE001 - Kira tắt/lỗi: phim này để lượt sau
            stats["failed"] += 1
            logger.warning("query_ai_kira_failed", movie_id=movie["id"], error=str(exc)[:300])
            continue
        stats["asked"] += 1
        added_here = 0
        for query in sorted(queries, key=lambda q: -(verdicts.get(_key(q), {}).get("confidence") or 0)):
            verdict = verdicts.get(_key(query))
            if verdict is None:
                continue
            accept = (
                verdict["verdict"] == "film"
                and verdict["confidence"] >= MIN_CONFIDENCE
                and added_here < MAX_ADDS_PER_MOVIE
                and stats["added"] < MAX_ADDS_PER_RUN
            )
            logger.info(
                "query_ai_decision", movie=movie["title"], query=query, accept=accept, dry_run=dry_run, **verdict
            )
            if not accept and verdict["verdict"] == "film":
                continue  # đủ ý nhưng vượt hạn mức/độ tự tin: lượt sau xét lại
            if accept:
                added_here += 1
                stats["added"] += 1
            else:
                stats["rejected"] += 1
            if dry_run:
                continue
            for platform in platforms:
                if platform in TEMPLATE_ONLY_PLATFORMS and _key(query) not in templated:
                    continue
                cand = {"tag": f"{platform}:{_key(query)}", "display": query, "source": "query", "seen": 0}
                keyword_id = None
                if accept:
                    keyword_id = f"kw_{uuid.uuid4()}"
                    await d1_query(
                        "INSERT INTO keywords (id, movie_id, platform, keyword, enabled, created_at, related_keywords) VALUES (?, ?, ?, ?, 1, ?, '[]')",
                        [keyword_id, movie["id"], platform, query, datetime.now(tz=UTC).isoformat()],
                    )
                await _record(movie["id"], cand, verdict, "added" if accept else "rejected", keyword_id)
    logger.info("query_ai_discovery_finished", dry_run=dry_run, telegram=bool(stats["added"]) and not dry_run, **stats)
    return stats
