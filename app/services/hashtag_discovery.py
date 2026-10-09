"""AI tự tìm thêm hashtag TikTok cho từng phim đang theo dõi (từ 2026-10-09) - TikTok chỉ tìm được bài qua hashtag
(crawl_request_consumer bỏ qua từ khoá không có "#"), mà mỗi phim có rất nhiều cách viết: đo 2026-10-09, "#phim<tên>"
thường đem về nhiều bài hơn chính hashtag gốc (#phimchuyendinhodoi 260 bài/14 ngày so với #ChuyenDiNhoDoi 138), và 12
phim chỉ có đúng một hashtag.

Mỗi ngày một lượt (scheduler._hashtag_tick, hoặc scripts.discover_hashtags):
1. Ứng viên của mỗi phim: biến thể tự sinh từ tên phim (variant_hashtags) + các hashtag đi kèm mà lượt crawl TikTok
   đã gom và Kira đã loại tag chung chung (luồng BFS, Redis tiktok:related_hashtags:{keyword_id}).
2. Luật cứng trước khi tốn lời gọi AI: bỏ tag đã là từ khoá của phim (kể cả đang TẮT - người vận hành đã tắt thì không
   bao giờ thêm lại), tag đã quyết trước đó, tag chung chung (_GENERIC), tag chứa tên một phim khác đang theo dõi.
3. Kira đọc thông tin phim (movie_context_block) và chấm từng tag: "film" chỉ khi tag gọi đích danh phim này.
4. Ràng buộc khi thêm: verdict "film" + confidence >= MIN_CONFIDENCE, tối đa MAX_ADDS_PER_MOVIE mỗi phim mỗi lượt và
   MAX_ADDS_PER_RUN cả lượt; thêm thành từ khoá TikTok đang bật và xếp crawl ngay.
5. Tự dọn: tag AI đã thêm mà sau PRUNE_AFTER_DAYS ngày không đem về bài nào thì tắt (status "pruned").

Mọi quyết định (thêm/bỏ/dọn, kèm lý do và độ tự tin) nằm trong bảng hashtag_ai_decisions (D1 scraper) - tag bị bỏ
được xét lại sau RECHECK_REJECTED_DAYS ngày (số bài đã khác)."""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from app.ai.kira import call_kira, parse_json_response
from app.ai.movie_context import movie_context_block
from app.clients.kafka import publish_crawl_request
from app.core.logging import get_logger
from app.services.d1 import _related_hashtags_for_keywords, d1_query
from app.services.relevance_rules import normalize_title, title_acronym

logger = get_logger(__name__)

TASK = "hashtag_discovery"
MIN_CONFIDENCE = 80
MAX_ADDS_PER_MOVIE = 3
MAX_ADDS_PER_RUN = 20
MAX_CANDIDATES_PER_MOVIE = 20
PRUNE_AFTER_DAYS = 3
RECHECK_REJECTED_DAYS = 14

# Tag chung chung không bao giờ là hashtag riêng của một phim - bỏ trước khi hỏi AI.
_GENERIC = frozenset(
    [
        "fyp",
        "foryou",
        "foryoupage",
        "xuhuong",
        "xuhuongtiktok",
        "trending",
        "viral",
        "tiktok",
        "tiktokvietnam",
        "capcut",
        "phim",
        "phimhay",
        "phimmoi",
        "phimviet",
        "phimvietnam",
        "phimchieurap",
        "phimrap",
        "phimtet",
        "review",
        "reviewphim",
        "cinema",
        "movie",
        "movies",
        "film",
        "cgv",
        "lotte",
        "galaxy",
        "bhd",
        "betacinemas",
        "cinestar",
        "rap",
        "rapphim",
        "trailer",
        "teaser",
        "hot",
        "giaitri",
        "tiktokgiaitri",
        "kinhdi",
        "hai",
        "haihuoc",
        "tinhcam",
        "hanhdong",
        "hoathinh",
        "vietnam",
        "saigon",
        "hanoi",
    ]
)

SYSTEM_PROMPT = """Bạn lọc hashtag TikTok cho một hệ thống theo dõi thảo luận về MỘT bộ phim chiếu rạp cụ thể (TARGET FILM).
Hệ thống sẽ tự động thu thập MỌI video gắn hashtag bạn chấp nhận, nên chấp nhận sai sẽ kéo về hàng loạt video rác.

Chỉ trả "film" khi hashtag gọi ĐÍCH DANH phim này:
- tên phim viết liền, có/không dấu, có tiền tố/hậu tố như phim, movie, film, năm, số phần (#phimholinhtrangsi, #holinhtrangsimovie, #holinhtrangsi2026);
- tên viết tắt RIÊNG của phim mà nhìn vào biết ngay là phim này (#hlts cho "Hộ Linh Tráng Sĩ") - nếu viết tắt có thể là thứ khác thì KHÔNG;
- tên phim kèm nhà phát hành/đạo diễn/chiến dịch ra mắt (#traibuonnguoibymoli);
- tên diễn viên/đạo diễn/nhân vật GHÉP CÙNG tên phim, ở bất kỳ thứ tự nào (#minhhangmemin, #meminminhhang, #tranthanhholinhtrangsi) - có tên phim trong hashtag là đủ;
- tên tiếng Anh chính thức của phim, hoặc hashtag chiến dịch truyền thông chỉ dùng cho phim này.

Trả "generic" cho: tag xu hướng/nền tảng (#fyp, #xuhuong), thể loại (#phimkinhdi), từ thông dụng, tên rạp/chuỗi rạp, tag review/trailer chung.
Trả "other" cho: tên một phim/series/chương trình khác, tên bài hát, tên diễn viên/nhân vật đứng một mình, KHÔNG kèm tên phim (diễn viên đóng nhiều thứ khác), tên nhân vật trong truyện/game.
Trả "unrelated" cho mọi thứ không liên quan tới phim.
Tên phim trùng cụm từ thông dụng (vd. "Người Được Chọn", "Loạn Thế") thì hashtag chỉ gồm đúng cụm từ đó là "generic" trừ khi có thêm tiền tố/hậu tố phim (#phimnguoiduocchon).
Không chắc thì KHÔNG trả "film" - bỏ sót một hashtag rẻ hơn nhiều so với kéo về video rác.

Trả về JSON hợp lệ, không bọc code block:
{"results":[{"tag":"<đúng như được đưa, không có #>","verdict":"film|generic|other|unrelated","confidence":0-100,"reason":"một câu ngắn tiếng Việt"}]}"""


def _squash(value: str | None) -> str:
    return normalize_title(value).replace(" ", "")


def _camel(value: str) -> str:
    return "".join(word.capitalize() for word in normalize_title(value).split())


def variant_hashtags(movie: dict[str, Any]) -> list[str]:
    """Biến thể hashtag từ tên phim ("Hộ Linh Tráng Sĩ" -> HoLinhTrangSi, PhimHoLinhTrangSi, HoLinhTrangSiMovie,
    HoLinhTrangSi2026, HLTS; tên có phụ đề "A: B" thêm cả A và B). Chưa lọc - AI và luật cứng quyết định."""
    title = str(movie.get("title") or "")
    parts = (
        [title, *[part for part in re.split(r"[:|–-]", title) if part.strip()]]
        if re.search(r"[:|–-]", title)
        else [title]
    )
    year = str(movie.get("released_at") or "")[:4]
    out: list[str] = []
    for part in parts:
        base = _camel(part)
        if len(base) < 6:
            continue
        out += [base, f"Phim{base}", f"{base}Movie"]
        if year.isdigit():
            out.append(f"{base}{year}")
    acronym = title_acronym(title)
    if acronym and len(acronym) >= 3:
        out.append(acronym.upper())
    return list(dict.fromkeys(out))


async def _ensure_table() -> None:
    await d1_query(
        """CREATE TABLE IF NOT EXISTS hashtag_ai_decisions (
            movie_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            display TEXT NOT NULL,
            source TEXT NOT NULL,
            seen INTEGER NOT NULL DEFAULT 0,
            verdict TEXT,
            confidence INTEGER,
            reason TEXT,
            status TEXT NOT NULL,
            keyword_id TEXT,
            decided_at TEXT NOT NULL,
            PRIMARY KEY (movie_id, tag)
        )""",
        quiet=True,
    )


async def _record(
    movie_id: str, cand: dict[str, Any], verdict: dict[str, Any] | None, status: str, keyword_id: str | None
) -> None:
    await d1_query(
        "INSERT INTO hashtag_ai_decisions (movie_id, tag, display, source, seen, verdict, confidence, reason, status, keyword_id, decided_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(movie_id, tag) DO UPDATE SET display = excluded.display, "
        "source = excluded.source, seen = excluded.seen, verdict = excluded.verdict, confidence = excluded.confidence, "
        "reason = excluded.reason, status = excluded.status, keyword_id = excluded.keyword_id, decided_at = excluded.decided_at",
        [
            movie_id,
            cand["tag"],
            cand["display"],
            cand["source"],
            cand.get("seen", 0),
            (verdict or {}).get("verdict"),
            (verdict or {}).get("confidence"),
            ((verdict or {}).get("reason") or "")[:300] or None,
            status,
            keyword_id,
            datetime.now(tz=UTC).isoformat(),
        ],
    )


async def _ask_kira(
    movie: dict[str, Any], candidates: list[dict[str, Any]], other_titles: Iterable[str]
) -> dict[str, dict[str, Any]]:
    others = [t for t in other_titles if t != movie.get("title")][:40]
    user = (
        f"{movie_context_block(movie)}\n"
        f'Other films being tracked (hashtags of these are "other"): {", ".join(others)}\n\n'
        "Candidate hashtags (seen = number of TikTok videos it appeared on together with this film's hashtags; 0 = generated from the title):\n"
        + "\n".join(f"- {c['display']} (seen={c.get('seen', 0)})" for c in candidates)
    )
    raw = await call_kira(task=TASK, system_prompt=SYSTEM_PROMPT, user_prompt=user, max_tokens=8000)
    parsed = parse_json_response(raw)
    results = parsed.get("results") if isinstance(parsed, dict) else None
    out: dict[str, dict[str, Any]] = {}
    for entry in results if isinstance(results, list) else []:
        if not isinstance(entry, dict):
            continue
        tag = _squash(str(entry.get("tag") or ""))
        verdict = str(entry.get("verdict") or "").strip().lower()
        if tag and verdict in {"film", "generic", "other", "unrelated"}:
            try:
                confidence = max(0, min(100, int(float(entry.get("confidence") or 0))))
            except TypeError, ValueError:
                confidence = 0
            out[tag] = {"verdict": verdict, "confidence": confidence, "reason": str(entry.get("reason") or "")}
    return out


async def _prune(dry_run: bool) -> int:
    """Tắt tag AI đã thêm mà PRUNE_AFTER_DAYS ngày không đem về bài nào."""
    cutoff = (datetime.now(tz=UTC) - timedelta(days=PRUNE_AFTER_DAYS)).isoformat()
    rows = await d1_query(
        "SELECT d.movie_id, d.tag, d.display, d.source, d.seen, d.keyword_id FROM hashtag_ai_decisions d "
        "WHERE d.status = 'added' AND d.decided_at < ? AND d.keyword_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM posts p WHERE p.keyword_id = d.keyword_id)",
        [cutoff],
    )
    for row in rows or []:
        logger.info("hashtag_ai_pruned", movie_id=row["movie_id"], tag=row["display"], dry_run=dry_run)
        if dry_run:
            continue
        await d1_query("UPDATE keywords SET enabled = 0 WHERE id = ?", [row["keyword_id"]])
        await _record(
            row["movie_id"], row, {"reason": f"{PRUNE_AFTER_DAYS} ngày không có bài nào"}, "pruned", row["keyword_id"]
        )
    return len(rows or [])


async def run_discovery(*, dry_run: bool = False, movie_id: str | None = None) -> dict[str, Any]:
    await _ensure_table()
    pruned = await _prune(dry_run)
    movies = (
        await d1_query(
            'SELECT id, title, slug, director, "cast", distributor, released_at, description FROM movies WHERE enabled = 1',
            quiet=True,
        )
        or []
    )
    keywords = await d1_query("SELECT id, movie_id, platform, keyword, enabled FROM keywords", quiet=True) or []
    decisions = await d1_query("SELECT movie_id, tag, status, decided_at FROM hashtag_ai_decisions", quiet=True) or []
    related = await _related_hashtags_for_keywords(
        [k["id"] for k in keywords if k["platform"] == "tiktok" and k["enabled"]]
    )

    titles = [m["title"] for m in movies if m.get("title")]
    title_tags = {m["id"]: _squash(m["title"]) for m in movies}
    recheck_before = (datetime.now(tz=UTC) - timedelta(days=RECHECK_REJECTED_DAYS)).isoformat()
    decided = {
        (d["movie_id"], d["tag"])
        for d in decisions
        if d["status"] != "rejected" or str(d["decided_at"]) >= recheck_before
    }
    # Tag đã là từ khoá của BẤT KỲ phim nào (mọi nền tảng, kể cả đang tắt) thì không thêm nữa.
    existing = {_squash(k["keyword"].lstrip("#")) for k in keywords}

    stats = {"movies": 0, "candidates": 0, "asked": 0, "added": 0, "rejected": 0, "pruned": pruned, "failed": 0}
    added_total = 0
    for movie in movies:
        if movie_id and movie["id"] != movie_id:
            continue
        stats["movies"] += 1
        cands: dict[str, dict[str, Any]] = {}
        for display in variant_hashtags(movie):
            cands.setdefault(
                _squash(display), {"tag": _squash(display), "display": display, "source": "variant", "seen": 0}
            )
        for kw in keywords:
            if kw["movie_id"] != movie["id"]:
                continue
            for item in related.get(kw["id"], []):
                tag = _squash(item["title"])
                entry = cands.setdefault(tag, {"tag": tag, "display": item["title"], "source": "related", "seen": 0})
                entry["seen"] = max(entry["seen"], int(item.get("count") or 0))
        other_tags = [t for mid, t in title_tags.items() if mid != movie["id"] and len(t) >= 6]
        cands = {
            tag: c
            for tag, c in cands.items()
            if 3 <= len(tag) <= 40
            and tag not in existing
            and tag not in _GENERIC
            and (movie["id"], tag) not in decided
            and not any(other in tag for other in other_tags)
        }
        if not cands:
            continue
        batch = sorted(cands.values(), key=lambda c: (c["source"] != "variant", -c["seen"]))[:MAX_CANDIDATES_PER_MOVIE]
        stats["candidates"] += len(batch)
        try:
            verdicts = await _ask_kira(movie, batch, titles)
        except Exception as exc:  # noqa: BLE001 - Kira tắt/lỗi: phim này để lượt sau, không chặn phim khác
            stats["failed"] += 1
            logger.warning("hashtag_ai_kira_failed", movie_id=movie["id"], error=str(exc)[:300])
            continue
        stats["asked"] += 1
        added_here = 0
        for cand in sorted(batch, key=lambda c: -(verdicts.get(c["tag"], {}).get("confidence") or 0)):
            verdict = verdicts.get(cand["tag"])
            if verdict is None:
                continue  # AI không trả lời tag này: để lượt sau
            accept = (
                verdict["verdict"] == "film"
                and verdict["confidence"] >= MIN_CONFIDENCE
                and added_here < MAX_ADDS_PER_MOVIE
                and added_total < MAX_ADDS_PER_RUN
            )
            logger.info(
                "hashtag_ai_decision",
                movie=movie["title"],
                tag=cand["display"],
                source=cand["source"],
                seen=cand["seen"],
                accept=accept,
                dry_run=dry_run,
                **verdict,
            )
            if not accept:
                if verdict["verdict"] == "film":
                    continue  # đủ ý nhưng vượt hạn mức/độ tự tin: không ghi "rejected", lượt sau xét lại
                stats["rejected"] += 1
                if not dry_run:
                    await _record(movie["id"], cand, verdict, "rejected", None)
                continue
            added_here += 1
            added_total += 1
            stats["added"] += 1
            existing.add(cand["tag"])
            if dry_run:
                continue
            keyword = f"#{cand['display'].lstrip('#')}"
            keyword_id = f"kw_{uuid.uuid4()}"
            await d1_query(
                "INSERT INTO keywords (id, movie_id, platform, keyword, enabled, created_at, related_keywords) VALUES (?, ?, 'tiktok', ?, 1, ?, '[]')",
                [keyword_id, movie["id"], keyword, datetime.now(tz=UTC).isoformat()],
            )
            await _record(movie["id"], cand, verdict, "added", keyword_id)
            await publish_crawl_request(platform="tiktok", keyword=keyword, keyword_id=keyword_id)
    logger.info(
        "hashtag_ai_discovery_finished", dry_run=dry_run, telegram=bool(stats["added"]) and not dry_run, **stats
    )
    return stats
