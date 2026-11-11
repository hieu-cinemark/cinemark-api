"""AI tìm thêm hashtag TikTok cho từng phim đang theo dõi (TikTok chỉ tìm bài được qua hashtag).

Mỗi ngày một lượt (và sau mỗi lượt Omar):

1. Ứng viên: biến thể tự sinh từ tên phim + hashtag đi kèm mà lượt cào đã gom.
2. Luật trước khi gọi AI: bỏ tag đã là từ khoá (kể cả đã tắt), tag đã quyết, tag chung chung, tag chứa tên phim khác.
3. AI đọc thông tin phim và chấm từng tag; chỉ thêm khi gọi đích danh phim này với độ tự tin đủ, có trần mỗi phim và
   mỗi lượt.
4. Tag mới lại góp ứng viên (BFS tới `BFS_MAX_DEPTH` tầng).
5. Tag AI thêm mà sau `PRUNE_AFTER_DAYS` ngày không đem về bài nào thì tắt.

Mọi quyết định ghi bảng `hashtag_ai_decisions`; tag bị bỏ được xét lại sau `RECHECK_REJECTED_DAYS` ngày.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Column, Integer, Table, Text, exists, select, text
from sqlalchemy.schema import CreateTable

from app.ai.domain.movie_context import movie_context_block
from app.ai.runtime import call_llm, parse_json_response
from app.clients.d1 import d1_query
from app.clients.kafka import publish_crawl_request
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec, d1_insert_many, metadata
from app.repositories.d1.store import add_keyword, enable_keyword, related_hashtags_for_keywords
from app.repositories.d1.tables import keywords, movies, posts
from app.services.omar.omar_tiktok import hashtag_exists
from app.services.relevance.relevance_rules import normalize_title, title_acronym

logger = get_logger(__name__)

TASK = "hashtag_discovery"
MIN_CONFIDENCE = 80
MAX_CANDIDATES_PER_MOVIE = 30
# BFS: tối đa chừng này tầng tính từ từ khoá gốc (người nhập = 0) để không lan sang chủ đề khác.
BFS_MAX_DEPTH = 2
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


# Quyết định của AI cho từng tag/câu tìm (source: hashtag/query); dùng chung với query_discovery.py.
decisions_table = Table(
    "hashtag_ai_decisions",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("tag", Text, primary_key=True),
    Column("display", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("seen", Integer, nullable=False, server_default=text("0")),
    Column("verdict", Text),
    Column("confidence", Integer),
    Column("reason", Text),
    Column("status", Text, nullable=False),
    Column("keyword_id", Text),
    Column("decided_at", Text, nullable=False),
    # Cột BFS (thêm sau): tag được sinh từ từ khoá nào và ở tầng mấy.
    Column("parent_keyword_id", Text),
    Column("depth", Integer, nullable=False, server_default=text("0")),
)
# Thông tin phim đưa vào prompt.
ALL_KEYWORDS = select(*(keywords.c[name] for name in ("id", "movie_id", "platform", "keyword", "enabled")))
# Phim đang bật kèm thông tin đưa vào prompt.
ENABLED_MOVIES = select(
    *(
        movies.c[name]
        for name in ("id", "title", "slug", "director", "cast", "distributor", "released_at", "description")
    )
).where(movies.c.enabled == 1)


async def _ensure_table() -> None:
    await d1_exec(CreateTable(decisions_table, if_not_exists=True), quiet=True)
    # Bảng cũ thiếu cột BFS: ALTER (lỗi khi cột đã có - bỏ qua).
    for column in ("parent_keyword_id TEXT", "depth INTEGER NOT NULL DEFAULT 0"):
        await d1_query(f"ALTER TABLE hashtag_ai_decisions ADD COLUMN {column}", quiet=True)


async def _record(
    movie_id: str, cand: dict[str, Any], verdict: dict[str, Any] | None, status: str, keyword_id: str | None
) -> None:
    verdict = verdict or {}
    await d1_insert_many(
        decisions_table,
        [
            {
                "movie_id": movie_id,
                "tag": cand["tag"],
                "display": cand["display"],
                "source": cand["source"],
                "seen": cand.get("seen", 0),
                "verdict": verdict.get("verdict"),
                "confidence": verdict.get("confidence"),
                "reason": (verdict.get("reason") or "")[:300] or None,
                "status": status,
                "keyword_id": keyword_id,
                "decided_at": datetime.now(tz=UTC).isoformat(),
                "parent_keyword_id": cand.get("parent_keyword_id"),
                "depth": int(cand.get("depth") or 0),
            }
        ],
        conflict=["movie_id", "tag"],
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
    raw = await call_llm(task=TASK, system_prompt=SYSTEM_PROMPT, user_prompt=user, max_tokens=8000)
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
    d = decisions_table.c
    rows = await d1_exec(
        select(d.movie_id, d.tag, d.display, d.source, d.seen, d.keyword_id).where(
            d.status == "added",
            d.decided_at < cutoff,
            d.keyword_id.is_not(None),
            ~exists().where(posts.c.keyword_id == d.keyword_id),
        )
    )
    for row in rows or []:
        logger.info("hashtag_ai_pruned", movie_id=row["movie_id"], tag=row["display"], dry_run=dry_run)
        if dry_run:
            continue
        await enable_keyword(row["keyword_id"], False)
        await _record(
            row["movie_id"], row, {"reason": f"{PRUNE_AFTER_DAYS} ngày không có bài nào"}, "pruned", row["keyword_id"]
        )
    return len(rows or [])


async def run_discovery(*, dry_run: bool = False, movie_id: str | None = None) -> dict[str, Any]:
    await _ensure_table()
    pruned = await _prune(dry_run)
    movies = await d1_exec(ENABLED_MOVIES, quiet=True) or []
    keywords = await d1_exec(ALL_KEYWORDS, quiet=True) or []
    decisions = (
        await d1_exec(
            select(
                *(
                    decisions_table.c[name]
                    for name in ("movie_id", "tag", "status", "decided_at", "keyword_id", "depth")
                )
            ),
            quiet=True,
        )
        or []
    )
    # Tầng BFS của mỗi từ khoá: từ khoá AI thêm mang depth đã ghi, từ khoá người nhập là 0.
    depth_of = {d["keyword_id"]: int(d.get("depth") or 0) for d in decisions if d.get("keyword_id")}

    related = await related_hashtags_for_keywords(
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
    # Tag đã là từ khoá hashtag của BẤT KỲ phim nào (kể cả đang tắt) thì không thêm nữa. Chỉ so từ khoá dạng "#..." /
    # của TikTok: câu tìm kiếm Facebook "phim Bò Sữa Bay" (query_discovery) không được chặn hashtag #PhimBoSuaBay.
    existing = {
        _squash(k["keyword"].lstrip("#")) for k in keywords if k["platform"] == "tiktok" or k["keyword"].startswith("#")
    }

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
            # Hashtag đi kèm (spider + Omar, d1.related_hashtags_for_keywords) - chỉ khi từ khoá cha chưa ở tầng sâu nhất.
            parent_depth = depth_of.get(kw["id"], 0)
            if parent_depth >= BFS_MAX_DEPTH:
                continue
            for item in related.get(kw["id"], []):
                tag = _squash(item["title"])
                entry = cands.setdefault(
                    tag,
                    {
                        "tag": tag,
                        "display": item["title"],
                        "source": "related",
                        "seen": 0,
                        "parent_keyword_id": kw["id"],
                        "depth": parent_depth + 1,
                    },
                )
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
        # Biến thể tự sinh (HoLinhTrangSiMovie, ChiChiEmEm32026...) phần lớn KHÔNG có trên TikTok - kiểm tra
        # qua Omar trước khi hỏi Kira; không có thì ghi "rejected" (xét lại sau RECHECK_REJECTED_DAYS ngày). Hashtag đi kèm
        # lấy từ video thật nên không cần kiểm tra.
        for tag, cand in list(cands.items()):
            if cand["source"] != "variant" or await hashtag_exists(cand["display"]) is not False:
                continue
            del cands[tag]
            stats["not_on_tiktok"] = stats.get("not_on_tiktok", 0) + 1
            if not dry_run:
                verdict = {"verdict": "not_on_tiktok", "confidence": 100, "reason": "Không có video trên TikTok (Omar)"}
                await _record(movie["id"], cand, verdict, "rejected", None)
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
                and added_here < settings.hashtag_discovery_max_per_movie
                and added_total < settings.hashtag_discovery_max_per_run
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
            await add_keyword(movie["id"], "tiktok", keyword, keyword_id)
            await _record(movie["id"], cand, verdict, "added", keyword_id)
            await publish_crawl_request(platform="tiktok", keyword=keyword, keyword_id=keyword_id)
    logger.info(
        "hashtag_ai_discovery_finished", dry_run=dry_run, telegram=bool(stats["added"]) and not dry_run, **stats
    )
    return stats
