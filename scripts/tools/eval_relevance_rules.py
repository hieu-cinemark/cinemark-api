"""Đo luật độ liên quan (relevance_rules.resolve_relevance) trên bộ bài đã rà soát tay data/visobert/relevance_hand.jsonl
(scripts/ml/relevance_hand_finetune.py --build). Luật được xem là "nâng" khi trả related; nhãn đúng: related = nên hiện, not_related /
uncertain = nên ẩn. In độ phủ (bắt được bao nhiêu bài thật), độ chính xác (bài nâng có đúng không) và các bài sai để sửa luật.

    python -m scripts.tools.eval_relevance_rules            # tóm tắt
    python -m scripts.tools.eval_relevance_rules --errors 25
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter, defaultdict
from pathlib import Path

from app.clients.d1 import d1_query
from app.core.config import settings
from app.services.relevance.relevance_rules import resolve_relevance

HAND = Path(__file__).resolve().parents[2] / "data" / "visobert" / "relevance_hand.jsonl"


async def load_movies() -> dict[str, dict]:
    rows = (
        await d1_query('SELECT id, title, slug, released_at, director, "cast", distributor FROM movies', timeout=60)
        or []
    )
    movies = {r["id"]: {**r, "keywords": []} for r in rows}
    for r in await d1_query("SELECT movie_id, keyword FROM keywords WHERE enabled = 1", timeout=60) or []:
        if r["movie_id"] in movies:
            movies[r["movie_id"]]["keywords"].append(r["keyword"])
    return movies


def decide(row: dict, movies: dict[str, dict]) -> tuple[bool, str | None]:
    movie = movies.get(row["movie_id"]) or {}
    slug = movie.get("slug")
    ok, _, ctx = resolve_relevance(
        None,
        row["text"],
        movie,
        has_keyword=True,
        strict=settings.is_strict_movie(slug, "tiktok"),
        full_title_rule=slug not in settings.ambiguous_title_movies,
        posted_at=row.get("posted_at"),
    )
    return ok is True, ctx


async def main(errors: int) -> None:
    movies = await load_movies()
    rows = [json.loads(line) for line in HAND.read_text(encoding="utf-8").split("\n") if line]
    lifted = Counter()
    wrong: list[tuple[str, dict, str | None]] = []
    missed: list[dict] = []
    for row in rows:
        up, ctx = decide(row, movies)
        gold = row["label"]
        lifted[(gold, up)] += 1
        if up and gold != "related":
            wrong.append((gold, row, ctx))
        if not up and gold == "related":
            missed.append(row)
    total_related = sum(n for (g, _), n in lifted.items() if g == "related")
    hit = lifted[("related", True)]
    all_up = sum(n for (_, u), n in lifted.items() if u)
    print(f"{len(rows)} bài rà soát: {dict(Counter(r['label'] for r in rows))}")
    print(f"độ phủ: luật nâng {hit}/{total_related} bài thật ({hit / max(1, total_related):.0%})")
    print(
        f"độ chính xác: {hit}/{all_up} bài luật nâng là thật ({hit / max(1, all_up):.0%}) · nâng nhầm: "
        f"not_related {lifted[('not_related', True)]}, uncertain {lifted[('uncertain', True)]}"
    )
    print("nâng nhầm theo lý do:", dict(Counter(c for _, _, c in wrong)))
    by_movie: dict[str, Counter] = defaultdict(Counter)
    for row in missed:
        by_movie[(movies.get(row["movie_id"]) or {}).get("title", "?")[:24]]["missed"] += 1
    print("bỏ sót theo phim:", {k: v["missed"] for k, v in sorted(by_movie.items(), key=lambda x: -x[1]["missed"])[:8]})
    for gold, row, ctx in wrong[:errors]:
        print(f"  NÂNG NHẦM [{gold}|{ctx}] {row['film'][:26]} | {row['text'][:110]!r}")
    for row in missed[:errors]:
        print(f"  BỎ SÓT    {row['film'][:26]} | {row['text'][:110]!r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--errors", type=int, default=0)
    asyncio.run(main(parser.parse_args().errors))
