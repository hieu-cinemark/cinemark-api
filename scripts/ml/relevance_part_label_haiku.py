"""Gán nhãn "phần này hay phần cũ" cho bài của các phim có phần trước (2026-11-04). Prompt post_relevance chỉ biết "phim khác",
còn model nhà trồng không thấy ngày đăng, nên bài về Chị Chị Em Em 2 vẫn tính cho Chị Chị Em Em 3.

Bài lấy trực tiếp từ D1 kèm ngày đăng, chia theo năm để bài cũ không bị bài mới lấn, lưu một lần ở part_candidates.jsonl.
Nhãn ghi nối vào part_labels.jsonl - chạy lại thì bỏ qua bài đã có. Không ghi gì vào DB.

Cách dùng:
    python -m scripts.ml.relevance_part_label_haiku --limit 200 --concurrency 4
    python -m scripts.ml.relevance_part_label_haiku
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from app.core.config import settings
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import movies as movies_table
from app.repositories.d1.tables import posts as posts_table

ROOT = Path(__file__).resolve().parents[2]
CANDIDATES_FILE = ROOT / "data" / "visobert" / "part_candidates.jsonl"
OUT_FILE = ROOT / "data" / "visobert" / "part_labels.jsonl"
TASK = "post_part_label"
BATCH_SIZE = 10
PER_FILM_YEAR = 600
MAX_CONTENT = 700
LABELS = {"this_part", "other_part", "not_related", "unclear"}

FRANCHISES: dict[str, dict[str, Any]] = {
    "chi-chi-em-em-3": {"earlier": ["Chị Chị Em Em (2019)", "Chị Chị Em Em 2 (2023)"]},
    "quy-an-tang-4-ho-tinh-tee-yod": {
        "earlier": [
            "Tee Yod / Quỷ Ăn Tạng (2023)",
            "Tee Yod 2 / Quỷ Ăn Tạng 2 (2024)",
            "Tee Yod 3 / Quỷ Ăn Tạng 3 (2025)",
        ]
    },
    "ut-lan-2": {"earlier": ["Út Lan: Oán Linh Giữ Của (phần đầu)"]},
    "tham-tu-kien-loi-nguyen-hoang-kim": {"earlier": ["Thám Tử Kiên: Kỳ Án Không Đầu (2025)"]},
    "shaun-the-sheep-quai-la-ghe-nha": {
        "earlier": [
            "Shaun the Sheep Movie (2015)",
            "Shaun the Sheep: Farmageddon (2019)",
            "phim truyền hình Shaun the Sheep",
        ]
    },
}

SYSTEM_PROMPT = """You label Vietnamese social media posts for a film analytics product. Each post was collected for a TARGET film that belongs to a franchise. Decide which instalment the post is actually about.

Labels:
- this_part: the post is about the TARGET instalment (trailer, cast, schedule, tickets, reviews, reactions). A post comparing it with an earlier part, or rewatching older parts to prepare for it, is still this_part.
- other_part: the post is about an earlier instalment of the same franchise and says nothing about the TARGET one.
- not_related: not about this franchise at all (same words used in another meaning, a different film, an unrelated person or product).
- unclear: about the franchise, but the text and the date cannot show which instalment; or too short to tell.

Use the post date as evidence. A post written before the TARGET was announced cannot be about it, unless it is plainly speculation about a future sequel. A post with no part number written right around the TARGET's release is probably about the TARGET, but only choose this_part when the content fits it; otherwise choose unclear. Never guess a label from the fact that the post was collected for this film.

Reply with JSON only: {"results":[{"i":1,"label":"this_part","score":0.0,"why":"max 10 words"}]}. score is your confidence in the chosen label, from 0 to 1. Include every post."""


def _batch_prompt(rows: list[dict[str, Any]], movies: dict[str, dict[str, Any]]) -> str:
    blocks = []
    for index, row in enumerate(rows, 1):
        movie = movies[row["movie_id"]]
        earlier = "; ".join(FRANCHISES[movie["slug"]]["earlier"])
        blocks.append(
            f"[{index}] TARGET: {movie['title']} (release {str(movie['released_at'])[:10]})\n"
            f"Earlier instalments: {earlier}\n"
            f"Post date: {str(row['posted_at'] or 'unknown')[:10]}\n"
            f"Post: {(row['content'] or '')[:MAX_CONTENT]}"
        )
    return "\n\n".join(blocks)


async def _load_movies() -> dict[str, dict[str, Any]]:
    slugs = sorted(FRANCHISES)
    stmt = select(movies_table.c.id, movies_table.c.slug, movies_table.c.title, movies_table.c.released_at).where(
        movies_table.c.slug.in_(slugs)
    )
    rows = await d1_exec(stmt, timeout=60.0)
    if rows is None:
        raise RuntimeError("movies_download_failed")
    return {row["id"]: row for row in rows}


async def _load_candidates(movies: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if CANDIDATES_FILE.exists():
        return [json.loads(line) for line in CANDIDATES_FILE.read_text(encoding="utf-8").split("\n") if line]
    picked: list[dict[str, Any]] = []
    for movie_id, movie in movies.items():
        years = await d1_exec(
            select(func.distinct(func.substr(posts_table.c.posted_at, 1, 4)).label("y")).where(
                posts_table.c.movie_id == movie_id, posts_table.c.posted_at.is_not(None)
            ),
            timeout=60.0,
        )
        for year in sorted(r["y"] for r in years or []):
            stmt = (
                select(
                    posts_table.c.id,
                    posts_table.c.movie_id,
                    posts_table.c.platform,
                    posts_table.c.content,
                    posts_table.c.posted_at,
                    posts_table.c.keyword_match,
                )
                .where(
                    posts_table.c.movie_id == movie_id,
                    func.substr(posts_table.c.posted_at, 1, 4) == year,
                    func.length(posts_table.c.content) >= 20,
                )
                .order_by(posts_table.c.id)
                .limit(PER_FILM_YEAR)
            )
            rows = await d1_exec(stmt, timeout=60.0)
            if rows is None:
                raise RuntimeError(f"posts_download_failed:{movie['slug']}:{year}")
            picked.extend(rows)
            print(f"  {movie['slug']} {year}: {len(rows)}", flush=True)
    random.Random(42).shuffle(picked)  # --limit phải ra mẫu trải đều phim và năm
    CANDIDATES_FILE.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in picked), encoding="utf-8")
    return picked


async def _label_batch(rows: list[dict[str, Any]], movies: dict[str, dict[str, Any]]) -> list[dict[str, Any] | None]:
    from app.ai.runtime import call_llm, parse_json_response

    # force: công tắc AI đang tắt là để chặn bộ phân loại lúc ingest, còn đây là việc người vận hành tự chạy.
    response = await call_llm(
        task=TASK,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=_batch_prompt(rows, movies),
        max_tokens=2000 + 300 * len(rows),
        force=True,
    )
    parsed = parse_json_response(response)
    results = parsed.get("results") if isinstance(parsed, dict) else None
    out: list[dict[str, Any] | None] = [None] * len(rows)
    for entry in results if isinstance(results, list) else []:
        index = entry.get("i") if isinstance(entry, dict) else None
        if isinstance(index, int) and 1 <= index <= len(rows) and entry.get("label") in LABELS:
            out[index - 1] = {"label": entry["label"], "score": entry.get("score"), "why": entry.get("why")}
    return out


async def main(args: argparse.Namespace) -> None:
    settings.ai_provider_concurrency = args.concurrency  # semaphore của app/ai/providers/client.py đọc lúc gọi đầu tiên
    movies = await _load_movies()
    missing = set(FRANCHISES) - {m["slug"] for m in movies.values()}
    if missing:
        raise RuntimeError(f"movies_missing:{sorted(missing)}")
    candidates = await _load_candidates(movies)
    done = set()
    if OUT_FILE.exists():
        done = {json.loads(line)["id"] for line in OUT_FILE.read_text(encoding="utf-8").split("\n") if line}
    todo = [r for r in candidates if r["id"] not in done]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(todo)} bài cần gán ({len(done)} đã có, {len(candidates)} bài ứng viên)", flush=True)

    sem = asyncio.Semaphore(args.concurrency)
    counts = dict.fromkeys(sorted(LABELS), 0) | {"failed": 0}
    started = time.monotonic()
    finished = 0
    out = OUT_FILE.open("a", encoding="utf-8")

    async def run(chunk: list[dict[str, Any]]) -> None:
        nonlocal finished
        async with sem:
            try:
                verdicts = await _label_batch(chunk, movies)
            except Exception as exc:  # noqa: BLE001 - lô lỗi thì lần chạy sau làm lại
                print(f"  lô lỗi: {str(exc)[:160]}", flush=True)
                verdicts = [None] * len(chunk)
        for row, verdict in zip(chunk, verdicts, strict=True):
            if verdict is None:
                counts["failed"] += 1
                continue
            counts[verdict["label"]] += 1
            out.write(json.dumps({"id": row["id"], **verdict}, ensure_ascii=False) + "\n")
        out.flush()
        finished += len(chunk)
        if finished % 500 < len(chunk):
            rate = finished / (time.monotonic() - started)
            print(f"  {finished}/{len(todo)} · {rate:.1f} bài/giây · {counts}", flush=True)

    await asyncio.gather(*(run(todo[i : i + BATCH_SIZE]) for i in range(0, len(todo), BATCH_SIZE)))
    out.close()
    print(f"xong {finished} bài trong {(time.monotonic() - started) / 60:.1f} phút: {counts}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=8)
    asyncio.run(main(parser.parse_args()))
