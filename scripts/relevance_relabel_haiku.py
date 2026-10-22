"""Gán lại nhãn độ liên quan cho các bài AI từng để "uncertain" trong dữ liệu huấn luyện model nhà trồng
(data/visobert/relevance.jsonl, tải bởi scripts/visobert_relevance_finetune.py) - bắt AI chọn hẳn related / not_related,
độ do dự nằm ở điểm số (2026-10-22). Hai lần fine-tune trước kẹt đúng ở nhóm bài này: không có nhãn rõ thì model không
học được ca khó.

Dùng nhà cung cấp AI đang chọn (chat, cùng prompt post_relevance với lúc chạy thật), lô 10 bài, nhiều lô song song.
Kết quả ghi nối vào data/visobert/relevance_relabel.jsonl - chạy lại thì bỏ qua bài đã có, dừng giữa chừng không mất gì.

Cách dùng:
    python -m scripts.relevance_relabel_haiku                      # mọi bài "uncertain"
    python -m scripts.relevance_relabel_haiku --limit 200 --concurrency 4
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.services.d1 import d1_query

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "data" / "visobert" / "relevance.jsonl"
OUT_FILE = ROOT / "data" / "visobert" / "relevance_relabel.jsonl"
BATCH_SIZE = 10
_BINARY_RULE = (
    '\n\nFOR THIS PASS: every post MUST be classified as "relevant" or "irrelevant" - never "uncertain". '
    "Pick the more likely one and express your doubt through the score (score = probability the post is about its "
    "target film; near 0.5 when unsure)."
)


async def _load_movies(ids: list[str]) -> dict[str, dict[str, Any]]:
    rows = await d1_query(f"SELECT * FROM movies WHERE id IN ({','.join('?' * len(ids))})", ids, timeout=60.0)
    if rows is None:
        raise RuntimeError("movies_download_failed")
    return {row["id"]: row for row in rows}


async def _label_batch(rows: list[dict[str, Any]], movies: dict[str, dict[str, Any]], titles: list[str]):
    from app.ai.prompts.post_relevance import POST_RELEVANCE_SYSTEM_PROMPT
    from app.ai.runtime import call_llm, parse_json_response
    from app.ai.tasks.post_relevance import TASK, _Job, _verdict, batch_prompt, max_tokens_for

    loop = asyncio.get_running_loop()
    jobs = []
    for row in rows:
        movie = movies.get(row["movie_id"]) or {"title": row["film"].split(" | ")[0]}
        keyword = row["film"].split(" | ")[1] if " | " in row["film"] else None
        others = [t for t in titles if t != movie.get("title")]
        jobs.append(_Job(row["text"], movie, keyword, None, others, loop.create_future()))
    response = await call_llm(
        task=TASK,
        system_prompt=POST_RELEVANCE_SYSTEM_PROMPT,
        user_prompt=batch_prompt(jobs) + _BINARY_RULE,
        max_tokens=max_tokens_for(len(jobs)),
    )
    parsed = parse_json_response(response)
    results = parsed.get("results") if isinstance(parsed, dict) else None
    out: list[dict[str, Any] | None] = [None] * len(rows)
    for entry in results if isinstance(results, list) else []:
        index = entry.get("i") if isinstance(entry, dict) else None
        if isinstance(index, int) and 1 <= index <= len(rows) and (verdict := _verdict(entry)) is not None:
            out[index - 1] = verdict
    return out


async def main(args: argparse.Namespace) -> None:
    settings.ai_provider_concurrency = args.concurrency  # semaphore của app/ai/client.py đọc lúc gọi đầu tiên
    rows = [json.loads(line) for line in DATA_FILE.read_text(encoding="utf-8").split("\n") if line]
    done = set()
    if OUT_FILE.exists():
        done = {json.loads(line)["id"] for line in OUT_FILE.read_text(encoding="utf-8").split("\n") if line}
    todo = [r for r in rows if r["label"] == "uncertain" and r["id"] not in done]
    if args.limit:
        todo = todo[: args.limit]
    movies = await _load_movies(sorted({r["movie_id"] for r in rows}))
    titles = sorted({m["title"] for m in movies.values() if m.get("title")})
    print(f"{len(todo)} bài cần gán lại ({len(done)} đã có) · {len(movies)} phim", flush=True)

    sem = asyncio.Semaphore(args.concurrency)
    counts = {"related": 0, "not_related": 0, "uncertain": 0, "failed": 0}
    started = time.monotonic()
    finished = 0
    out = OUT_FILE.open("a", encoding="utf-8")

    async def run(chunk: list[dict[str, Any]]) -> None:
        nonlocal finished
        async with sem:
            try:
                verdicts = await _label_batch(chunk, movies, titles)
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
