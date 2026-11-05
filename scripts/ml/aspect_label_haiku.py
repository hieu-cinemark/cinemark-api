"""Gán lại nhãn khía cạnh của comment bằng Haiku (2026-11-04), làm dữ liệu train lại visobert-aspects. Nhãn cũ chỉ đúng ~68-72%
(chấm tay 26/10) và đó là trần của model học từ chúng; lỗi hay gặp là gán mục cho comment không nói gì về mục đó.

Lấy từ ~/cinemark-ml/aspects/comments.jsonl (scripts/tools/aspect_probe.py), xen kẽ comment từng có nhãn và chưa có nhãn nên
--limit vẫn ra mẫu cân đối. Nhãn ghi nối vào haiku_labels.jsonl - chạy lại thì bỏ qua comment đã có. Không ghi gì vào DB.

    python -m scripts.ml.aspect_label_haiku --limit 200 --concurrency 4
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from collections import Counter
from itertools import chain, zip_longest
from typing import Any

from app.ai.aspects import aspect_prompt_lines, parse_aspects
from app.core.config import settings
from app.repositories.d1.store import d1_query
from app.services.topics.text import normalize
from scripts.tools.aspect_probe import DATA_DIR, SEED

IN_FILE = DATA_DIR / "comments.jsonl"
OUT_FILE = DATA_DIR / "haiku_labels.jsonl"
TASK = "comment_aspect_label"
BATCH_SIZE = 10
MIN_CHARS = 10  # cùng ngưỡng với scripts/ml/aspect_finetune.py
POST_CHARS = 200
QUERY_CHUNK = 90

SYSTEM_PROMPT = f"""You label Vietnamese social media comments about a film for an analytics product. For each comment, list which parts of the film the comment itself talks about, and whether it praises (+) or criticizes (-) that part.

Parts:
{aspect_prompt_lines()}

Rules:
- Label only what the comment's own words say. The film title and post are context for understanding the comment, not evidence for a label.
- Most comments have no label. Use an empty list for tags, greetings, jokes between friends, questions, off-topic chatter, stickers and emoji alone, or plain reactions with no named part or stated feeling.
- General praise or criticism of the film ("phim hay", "phim dở") is an empty list unless it names a part.
- Naming an actor is "dien_vien" only when it says something about the casting or the actor's looks, fame or fit. Comments on how they perform are "dien_xuat".
- "cam_xuc" covers any stated feeling while watching, even with no part or film named: "sợ quá", "thương quá", "khóc luôn", "cười xỉu". Use "+" when the feeling is what the film aims for, including fear and sadness, and "-" only when the comment complains about it, such as boring or not funny.
- Choose the polarity the comment expresses. If a part is mentioned with no praise or criticism, leave it out.
- Never invent a key.

Reply with JSON only: {{"results":[{{"i":1,"aspects":["dien_xuat:+"],"score":0.0}}]}}. score is your confidence from 0 to 1. Include every comment."""


def _load_rows() -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in IN_FILE.open(encoding="utf-8")]
    rows = [r for r in rows if len(normalize(r["text"])) >= MIN_CHARS]
    rng = random.Random(SEED)
    labelled = [r for r in rows if r["labels"]]
    unlabelled = [r for r in rows if not r["labels"]]
    rng.shuffle(labelled)
    rng.shuffle(unlabelled)
    return [r for r in chain.from_iterable(zip_longest(labelled, unlabelled)) if r is not None]


async def _load_context(post_ids: list[str]) -> dict[str, dict[str, Any]]:
    context: dict[str, dict[str, Any]] = {}
    for i in range(0, len(post_ids), QUERY_CHUNK):
        chunk = post_ids[i : i + QUERY_CHUNK]
        found = await d1_query(
            f"SELECT p.id, m.title, substr(p.content, 1, {POST_CHARS}) AS content FROM posts p "
            f"JOIN movies m ON m.id = p.movie_id WHERE p.id IN ({','.join('?' * len(chunk))})",
            chunk,
            timeout=60.0,
        )
        if found is None:
            raise RuntimeError("post_context_download_failed")
        context.update({r["id"]: r for r in found})
    return context


def _batch_prompt(rows: list[dict[str, Any]], context: dict[str, dict[str, Any]]) -> str:
    blocks = []
    for index, row in enumerate(rows, 1):
        post = context.get(row["post_id"]) or {}
        post_text = " ".join((post.get("content") or "").split())
        blocks.append(
            f"[{index}] Film: {post.get('title') or 'unknown'}\nPost: {post_text or 'unknown'}\nComment: {row['text'][:500]}"
        )
    return "\n\n".join(blocks)


async def _label_batch(rows: list[dict[str, Any]], context: dict[str, dict[str, Any]]) -> list[dict[str, Any] | None]:
    from app.ai.runtime import call_llm, parse_json_response

    # force: công tắc AI đang tắt là để chặn bộ phân loại lúc ingest, còn đây là việc người vận hành tự chạy.
    response = await call_llm(
        task=TASK,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=_batch_prompt(rows, context),
        max_tokens=2000 + 200 * len(rows),
        force=True,
    )
    parsed = parse_json_response(response)
    results = parsed.get("results") if isinstance(parsed, dict) else None
    out: list[dict[str, Any] | None] = [None] * len(rows)
    for entry in results if isinstance(results, list) else []:
        index = entry.get("i") if isinstance(entry, dict) else None
        if isinstance(index, int) and 1 <= index <= len(rows):
            out[index - 1] = {"aspects": parse_aspects(entry.get("aspects")), "score": entry.get("score")}
    return out


async def main(args: argparse.Namespace) -> None:
    settings.ai_provider_concurrency = args.concurrency  # semaphore của app/ai/client.py đọc lúc gọi đầu tiên
    rows = _load_rows()
    done = set()
    if OUT_FILE.exists():
        done = {json.loads(line)["id"] for line in OUT_FILE.open(encoding="utf-8")}
    todo = [r for r in rows if r["id"] not in done]
    if args.limit:
        todo = todo[: args.limit]
    context = await _load_context(sorted({r["post_id"] for r in todo}))
    print(f"{len(todo)} comment cần gán ({len(done)} đã có) · thấy {len(context)} bài gốc", flush=True)

    sem = asyncio.Semaphore(args.concurrency)
    counts: Counter[str] = Counter()
    started = time.monotonic()
    out = OUT_FILE.open("a", encoding="utf-8")

    async def run(chunk: list[dict[str, Any]]) -> None:
        async with sem:
            try:
                verdicts = await _label_batch(chunk, context)
            except Exception as exc:  # noqa: BLE001 - lô lỗi thì lần chạy sau làm lại
                print(f"  lô lỗi: {str(exc)[:160]}", flush=True)
                verdicts = [None] * len(chunk)
        for row, verdict in zip(chunk, verdicts, strict=True):
            if verdict is None:
                counts["failed"] += 1
                continue
            counts["labelled" if verdict["aspects"] else "empty"] += 1
            out.write(json.dumps({"id": row["id"], **verdict}, ensure_ascii=False) + "\n")
        out.flush()

    await asyncio.gather(*(run(todo[i : i + BATCH_SIZE]) for i in range(0, len(todo), BATCH_SIZE)))
    out.close()
    print(f"xong trong {(time.monotonic() - started) / 60:.1f} phút: {dict(counts)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=8)
    asyncio.run(main(parser.parse_args()))
