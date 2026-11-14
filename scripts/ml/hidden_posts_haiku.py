"""Gán nhãn lại bằng Haiku cho mọi bài đang ẩn trên D1 (14/11/2026): not_related, uncertain, và bài chưa có nhãn không
khớp từ khoá. Cùng prompt post_relevance với lúc chạy thật, nhưng bắt AI chọn hẳn related / not_related (độ do dự nằm ở
điểm số), lô 10 bài, nhiều lô song song.

Hai bước:
  label   đọc bài ẩn từ D1, gọi AI, ghi nối vào data/relevance_hidden/labels.jsonl - chạy lại thì bỏ qua bài đã có.
  apply   ghi nhãn vào D1: related (điểm >= --min-related) -> hiện lại trên dashboard, related điểm thấp -> uncertain, not_related -> lượt dọn bài không liên quan
          (scripts/tools/purge_irrelevant_posts.py, có sao lưu R2) sẽ xoá. Bài AI không trả lời được thì giữ nguyên.

    python -m scripts.ml.hidden_posts_haiku label --limit 50
    python -m scripts.ml.hidden_posts_haiku label --concurrency 8
    python -m scripts.ml.hidden_posts_haiku apply --dry-run
    python -m scripts.ml.hidden_posts_haiku apply
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import and_, func, or_, select, update

from app.core.config import settings
from app.repositories.d1.sql import d1_exec, json_values
from app.repositories.d1.tables import keywords as keywords_table
from app.repositories.d1.tables import movies as movies_table
from app.repositories.d1.tables import posts as posts_table

ROOT = Path(__file__).resolve().parents[2]
OUT_FILE = ROOT / "data" / "relevance_hidden" / "labels.jsonl"
BATCH_SIZE = 10
PAGE = 2000
CHUNK = 90  # id mỗi câu UPDATE (D1 tối đa 100 tham số)
_BINARY_RULE = (
    '\n\nFOR THIS PASS: every post MUST be classified as "relevant" or "irrelevant" - never "uncertain". '
    "Pick the more likely one and express your doubt through the score (score = probability the post is about its "
    "target film; near 0.5 when unsure)."
)


def _hidden(p: Any) -> Any:
    return or_(
        p.c.relevance_label.in_(("not_related", "uncertain")),
        and_(p.c.relevance_label.is_(None), p.c.keyword_match == 0),
    )


async def _load_movies() -> dict[str, dict[str, Any]]:
    rows = await d1_exec(
        select(
            movies_table.c.id,
            movies_table.c.title,
            movies_table.c.director,
            movies_table.c.cast,
            movies_table.c.distributor,
            movies_table.c.released_at,
            movies_table.c.description,
        ),
        timeout=60.0,
    )
    if rows is None:
        raise RuntimeError("movies_download_failed")
    return {row["id"]: row for row in rows if row.get("title")}


async def _load_hidden() -> list[dict[str, Any]]:
    """Bài ẩn có nội dung, theo id; phân trang theo id để mỗi câu đọc gọn trong giới hạn của D1."""
    p, k = posts_table.alias("p"), keywords_table.alias("k")
    out: list[dict[str, Any]] = []
    after = ""
    while True:
        rows = await d1_exec(
            select(
                p.c.id,
                p.c.movie_id,
                p.c.platform,
                p.c.relevance_label,
                func.substr(p.c.content, 1, 1500).label("content"),
                k.c.keyword,
            )
            .select_from(p.outerjoin(k, k.c.id == p.c.keyword_id))
            .where(_hidden(p), p.c.id > after)
            .order_by(p.c.id)
            .limit(PAGE),
            timeout=60.0,
        )
        if rows is None:
            raise RuntimeError("hidden_posts_download_failed")
        out.extend(rows)
        if len(rows) < PAGE:
            return out
        after = rows[-1]["id"]


async def _label_batch(rows: list[dict[str, Any]], movies: dict[str, dict[str, Any]], titles: list[str]):
    from app.ai.prompts.post_relevance import POST_RELEVANCE_SYSTEM_PROMPT
    from app.ai.runtime import call_llm, parse_json_response
    from app.ai.tasks.post_relevance import TASK, _Job, _verdict, batch_prompt, max_tokens_for

    loop = asyncio.get_running_loop()
    jobs = []
    for row in rows:
        movie = movies[row["movie_id"]]
        others = [t for t in titles if t != movie["title"]]
        jobs.append(_Job(row["content"], movie, row.get("keyword"), row.get("platform"), others, loop.create_future()))
    response = await call_llm(
        task=TASK,
        system_prompt=POST_RELEVANCE_SYSTEM_PROMPT,
        user_prompt=batch_prompt(jobs) + _BINARY_RULE,
        max_tokens=max_tokens_for(len(jobs)),
        force=True,  # job người vận hành chạy: không cần bật AI toàn hệ thống
    )
    parsed = parse_json_response(response)
    results = parsed.get("results") if isinstance(parsed, dict) else None
    out: list[dict[str, Any] | None] = [None] * len(rows)
    for entry in results if isinstance(results, list) else []:
        index = entry.get("i") if isinstance(entry, dict) else None
        if isinstance(index, int) and 1 <= index <= len(rows) and (verdict := _verdict(entry)) is not None:
            out[index - 1] = verdict
    return out


async def label(args: argparse.Namespace) -> None:
    settings.ai_provider_concurrency = args.concurrency  # semaphore của app/ai/providers/client.py đọc lúc gọi đầu tiên
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if OUT_FILE.exists():
        done = {json.loads(line)["id"] for line in OUT_FILE.read_text(encoding="utf-8").split("\n") if line}
    movies = await _load_movies()
    titles = sorted({m["title"] for m in movies.values()})
    rows = await _load_hidden()
    skipped = Counter()
    todo = []
    for row in rows:
        if row["id"] in done:
            continue
        if not (row.get("content") or "").strip():
            skipped["no_content"] += 1
        elif row.get("movie_id") not in movies:
            skipped["no_movie"] += 1
        else:
            todo.append(row)
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(rows)} bài ẩn · {len(todo)} cần gán ({len(done)} đã có) · bỏ qua {dict(skipped)}", flush=True)

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
            record = {"id": row["id"], "was": row["relevance_label"], "movie_id": row["movie_id"], **verdict}
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
        out.flush()
        finished += len(chunk)
        if finished % 500 < len(chunk):
            rate = finished / (time.monotonic() - started)
            print(f"  {finished}/{len(todo)} · {rate:.1f} bài/giây · {counts}", flush=True)

    await asyncio.gather(*(run(todo[i : i + BATCH_SIZE]) for i in range(0, len(todo), BATCH_SIZE)))
    out.close()
    print(f"xong {finished} bài trong {(time.monotonic() - started) / 60:.1f} phút: {counts}")


async def apply(args: argparse.Namespace) -> None:
    latest: dict[str, dict[str, Any]] = {}
    for line in OUT_FILE.read_text(encoding="utf-8").split("\n"):
        if line:
            record = json.loads(line)
            latest[record["id"]] = record
    by_label: dict[str, list[str]] = {"related": [], "uncertain": [], "not_related": []}
    for record in latest.values():
        name = record["label"]
        if name == "related" and record.get("confidence", 0) < args.min_related:
            name = "uncertain"  # AI nghiêng về liên quan nhưng gần 50/50: vẫn ẩn, không xoá
        if name in by_label:
            by_label[name].append(record["id"])
    print({label: len(ids) for label, ids in by_label.items()}, flush=True)
    if args.dry_run:
        return
    now = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    for label_name, ids in by_label.items():
        for start in range(0, len(ids), CHUNK):
            # Chỉ đổi bài vẫn còn ẩn - bài đã được hiện lại bằng tay hay luật trong lúc chạy thì giữ nguyên.
            await d1_exec(
                update(posts_table)
                .where(posts_table.c.id.in_(json_values(ids[start : start + CHUNK])), _hidden(posts_table))
                .values(relevance_label=label_name, relevance_labeled_at=now),
                timeout=60.0,
            )
            await asyncio.sleep(0.2)
        print(f"  ghi xong {label_name}: {len(ids)}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_label = sub.add_parser("label")
    p_label.add_argument("--limit", type=int, default=0)
    p_label.add_argument("--concurrency", type=int, default=8)
    p_apply = sub.add_parser("apply")
    p_apply.add_argument("--dry-run", action="store_true")
    p_apply.add_argument("--min-related", type=float, default=0.6)
    parsed = parser.parse_args()
    asyncio.run(label(parsed) if parsed.cmd == "label" else apply(parsed))
