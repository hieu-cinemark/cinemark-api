"""Re-labels posts.relevance_label / relevance_confidence directly on
remote D1 using Kira (app/kira/prompt.py's SYSTEM_PROMPT/DATA_PROMPT pair -
the semantic classifier this product used before switching to local
PhoBERT, see app/services/relevance_phobert.py's own docstring) - intended
to produce a cleaner training set for phobert-classifier's next retrain,
not just a one-off relabel. See scripts/export_relevance_labels_for_training.py
(sibling repo phobert-classifier) for turning the result into a CSV
train_relevance.py can read.

WHY THIS EXISTS: the current relevance_label column is a mix of an older
keyword-substring heuristic and a PhoBERT model that's known to still get
title-collision cases wrong (a movie whose title is also ordinary
vocabulary, e.g. "Huyết Thống" - see movie_hashtag_present's own
docstring). Kira's prompt already has a TITLE-COLLISION RULE built in
specifically for this, plus semantic tolerance (synonyms/slang/abbreviated
mentions) the old substring check never had - re-labeling with it should
be a real quality upgrade for whatever a retrained PhoBERT then learns
from.

SCALE WARNING - read before running unscoped: this database has
~150,000 posts. Each Kira call costs real tokens (see class-instance
"active_report_provider" work this session for why Kira previously sat
idle) - a rough budget of 10M tokens covers on the order of a few
thousand calls, not all 150k. This script does NOT relabel everything by
default for that reason - see --limit/--movie-id below. Recommended: run
a small --limit first, check actual usage in the logs (ai_call_finished's
total_tokens field) or this script's own running estimate, then decide
how far to scale before committing to a large run.

Resumable: paginates ORDER BY p.id (not OFFSET, which would skip/repeat
rows if run concurrently with anything else writing posts) - pass
--after-id <last id printed before it stopped> to pick back up. Every
batch is committed to D1 immediately, so nothing already labeled is lost
if this is killed.

Usage:
  # Pilot batch - do this first.
  .venv/bin/python -m scripts.label_posts_relevance_kira --limit 500

  # Scoped to one movie (e.g. re-checking a known problem title).
  .venv/bin/python -m scripts.label_posts_relevance_kira --movie-id movie_xxx

  # Resume an interrupted run.
  .venv/bin/python -m scripts.label_posts_relevance_kira --after-id post_xxx --limit 5000

  # See what it would do without writing anything or spending tokens.
  .venv/bin/python -m scripts.label_posts_relevance_kira --limit 5 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.logging import get_logger
from app.kira.client import call_kira, parse_json_response
from app.kira.prompt import DATA_PROMPT, SYSTEM_PROMPT
from app.services.d1 import d1_query

logger = get_logger(__name__)

PAGE_SIZE = 200
# Kira calls run concurrently up to this many in flight from this script's
# own side - app.ai_client's own per-provider semaphore (asyncio.Semaphore
# (2), see call_ai) is the real ceiling on actual simultaneous requests to
# Kira, so this just needs to be >= that to keep the pipe full without
# adding a second, redundant limiter.
CONCURRENT_CALLS = 6
# Same D1 bound-parameter ceiling reasoning as scripts/push_relevance_labels.py's
# own ROWS_PER_BATCH comment.
ROWS_PER_D1_BATCH = 10
# Reasoning model - spends reasoning_content tokens before ever writing the
# JSON answer (see the now-deleted app/kira/relevance.py's own comment,
# confirmed live 2026-09: 500 intermittently truncated mid-thought for this
# exact prompt, 1500 didn't). Kept generous rather than re-tuning per model.
MAX_TOKENS = 1500

# Kira's classification -> this column's existing 3-value scheme.
_LABEL_MAP = {"relevant": "related", "irrelevant": "not_related", "uncertain": "uncertain"}

CSV_OUTPUT_PATH = Path("scripts/_kira_relevance_labels.csv")


def _field(value: Any) -> str:
    return str(value) if value else "N/A"


def _movie_context(row: dict[str, Any]) -> str:
    facts = []
    if row.get("movie_title"):
        facts.append(f"Movie title: {row['movie_title']}")
    if row.get("movie_director"):
        facts.append(f"Director: {row['movie_director']}")
    if row.get("movie_cast"):
        facts.append(f"Cast: {row['movie_cast']}")
    if row.get("movie_distributor"):
        facts.append(f"Distributor: {row['movie_distributor']}")
    return "\n".join(facts) if facts else "N/A"


async def _classify_one(row: dict[str, Any]) -> dict[str, Any] | None:
    keyword = row.get("keyword") or row.get("movie_title") or ""
    prompt = DATA_PROMPT.format(
        keyword=keyword,
        movie_context=_movie_context(row),
        title=_field(row.get("movie_title")),
        description="N/A",
        content=row.get("content") or "N/A",
        caption="N/A",
        hashtags="N/A",
        comments="N/A",
        metadata="N/A",
        image="N/A",
    )
    try:
        response = await call_kira(
            task="relevance",
            system_prompt=SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=MAX_TOKENS,
            # Operator-triggered batch job, same as import_parser.py's own
            # Settings-import calls - must not silently no-op just because
            # the ingest-classifiers enabled toggle happens to be off.
            force=True,
            platform=row.get("platform"),
        )
        parsed = parse_json_response(response)
        classification = parsed.get("classification") if isinstance(parsed, dict) else None
        if classification not in _LABEL_MAP:
            raise ValueError(f"unexpected classification: {json.dumps(parsed)[:200]!r}")
        return parsed
    except Exception as exc:
        logger.warning("kira_relevance_label_failed", post_id=row["id"], error=str(exc))
        return None


async def _write_batch(rows_with_results: list[tuple[dict[str, Any], dict[str, Any]]], now: str) -> None:
    label_cases = " ".join("WHEN ? THEN ?" for _ in rows_with_results)
    conf_cases = " ".join("WHEN ? THEN ?" for _ in rows_with_results)
    time_cases = " ".join("WHEN ? THEN ?" for _ in rows_with_results)
    placeholders = ", ".join("?" for _ in rows_with_results)

    sql = f"""
        UPDATE posts SET
            relevance_label = CASE id {label_cases} END,
            relevance_confidence = CASE id {conf_cases} END,
            relevance_labeled_at = CASE id {time_cases} END
        WHERE id IN ({placeholders})
    """
    params: list[Any] = []
    for row, parsed in rows_with_results:
        params.extend([row["id"], _LABEL_MAP[parsed["classification"]]])
    for row, parsed in rows_with_results:
        params.extend([row["id"], float(parsed.get("score") or 0.0)])
    for row, _parsed in rows_with_results:
        params.extend([row["id"], now])
    params.extend(row["id"] for row, _parsed in rows_with_results)

    await d1_query(sql, params)


def _text_for_training(row: dict[str, Any]) -> str:
    """Same "Phim: X | Keyword: Y\\ncontent" shape serve.py's own
    _relevance_text builds at inference time - training text should match
    what the model is actually queried with."""
    bits = []
    if row.get("movie_title"):
        bits.append(f"Phim: {row['movie_title']}")
    keyword = row.get("keyword")
    if keyword and str(keyword).casefold() != str(row.get("movie_title") or "").casefold():
        bits.append(f"Keyword: {keyword}")
    prefix = " | ".join(bits)
    content = row.get("content") or ""
    return f"{prefix}\n{content}" if prefix else content


async def run(
    *, movie_id: str | None, after_id: str, limit: int | None, dry_run: bool, csv_writer: Any, csv_file: Any
) -> None:
    where = ["p.content IS NOT NULL", "p.content != ''", "p.id > ?"]
    params: list[Any] = [after_id]
    if movie_id:
        where.append("p.movie_id = ?")
        params.append(movie_id)
    where_sql = " AND ".join(where)

    counts: Counter[str] = Counter()
    total_processed = 0
    last_id = after_id
    approx_tokens = 0

    while limit is None or total_processed < limit:
        page_limit = PAGE_SIZE if limit is None else min(PAGE_SIZE, limit - total_processed)
        rows = await d1_query(
            f"""
            SELECT p.id, p.platform, p.content, p.movie_id, k.keyword,
                   m.title AS movie_title, m.director AS movie_director,
                   m.`cast` AS movie_cast, m.distributor AS movie_distributor
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            WHERE {where_sql}
            ORDER BY p.id
            LIMIT ?
            """,
            [*params, page_limit],
            timeout=30.0,
        )
        if not rows:
            break

        for i in range(0, len(rows), CONCURRENT_CALLS):
            chunk = rows[i : i + CONCURRENT_CALLS]
            if dry_run:
                for row in chunk:
                    print(f"[dry-run] would classify {row['id']} (movie={row.get('movie_title')!r})")
                total_processed += len(chunk)
                continue

            results = await asyncio.gather(*(_classify_one(row) for row in chunk))
            to_write: list[tuple[dict[str, Any], dict[str, Any]]] = []
            for row, parsed in zip(chunk, results):
                total_processed += 1
                approx_tokens += (len(SYSTEM_PROMPT) + len(row.get("content") or "")) // 4 + 400
                if parsed is None:
                    counts["error"] += 1
                    continue
                label = _LABEL_MAP[parsed["classification"]]
                counts[label] += 1
                to_write.append((row, parsed))
                csv_writer.writerow([row["id"], _text_for_training(row), label, parsed.get("score")])

            if to_write:
                now = datetime.now(tz=timezone.utc).isoformat()
                for j in range(0, len(to_write), ROWS_PER_D1_BATCH):
                    await _write_batch(to_write[j : j + ROWS_PER_D1_BATCH], now)
                csv_file.flush()

        last_id = rows[-1]["id"]
        print(
            f"processed={total_processed} last_id={last_id} "
            f"related={counts['related']} not_related={counts['not_related']} "
            f"uncertain={counts['uncertain']} errors={counts['error']} "
            f"~tokens={approx_tokens:,} (resume with --after-id {last_id})",
            flush=True,
        )

        if len(rows) < page_limit:
            break

    print(f"\ndone - total processed: {total_processed}, breakdown: {dict(counts)}")
    if not dry_run:
        print(f"training rows appended to: {CSV_OUTPUT_PATH}")
    print(f"resume point if needed: --after-id {last_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", help="Only label posts for this movie_id")
    parser.add_argument("--after-id", default="", help="Resume after this post id (ORDER BY id cursor)")
    parser.add_argument("--limit", type=int, default=None, help="Stop after labeling this many posts")
    parser.add_argument("--dry-run", action="store_true", help="List what would be classified, spend no tokens")
    args = parser.parse_args()

    # Opened here (sync context), not inside run() - keeps blocking file I/O
    # out of the async function (ASYNC230) without needing asyncio.to_thread
    # for what's just one open() and a few flush() calls.
    _csv_file = None
    _csv_writer = None
    if not args.dry_run:
        CSV_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        _is_new = not CSV_OUTPUT_PATH.exists()
        _csv_file = CSV_OUTPUT_PATH.open("a", encoding="utf-8", newline="")
        _csv_writer = csv.writer(_csv_file)
        if _is_new:
            _csv_writer.writerow(["post_id", "text", "label", "score"])

    try:
        asyncio.run(
            run(
                movie_id=args.movie_id,
                after_id=args.after_id,
                limit=args.limit,
                dry_run=args.dry_run,
                csv_writer=_csv_writer,
                csv_file=_csv_file,
            )
        )
    finally:
        if _csv_file:
            _csv_file.close()
