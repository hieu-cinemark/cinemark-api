"""Applies app/services/relevance_rules.py to posts already in D1 - the
ingest consumer only applies them to new posts.

Scope: posts currently counted as relevant (keyword_match > 0, what every
dashboard query filters on). A post a rule rejects gets keyword_match = 0
and relevance_label = 'not_related' - nothing is deleted. Every change is
written first to a rollback CSV (id, previous keyword_match /
relevance_label / relevance_confidence, rule), so it can be undone with
--rollback <csv>.

Usage:
  .venv/bin/python -m scripts.apply_relevance_rules            # dry run: counts + samples
  .venv/bin/python -m scripts.apply_relevance_rules --apply
  .venv/bin/python -m scripts.apply_relevance_rules --rollback scripts/_relevance_rules_rollback_<ts>.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from app.core.logging import get_logger
from app.services.d1 import d1_query
from app.services.relevance_rules import foreign_language_reason, mentions_other_film

logger = get_logger(__name__)

PAGE_SIZE = 2000
# D1 caps bound parameters at 100 per statement; each row here uses 7
# (3 CASE pairs + the IN list), so 12 rows = 84.
BATCH = 12
QUERY_TIMEOUT = 60.0


async def _candidates(titles: list[str]) -> list[dict]:
    changes: list[dict] = []
    after = ""
    while True:
        rows = await d1_query(
            """
            SELECT p.id, p.keyword_match, p.relevance_label, p.relevance_confidence, p.content,
                   json_extract(p.raw_json, '$.text_language') AS text_language,
                   m.title AS movie_title, k.keyword AS keyword
            FROM posts p
            JOIN movies m ON m.id = p.movie_id
            LEFT JOIN keywords k ON k.id = p.keyword_id
            WHERE p.keyword_match > 0 AND p.id > ?
            ORDER BY p.id LIMIT ?
            """,
            [after, PAGE_SIZE],
            timeout=QUERY_TIMEOUT,
        )
        if rows is None:
            raise RuntimeError(f"D1 read failed after id {after!r} - candidate list would be incomplete")
        if not rows:
            return changes
        for row in rows:
            rule = foreign_language_reason(row["content"], row["text_language"])
            rule = f"non_vietnamese:{rule}" if rule else None
            if rule is None:
                other = mentions_other_film(row["content"], row["movie_title"], row["keyword"], titles)
                rule = f"other_film:{other}" if other else None
            if rule:
                changes.append({**row, "rule": rule})
        after = rows[-1]["id"]


async def _write(updates: list[tuple[str, int, str | None, float | None]]) -> None:
    """updates: (id, keyword_match, relevance_label, relevance_confidence)."""
    for start in range(0, len(updates), BATCH):
        chunk = updates[start : start + BATCH]
        km_cases = " ".join("WHEN ? THEN ?" for _ in chunk)
        rl_cases = " ".join("WHEN ? THEN ?" for _ in chunk)
        rc_cases = " ".join("WHEN ? THEN ?" for _ in chunk)
        params: list = []
        for pid, km, _rl, _rc in chunk:
            params += [pid, km]
        for pid, _km, rl, _rc in chunk:
            params += [pid, rl]
        for pid, _km, _rl, rc in chunk:
            params += [pid, rc]
        params += [u[0] for u in chunk]
        result = await d1_query(
            f"UPDATE posts SET keyword_match = CASE id {km_cases} END, relevance_label = CASE id {rl_cases} END, "
            f"relevance_confidence = CASE id {rc_cases} END WHERE id IN ({','.join('?' * len(chunk))})",
            params,
            timeout=QUERY_TIMEOUT,
        )
        # d1_query returns None (logged, not raised) on failure - stop rather
        # than report rows as updated that never were.
        if result is None:
            raise RuntimeError(f"D1 update failed for batch starting at {chunk[0][0]!r} ({start} rows written so far)")


async def run(apply: bool) -> None:
    titles = [r["title"] for r in await d1_query("SELECT title FROM movies") or [] if r.get("title")]
    changes = await _candidates(titles)
    by_rule = Counter(c["rule"].split(":")[0] + ":" + c["rule"].split(":")[1] for c in changes)
    logger.info("relevance_rules_candidates", total=len(changes), by_rule=dict(by_rule))
    random.seed(1)
    for c in random.sample(changes, min(10, len(changes))):
        logger.info("sample", rule=c["rule"], movie=c["movie_title"], content=(c["content"] or "")[:120])
    if not apply or not changes:
        return
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    rollback = Path(f"scripts/_relevance_rules_rollback_{stamp}.csv")
    with rollback.open("w", encoding="utf-8", newline="") as f:  # noqa: ASYNC230 - one small local file
        writer = csv.writer(f)
        writer.writerow(["id", "keyword_match", "relevance_label", "relevance_confidence", "rule"])
        for c in changes:
            writer.writerow(
                [c["id"], c["keyword_match"], c["relevance_label"] or "", c["relevance_confidence"] or "", c["rule"]]
            )
    await _write([(c["id"], 0, "not_related", 1.0) for c in changes])
    logger.info("relevance_rules_applied", updated=len(changes), rollback_file=str(rollback))


async def rollback(path: Path) -> None:
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    await _write(
        [
            (
                r["id"],
                int(r["keyword_match"]),
                r["relevance_label"] or None,
                float(r["relevance_confidence"]) if r["relevance_confidence"] else None,
            )
            for r in rows
        ]
    )
    logger.info("relevance_rules_rolled_back", restored=len(rows), file=str(path))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    parser.add_argument("--rollback", type=Path, help="restore the values saved in this rollback CSV")
    args = parser.parse_args()
    if args.rollback:
        asyncio.run(rollback(args.rollback))
    else:
        asyncio.run(run(args.apply))


if __name__ == "__main__":
    main()
