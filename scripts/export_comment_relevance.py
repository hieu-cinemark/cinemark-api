"""Export a comment-relevance training set for PhoBERT.

Comments have no relevance label of their own (only sentiment) - this
weak-labels each comment from its *parent post's* already-computed
`keyword_match` (substring match or Kira, see app/repositories/d1/posts.py's
persist_post) instead of spending a fresh Kira call per comment: a comment
under a post already judged related to the movie is labeled "related", one
under a post judged unrelated is "not_related". Same text shape as
phobert-classifier's serve.py _relevance_text() (movie/keyword context line
+ the text itself) so train/serve match.

This is a proxy signal, not a human-verified comment-level label - a
specific comment can still be off-topic banter under an on-topic post, or
vice versa. Good enough to bootstrap a first model; re-evaluate against a
small hand-checked sample before trusting it in production.

Usage (from cinemark-api, DB_MODE=local by default via .env):

  .venv/bin/python -m scripts.export_comment_relevance
  .venv/bin/python -m scripts.export_comment_relevance --limit 5000
"""

from __future__ import annotations

import argparse
import asyncio
import csv
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
OUT_CSV = WORKSPACE / "phobert-classifier" / "data" / "comment_relevance.csv"

MIN_MESSAGE_LENGTH = 10


def _relevance_text(message: str, movie_title: str | None, keyword: str | None) -> str:
    """Mirrors phobert-classifier/serve.py's _relevance_text() exactly -
    train/serve must build the same string shape from the same fields."""
    title = (movie_title or "").strip()
    kw = (keyword or "").strip()
    bits = []
    if title:
        bits.append(f"Phim: {title}")
    if kw and kw.casefold() != title.casefold():
        bits.append(f"Keyword: {kw}")
    prefix = " | ".join(bits)
    return f"{prefix}\n{message}" if prefix else message


async def fetch_rows() -> list[dict]:
    from app.services import d1

    rows: list[dict] = []
    last_id = ""
    while True:
        chunk = await d1.d1_query(
            """
            SELECT c.id AS comment_id, c.message, c.platform,
                   p.keyword_match, m.title AS movie_title, k.keyword
            FROM comments c
            JOIN posts p ON p.id = c.post_id
            LEFT JOIN movies m ON m.id = p.movie_id
            LEFT JOIN keywords k ON k.id = p.keyword_id
            WHERE c.message IS NOT NULL
              AND LENGTH(TRIM(c.message)) >= ?
              AND c.id > ?
            ORDER BY c.id
            LIMIT 1000
            """,
            [MIN_MESSAGE_LENGTH, last_id],
            timeout=120.0,
        )
        if not chunk:
            break
        rows.extend(chunk)
        last_id = chunk[-1]["comment_id"]
        print(f"  loaded {len(rows)}")
    return rows


async def main(limit: int | None) -> None:
    print("Fetching comments joined to their parent post's keyword_match...")
    raw = await fetch_rows()
    print(f"Candidate rows: {len(raw)}")

    out_rows: list[dict] = []
    skipped = Counter()
    for r in raw:
        message = (r.get("message") or "").strip()
        if not message:
            skipped["empty_message"] += 1
            continue
        keyword_match = r.get("keyword_match")
        if keyword_match is None:
            skipped["no_parent_post"] += 1
            continue
        label = "related" if int(keyword_match) == 1 else "not_related"
        out_rows.append(
            {
                "text": _relevance_text(message, r.get("movie_title"), r.get("keyword")),
                "label": label,
                "platform": r.get("platform") or "",
                "movie_title": r.get("movie_title") or "",
                "keyword": r.get("keyword") or "",
                "comment_id": r.get("comment_id") or "",
            }
        )

    if limit and len(out_rows) > limit:
        # Keep the class ratio instead of just truncating - a plain head()
        # would skew toward whichever label happens to sort first by id.
        by_label: dict[str, list[dict]] = {"related": [], "not_related": []}
        for row in out_rows:
            by_label[row["label"]].append(row)
        ratio = limit / len(out_rows)
        out_rows = by_label["related"][: int(len(by_label["related"]) * ratio)] + by_label["not_related"][
            : int(len(by_label["not_related"]) * ratio)
        ]

    out_rows.sort(key=lambda x: x["comment_id"])
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["text", "label", "platform", "movie_title", "keyword", "comment_id"])
        w.writeheader()
        w.writerows(out_rows)

    print(f"Skipped: {dict(skipped)}")
    print(f"Wrote {len(out_rows)} rows -> {OUT_CSV}  {dict(Counter(r['label'] for r in out_rows))}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="Cap total rows, keeping the class ratio")
    args = parser.parse_args()
    asyncio.run(main(args.limit))
