"""Export a sentiment holdout set for PhoBERT evaluation.

Modes:
  d1 (default) — labeled D1 comments (movie title in post, quality filters)
                 whose text is NOT already in sentiment.csv.
  split        — recreate train.py's 10% eval split (seed=42) from
                 sentiment.csv. Use this when D1 has no leftovers because
                 the train set already consumed every qualifying comment.

Usage (from cinemark-api, with .env DB_MODE=remote):

  .venv/bin/python scripts/export_sentiment_holdout.py --mode d1 --limit 500

For the usual case (train already ate all D1 comments), prefer from
phobert-classifier:

  .venv/bin/python export_holdout_split.py
  .venv/bin/python eval_holdout.py
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services import d1  # noqa: E402
from app.services.d1 import post_mentions_movie  # noqa: E402

WORKSPACE = ROOT.parent
TRAIN_CSV = WORKSPACE / "phobert-classifier" / "data" / "sentiment.csv"
OUT_CSV = WORKSPACE / "phobert-classifier" / "data" / "holdout.csv"

EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF"
    "\U0001F1E0-\U0001F1FF"
    "\U0000FE00-\U0000FE0F"
    "\U0000200D"
    "]+",
    flags=re.UNICODE,
)
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
MENTION_RE = re.compile(r"@[\w.]+")
HASHTAG_RE = re.compile(r"#\w+", re.UNICODE)
WS_RE = re.compile(r"\s+")


def substantive_text(s: str) -> str:
    s = URL_RE.sub(" ", s)
    s = MENTION_RE.sub(" ", s)
    s = HASHTAG_RE.sub(lambda m: " " + m.group(0)[1:], s)
    s = EMOJI_RE.sub(" ", s)
    chars: list[str] = []
    for ch in s:
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("N"):
            chars.append(ch)
        elif ch.isspace():
            chars.append(" ")
    return WS_RE.sub(" ", "".join(chars)).strip()


def is_usable_comment(text: str) -> bool:
    text = text.strip()
    if not text:
        return False
    sub = substantive_text(text)
    return bool(sub) and len(sub) >= 12 and len(text) >= 15


def load_train_texts(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            t = (row.get("text") or "").strip().casefold()
            if t:
                out.add(t)
    return out


async def fetch_labeled_rows() -> list[dict]:
    rows: list[dict] = []
    last_id = ""
    while True:
        chunk = await d1.d1_query(
            """
            SELECT c.id AS comment_id, c.message, c.sentiment, c.platform,
                   p.content AS post_content, m.title AS movie_title, m.enabled AS movie_enabled
            FROM comments c
            JOIN posts p ON p.id = c.post_id
            JOIN movies m ON m.id = p.movie_id
            WHERE c.sentiment IS NOT NULL
              AND c.message IS NOT NULL
              AND LENGTH(TRIM(c.message)) >= 10
              AND c.id > ?
            ORDER BY c.id
            LIMIT 1000
            """,
            [last_id],
            timeout=120.0,
        )
        if not chunk:
            break
        rows.extend(chunk)
        last_id = chunk[-1]["comment_id"]
        print(f"  loaded {len(rows)}")
    return rows


def export_from_train_split(train_csv: Path, out: Path, seed: int = 42, test_size: float = 0.1) -> list[dict]:
    """Recreate the eval split train.py used (datasets.train_test_split seed)."""
    from datasets import Dataset

    rows: list[dict] = []
    with train_csv.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            text = (row.get("text") or "").strip()
            label = (row.get("label") or "").strip()
            if text and label in {"positive", "negative", "neutral"}:
                rows.append({"text": text, "label": label})
    split = Dataset.from_list(rows).train_test_split(test_size=test_size, seed=seed)
    holdout = [
        {
            "text": r["text"],
            "label": r["label"],
            "platform": "train_split",
            "movie_title": "",
            "comment_id": "",
        }
        for r in split["test"]
    ]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["text", "label", "platform", "movie_title", "comment_id"],
        )
        w.writeheader()
        w.writerows(holdout)
    return holdout


async def export_from_d1(train_csv: Path, out: Path, limit: int) -> list[dict]:
    train_texts = load_train_texts(train_csv)
    print(f"Train texts to exclude: {len(train_texts)}")

    print("Fetching labeled comments from D1...")
    raw = await fetch_labeled_rows()
    print(f"Labeled rows: {len(raw)}")

    holdout: list[dict] = []
    skipped = Counter()
    for r in raw:
        text = (r.get("message") or "").strip()
        label = (r.get("sentiment") or "").strip()
        if label not in {"positive", "negative", "neutral"}:
            skipped["bad_label"] += 1
            continue
        if not r.get("movie_enabled"):
            skipped["movie_disabled"] += 1
            continue
        if not post_mentions_movie(r.get("post_content"), r.get("movie_title")):
            skipped["no_title_mention"] += 1
            continue
        if not is_usable_comment(text):
            skipped["too_short_or_emoji"] += 1
            continue
        if text.casefold() in train_texts:
            skipped["in_train"] += 1
            continue
        holdout.append(
            {
                "text": text,
                "label": label,
                "platform": r.get("platform") or "",
                "movie_title": r.get("movie_title") or "",
                "comment_id": r.get("comment_id") or "",
            }
        )

    holdout.sort(key=lambda x: x["comment_id"])
    if limit and len(holdout) > limit:
        by_label: dict[str, list[dict]] = {"negative": [], "neutral": [], "positive": []}
        for row in holdout:
            by_label[row["label"]].append(row)
        per = max(1, limit // 3)
        picked: list[dict] = []
        for lab in ("negative", "neutral", "positive"):
            picked.extend(by_label[lab][:per])
        picked_ids = {p["comment_id"] for p in picked}
        for row in holdout:
            if len(picked) >= limit:
                break
            if row["comment_id"] not in picked_ids:
                picked.append(row)
        holdout = picked[:limit]

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["text", "label", "platform", "movie_title", "comment_id"],
        )
        w.writeheader()
        w.writerows(holdout)

    print(f"Skipped: {dict(skipped)}")
    return holdout


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("d1", "split"),
        default="split",
        help="split=recreate train.py eval set (default); d1=leftover D1 comments",
    )
    parser.add_argument("--limit", type=int, default=500, help="Max rows for --mode d1")
    parser.add_argument("--out", type=Path, default=OUT_CSV)
    parser.add_argument("--train-csv", type=Path, default=TRAIN_CSV)
    args = parser.parse_args()

    if args.mode == "split":
        # datasets lives in phobert-classifier venv; prefer that interpreter,
        # but fall back to a pure-python shuffle if import fails.
        try:
            holdout = export_from_train_split(args.train_csv, args.out)
        except ImportError:
            import random

            rows: list[dict] = []
            with args.train_csv.open(encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    text = (row.get("text") or "").strip()
                    label = (row.get("label") or "").strip()
                    if text and label in {"positive", "negative", "neutral"}:
                        rows.append(
                            {
                                "text": text,
                                "label": label,
                                "platform": "train_split",
                                "movie_title": "",
                                "comment_id": "",
                            }
                        )
            rng = random.Random(42)
            rng.shuffle(rows)
            n_test = max(1, int(len(rows) * 0.1))
            holdout = rows[:n_test]
            args.out.parent.mkdir(parents=True, exist_ok=True)
            with args.out.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["text", "label", "platform", "movie_title", "comment_id"],
                )
                w.writeheader()
                w.writerows(holdout)
            print("Note: used random.Random(42) fallback (datasets not installed here)")
    else:
        holdout = await export_from_d1(args.train_csv, args.out, args.limit)

    print(f"Holdout: {len(holdout)}  {dict(Counter(r['label'] for r in holdout))}")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
