"""Thử gom cụm bình luận của một phim từ embedding đã tính (scripts/jobs/embed_comments.py), chưa đặt tên cụm (2026-11-05).

Bỏ bình luận ở bài hưởng ứng trend. Chỉ đọc D1 và ghi file: data/clusters/<movie_id>.json (cụm, số lượng, câu mẫu) và .npz (tâm cụm).

    python -m scripts.tools.cluster_film_comments --movie-id <id> --k 30
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

from app.repositories.d1.store import d1_query
from app.services.topics.clustering import kmeans
from scripts.jobs.embed_comments import DB_PATH

OUT_DIR = Path(__file__).resolve().parents[2] / "data" / "clusters"
QUERY_CHUNK = 90
CLOSEST = 12
RANDOM_SAMPLES = 8


def load_vectors(movie_id: str) -> tuple[list[str], np.ndarray]:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    rows = conn.execute("SELECT comment_id, embedding FROM comment_embeddings WHERE movie_id = ?", (movie_id,)).fetchall()
    ids = [row[0] for row in rows]
    x = np.frombuffer(b"".join(row[1] for row in rows), dtype=np.float16).reshape(len(rows), -1).astype(np.float32)
    return ids, x / np.linalg.norm(x, axis=1, keepdims=True).clip(1e-6)


async def trend_comment_ids(movie_id: str) -> set[str]:
    posts = await d1_query("SELECT id FROM posts WHERE movie_id = ? AND trend_post = 1", [movie_id], timeout=60.0, quiet=True)
    if posts is None:
        raise RuntimeError("trend_post_read_failed")
    post_ids = [row["id"] for row in posts]
    found: set[str] = set()
    for i in range(0, len(post_ids), QUERY_CHUNK):
        chunk = post_ids[i : i + QUERY_CHUNK]
        rows = await d1_query(
            f"SELECT id FROM comments WHERE post_id IN ({','.join('?' * len(chunk))})", chunk, timeout=60.0, quiet=True
        )
        if rows is None:
            raise RuntimeError("trend_comment_read_failed")
        found.update(row["id"] for row in rows)
    return found


async def comment_details(ids: list[str]) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {}
    for i in range(0, len(ids), QUERY_CHUNK):
        chunk = ids[i : i + QUERY_CHUNK]
        rows = await d1_query(
            "SELECT c.id, c.message, c.sentiment, c.reactions_count, par.message AS parent FROM comments c "
            "LEFT JOIN comments par ON par.platform = c.platform AND par.external_id = c.parent_external_id "
            f"WHERE c.id IN ({','.join('?' * len(chunk))})",
            chunk,
            timeout=60.0,
            quiet=True,
        )
        details.update({row["id"]: row for row in rows or []})
    return details


async def main(args: argparse.Namespace) -> None:
    ids, x = load_vectors(args.movie_id)
    trend = await trend_comment_ids(args.movie_id)
    keep = np.array([cid not in trend for cid in ids])
    print(f"{len(ids)} vector, bỏ {int((~keep).sum())} ở bài trend, còn {int(keep.sum())}", flush=True)
    ids = [cid for cid, ok in zip(ids, keep, strict=True) if ok]
    x = x[keep]
    clusters = sorted(kmeans(x, args.k), key=lambda c: -len(c.indices))
    rng = random.Random(7)
    picked: list[tuple[int, list[str]]] = []
    for c in clusters:
        closest = c.indices[np.argsort(-(x[c.indices] @ c.centroid))[:CLOSEST]]
        others = rng.sample([int(i) for i in c.indices], min(RANDOM_SAMPLES, len(c.indices)))
        picked.append((c.id, [ids[int(i)] for i in closest] + [ids[i] for i in others]))
    details = await comment_details(sorted({cid for _, group in picked for cid in group}))
    output = []
    for c, (_, group) in zip(clusters, picked, strict=True):
        output.append(
            {
                "cluster": c.id,
                "comments": len(c.indices),
                "share_percent": round(len(c.indices) * 100 / len(ids), 1),
                "cohesion": round(float((x[c.indices] @ c.centroid).mean()), 3),
                "samples": [
                    {
                        "text": (details[cid].get("message") or "")[:160],
                        "parent": (details[cid].get("parent") or "")[:80] or None,
                        "likes": details[cid].get("reactions_count"),
                        "sentiment": details[cid].get("sentiment"),
                    }
                    for cid in group
                    if cid in details
                ],
            }
        )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{args.movie_id}.json").write_text(json.dumps(output, ensure_ascii=False, indent=1), encoding="utf-8")
    np.savez_compressed(OUT_DIR / f"{args.movie_id}.npz", centroids=np.stack([c.centroid for c in clusters]))
    print(f"đã ghi {OUT_DIR / f'{args.movie_id}.json'}: {len(output)} cụm")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--k", type=int, default=30)
    asyncio.run(main(parser.parse_args()))
