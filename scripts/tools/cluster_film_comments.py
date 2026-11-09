"""Thử gom cụm bình luận của một phim từ embedding đã tính (scripts/jobs/embed_comments.py), chưa đặt tên cụm (2026-11-05).

Chỉ gom bình luận gốc có nội dung (09/11/2026): bỏ bài hưởng ứng trend, phản hồi, câu chỉ có tag/icon và câu quá ngắn (dưới
MIN_WORDS chữ) - các câu đó làm cụm lẫn lộn hoặc thành cụm "đẹp quá"/"thương quá" vô nghĩa. Chỉ đọc D1 và ghi file:
data/clusters/<movie_id>.json (cụm, số lượng, câu mẫu), .npz (tâm cụm), .members.json (id comment từng cụm) và .details.json
(nội dung, like, cảm xúc của từng comment đã gom - cho bước tổng kết).

    python -m scripts.tools.cluster_film_comments --movie-id <id> --k 30
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

from app.ai.local.embedding import embed_texts
from app.ai.tasks.sentiment_rules import rule_sentiment, strip_placeholders
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.repositories.d1.store import d1_query
from app.services.topics.clustering import kmeans
from scripts.jobs.embed_comments import DB_PATH

OUT_DIR = Path(__file__).resolve().parents[2] / "data" / "clusters"
POST_CHUNK = 40
CLOSEST = 12
RANDOM_SAMPLES = 4
TOP_LIKED_SAMPLES = 4
MIN_WORDS = 4
# Facebook / Threads: thảo luận nằm nhiều ở chính bài đăng (review, cảm nhận dài), không chỉ ở bình luận - gom cả bài đăng
# (09/11/2026). TikTok bài đăng chủ yếu là caption video của kênh, bỏ.
POST_PLATFORMS = ("facebook", "threads")
POST_TEXT_LIMIT = 600


def load_vectors(movie_id: str) -> tuple[list[str], np.ndarray]:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT comment_id, embedding FROM comment_embeddings WHERE movie_id = ?", (movie_id,)
    ).fetchall()
    ids = [row[0] for row in rows]
    x = np.frombuffer(b"".join(row[1] for row in rows), dtype=np.float16).reshape(len(rows), -1).astype(np.float32)
    return ids, x / np.linalg.norm(x, axis=1, keepdims=True).clip(1e-6)


def has_content(message: str | None) -> bool:
    """Câu đủ nội dung để gom: không phải tag/icon/tiếng cười và có ít nhất MIN_WORDS chữ."""
    if rule_sentiment(message) is not None:
        return False
    words = [
        w
        for w in re.split(r"\s+", strip_placeholders(message))
        if any(ch.isalpha() for ch in w) and not w.startswith("@")
    ]
    return len(words) >= MIN_WORDS


async def comment_details(movie_id: str) -> dict[str, dict[str, Any]]:
    """Bình luận gốc của các bài liên quan, không phải bài trend, của phim - đọc theo từng nhóm bài qua index post_id."""
    posts = await d1_query(
        f"SELECT p.id FROM posts p WHERE p.movie_id = ? AND COALESCE(p.trend_post, 0) = 0 AND {RELEVANT_POST_SQL}",
        [movie_id],
        timeout=90.0,
        quiet=True,
    )
    if posts is None:
        raise RuntimeError("post_read_failed")
    post_ids = [row["id"] for row in posts]
    details: dict[str, dict[str, Any]] = {}
    for i in range(0, len(post_ids), POST_CHUNK):
        chunk = post_ids[i : i + POST_CHUNK]
        rows = await d1_query(
            "SELECT c.id, c.message, c.reactions_count AS likes, CASE WHEN c.sentiment_source = 'rule' THEN NULL ELSE c.sentiment END AS sentiment "
            f"FROM comments c WHERE c.post_id IN ({','.join('?' * len(chunk))}) AND c.parent_external_id IS NULL",
            chunk,
            timeout=90.0,
            quiet=True,
        )
        if rows is None:
            raise RuntimeError("comment_read_failed")
        for row in rows:
            details[row["id"]] = {
                "text": row["message"] or "",
                "likes": row["likes"] or 0,
                "sentiment": row["sentiment"],
            }
    return details


async def post_details(movie_id: str) -> dict[str, dict[str, Any]]:
    marks = ",".join("?" * len(POST_PLATFORMS))
    rows = await d1_query(
        f"SELECT p.id, p.platform, substr(p.content, 1, {POST_TEXT_LIMIT}) AS content, p.like_count AS likes, p.sentiment "
        f"FROM posts p WHERE p.movie_id = ? AND p.platform IN ({marks}) AND COALESCE(p.trend_post, 0) = 0 AND {RELEVANT_POST_SQL}",
        [movie_id, *POST_PLATFORMS],
        timeout=90.0,
        quiet=True,
    )
    if rows is None:
        raise RuntimeError("post_read_failed")
    return {
        row["id"]: {
            "text": row["content"] or "",
            "likes": row["likes"] or 0,
            "sentiment": row["sentiment"],
            "post": row["platform"],
        }
        for row in rows
        if has_content(row["content"])
    }


async def main(args: argparse.Namespace) -> None:
    ids, x = load_vectors(args.movie_id)
    details = await comment_details(args.movie_id)
    posts = await post_details(args.movie_id)
    # Bài đăng không có trong kho embedding (chỉ comment) - tính tại chỗ, vài nghìn bài mỗi phim.
    post_ids = list(posts)
    if post_ids:
        ids = ids + post_ids
        x = np.vstack([x, embed_texts([posts[pid]["text"] for pid in post_ids])])
        details.update(posts)
    print(f"{len(post_ids)} bài đăng Facebook/Threads gom cùng bình luận", flush=True)
    keep = np.array([cid in details and has_content(details[cid]["text"]) for cid in ids])
    print(
        f"{len(ids)} vector, {len(details)} bình luận gốc hợp lệ của phim, gom {int(keep.sum())} câu đủ nội dung",
        flush=True,
    )
    ids = [cid for cid, ok in zip(ids, keep, strict=True) if ok]
    x = x[keep]
    clusters = sorted(kmeans(x, args.k), key=lambda c: -len(c.indices))
    rng = random.Random(7)
    output = []
    for c in clusters:
        members = [ids[int(i)] for i in c.indices]
        closest = [ids[int(i)] for i in c.indices[np.argsort(-(x[c.indices] @ c.centroid))[:CLOSEST]]]
        liked = sorted(members, key=lambda cid: -details[cid]["likes"])[:TOP_LIKED_SAMPLES]
        others = rng.sample(members, min(RANDOM_SAMPLES, len(members)))
        sample_ids = list(dict.fromkeys(closest + liked + others))
        output.append(
            {
                "cluster": c.id,
                "comments": len(c.indices),
                "share_percent": round(len(c.indices) * 100 / len(ids), 1),
                "cohesion": round(float((x[c.indices] @ c.centroid).mean()), 3),
                "samples": [
                    {
                        "text": (f"[Bài đăng {details[cid]['post']}] " if details[cid].get("post") else "")
                        + details[cid]["text"][:160],
                        "parent": None,
                        "likes": details[cid]["likes"],
                        "sentiment": details[cid]["sentiment"],
                    }
                    for cid in sample_ids
                ],
            }
        )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{args.movie_id}.json").write_text(json.dumps(output, ensure_ascii=False, indent=1), encoding="utf-8")
    np.savez_compressed(OUT_DIR / f"{args.movie_id}.npz", centroids=np.stack([c.centroid for c in clusters]))
    # Thành viên từng cụm và chi tiết comment đã gom cho name_clusters / summarize_clusters / publish_clusters.
    members = {str(c.id): [ids[int(i)] for i in c.indices] for c in clusters}
    (OUT_DIR / f"{args.movie_id}.members.json").write_text(json.dumps(members), encoding="utf-8")
    kept = {cid: details[cid] for cid in ids}
    (OUT_DIR / f"{args.movie_id}.details.json").write_text(json.dumps(kept, ensure_ascii=False), encoding="utf-8")
    print(f"đã ghi {OUT_DIR / f'{args.movie_id}.json'}: {len(output)} cụm")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--k", type=int, default=30)
    asyncio.run(main(parser.parse_args()))
