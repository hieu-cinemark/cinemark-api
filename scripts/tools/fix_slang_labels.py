"""Sửa nhãn cũ của bình luận dùng tiếng lóng khen (2026-11-09): "t gãy khúc này rồi" bị model cảm xúc / khía cạnh gán chê.
Áp sentiment_rules.slang_positive cho bình luận đã có nhãn của một phim: sentiment negative -> positive, khía cạnh ":-" -> ":+".
Đọc theo từng nhóm bài (index post_id); --dry-run chỉ in.

    python -m scripts.tools.fix_slang_labels --movie-id <id> --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.ai.tasks.sentiment_rules import slang_positive
from app.repositories.d1.store import d1_query

POST_CHUNK = 40
SLANG_LIKE = (
    "%gãy%",
    "%u mê%",
    "%gục ngã%",
    "%đổ gục%",
    "%xỉu%",
    "%rụng tim%",
    "%chết mê%",
    "%mê mẩn%",
    "%ngất ngây%",
    "%mlem%",
    "%slay%",
)


async def run(movie_id: str, dry_run: bool) -> None:
    posts = await d1_query("SELECT id FROM posts WHERE movie_id = ?", [movie_id], timeout=90.0, quiet=True)
    post_ids = [row["id"] for row in posts or []]
    like = " OR ".join("c.message LIKE ?" for _ in SLANG_LIKE)
    fixes = []
    for i in range(0, len(post_ids), POST_CHUNK):
        chunk = post_ids[i : i + POST_CHUNK]
        rows = await d1_query(
            f"SELECT c.id, c.message, c.sentiment, c.aspects FROM comments c WHERE c.post_id IN ({','.join('?' * len(chunk))}) AND ({like})",
            [*chunk, *SLANG_LIKE],
            timeout=90.0,
            quiet=True,
        )
        for row in rows or []:
            if not slang_positive(row["message"]):
                continue
            aspects = json.loads(row["aspects"]) if row["aspects"] and row["aspects"].startswith("[") else None
            new_aspects = (
                [label[:-1] + "+" if label.endswith(":-") else label for label in aspects] if aspects else aspects
            )
            new_sentiment = "positive" if row["sentiment"] == "negative" else row["sentiment"]
            if new_sentiment != row["sentiment"] or new_aspects != aspects:
                fixes.append(
                    {
                        "id": row["id"],
                        "s": new_sentiment,
                        "a": json.dumps(new_aspects) if new_aspects is not None else row["aspects"],
                    }
                )
                if dry_run and len(fixes) <= 15:
                    print(f"{row['sentiment']} -> {new_sentiment} | {' '.join(str(row['message']).split())[:90]}")
    print(f"{len(fixes)} bình luận cần sửa")
    if dry_run:
        return
    for i in range(0, len(fixes), 300):
        await d1_query(
            "UPDATE comments SET sentiment = j.s, aspects = j.a FROM (SELECT value ->> 'id' AS id, value ->> 's' AS s, value ->> 'a' AS a "
            "FROM json_each(?1)) AS j WHERE comments.id = j.id",
            [json.dumps(fixes[i : i + 300])],
            timeout=60.0,
        )
    print("đã ghi")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.movie_id, args.dry_run))
