"""Đưa nhóm bình luận của một phim lên D1 để Social Topic hiện (2026-11-09): đọc data/clusters/<movie_id>.final.json
(scripts/tools/summarize_clusters.py - đã gộp, lọc, đếm khen/chê, tổng kết), thay toàn bộ nhóm cũ của phim.

    comment_cluster_topics  (movie_id, cluster) -> name, description, kind, comments, share_percent, merge_with, generated_at,
                            positive, negative, neutral, summary (JSON tom_tat/khen/che/trich_dan)
    comment_cluster_members (movie_id, cluster, comment_id)

    python -m scripts.tools.publish_clusters --movie-id <id>
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime

from app.repositories.d1.sql import d1_exec
from app.repositories.d1.store import d1_query
from app.repositories.d1.tables import comment_cluster_members, comment_cluster_topics
from scripts.tools.cluster_film_comments import OUT_DIR

# D1 giới hạn mỗi câu 100 KB: ~1.500 id comment (khoảng 50 byte mỗi id) mỗi lần ghi.
MEMBER_CHUNK = 1500

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS comment_cluster_topics (
        movie_id TEXT NOT NULL, cluster INTEGER NOT NULL, name TEXT NOT NULL, description TEXT, kind TEXT,
        comments INTEGER NOT NULL, share_percent REAL, merge_with TEXT, generated_at TEXT NOT NULL,
        PRIMARY KEY (movie_id, cluster)
    )""",
    """CREATE TABLE IF NOT EXISTS comment_cluster_members (
        movie_id TEXT NOT NULL, cluster INTEGER NOT NULL, comment_id TEXT NOT NULL,
        PRIMARY KEY (movie_id, cluster, comment_id)
    ) WITHOUT ROWID""",
)
EXTRA_COLUMNS = ("positive INTEGER", "negative INTEGER", "neutral INTEGER", "summary TEXT", "posts INTEGER")


async def run(movie_id: str) -> None:
    groups = json.loads((OUT_DIR / f"{movie_id}.final.json").read_text(encoding="utf-8"))
    for sql in SCHEMA:
        # Giữ SQL thuần: CREATE TABLE/ALTER TABLE là DDL.
        if await d1_query(sql, timeout=60.0) is None:
            raise SystemExit("không tạo được bảng")
    existing = {
        # Giữ SQL thuần: pragma_table_info đọc cột thực tế của D1.
        row["name"]
        for row in await d1_query("SELECT name FROM pragma_table_info('comment_cluster_topics')") or []
    }
    for column in EXTRA_COLUMNS:
        if column.split()[0] not in existing:
            await d1_query(f"ALTER TABLE comment_cluster_topics ADD COLUMN {column}", timeout=60.0)
    for table in ("comment_cluster_topics", "comment_cluster_members"):
        tbl = comment_cluster_topics if table == "comment_cluster_topics" else comment_cluster_members
        await d1_exec(tbl.delete().where(tbl.c.movie_id == movie_id), timeout=120.0)
    now = datetime.now(tz=UTC).isoformat()
    topics = [
        {
            "cluster": group["cluster"],
            "name": group.get("name") or f"Nhóm {group['cluster']}",
            "description": group.get("description"),
            "kind": group.get("kind"),
            "comments": group["comments"],
            "share": group.get("share_percent"),
            "merge": json.dumps(group.get("merged") or []),
            "positive": group.get("positive", 0),
            "negative": group.get("negative", 0),
            "neutral": group.get("neutral", 0),
            "summary": json.dumps(group["summary"], ensure_ascii=False) if group.get("summary") else None,
            "posts": group.get("posts", 0),
        }
        for group in groups
    ]
    # Giữ SQL thuần: INSERT ... SELECT ... json_each - ghi cả nhóm bằng một tham số JSON.
    await d1_query(
        "INSERT INTO comment_cluster_topics (movie_id, cluster, name, description, kind, comments, share_percent, merge_with, generated_at, "
        "positive, negative, neutral, summary, posts) "
        "SELECT ?1, value ->> 'cluster', value ->> 'name', value ->> 'description', value ->> 'kind', value ->> 'comments', "
        "value ->> 'share', value ->> 'merge', ?2, value ->> 'positive', value ->> 'negative', value ->> 'neutral', value ->> 'summary', value ->> 'posts' "
        "FROM json_each(?3)",
        [movie_id, now, json.dumps(topics, ensure_ascii=False)],
        timeout=60.0,
    )
    written = 0
    for group in groups:
        ids = group["members"]
        for i in range(0, len(ids), MEMBER_CHUNK):
            chunk = ids[i : i + MEMBER_CHUNK]
            # Giữ SQL thuần: INSERT OR IGNORE ... json_each - nạp cả lô id thành viên bằng một tham số JSON.
            result = await d1_query(
                "INSERT OR IGNORE INTO comment_cluster_members (movie_id, cluster, comment_id) SELECT ?1, ?2, value FROM json_each(?3)",
                [movie_id, int(group["cluster"]), json.dumps(chunk)],
                timeout=60.0,
            )
            if result is None:
                raise SystemExit(f"ghi thành viên nhóm {group['cluster']} lỗi sau {written} dòng")
            written += len(chunk)
    print(f"{movie_id}: {len(topics)} nhóm, {written} thành viên")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    asyncio.run(run(parser.parse_args().movie_id))
