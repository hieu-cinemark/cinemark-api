"""Bản tin sau công chiếu trong D1 dùng chung với Cinemark Worker."""

import json

from app.clients.d1 import d1_query, d1_strict

TABLE_SQL = """CREATE TABLE IF NOT EXISTS movie_press_reviews (
    movie_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    next_refresh_at TEXT NOT NULL,
    response_json TEXT NOT NULL,
    analysis_text TEXT,
    analysis_at TEXT,
    analysis_model TEXT,
    analysis_lock_owner TEXT,
    analysis_lock_until INTEGER
)"""


async def ensure_table() -> None:
    with d1_strict():
        await d1_query(TABLE_SQL)


async def load(movie_id: str) -> dict | None:
    with d1_strict():
        rows = await d1_query("SELECT * FROM movie_press_reviews WHERE movie_id = ? LIMIT 1", [movie_id])
    if not rows:
        return None
    row = rows[0]
    return {
        "movieId": row["movie_id"],
        "snapshotId": row["snapshot_id"],
        "generatedAt": row["fetched_at"],
        "nextRefreshAt": row["next_refresh_at"],
        "data": json.loads(row["response_json"]),
        "analysis": {
            "text": row["analysis_text"],
            "generatedAt": row["analysis_at"],
            "model": row["analysis_model"],
            "provider": "Bee",
        }
        if row["analysis_text"]
        else None,
    }


async def save(snapshot: dict) -> None:
    with d1_strict():
        await d1_query(
            """INSERT INTO movie_press_reviews
            (movie_id, snapshot_id, fetched_at, next_refresh_at, response_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(movie_id) DO UPDATE SET
                snapshot_id = excluded.snapshot_id, fetched_at = excluded.fetched_at,
                next_refresh_at = excluded.next_refresh_at, response_json = excluded.response_json,
                analysis_text = NULL, analysis_at = NULL, analysis_model = NULL,
                analysis_lock_owner = NULL, analysis_lock_until = NULL
            WHERE excluded.fetched_at > movie_press_reviews.fetched_at""",
            [
                snapshot["movieId"],
                snapshot["snapshotId"],
                snapshot["generatedAt"],
                snapshot["nextRefreshAt"],
                json.dumps(snapshot["data"], ensure_ascii=False),
            ],
        )


async def update_extracted(snapshot: dict, previous_id: str) -> None:
    """Bổ sung nội dung vào bản đã lưu, không đổi lịch lấy tin hoặc ghi đè bản mới hơn."""
    with d1_strict():
        await d1_query(
            """UPDATE movie_press_reviews SET snapshot_id = ?, response_json = ?,
            analysis_text = NULL, analysis_at = NULL, analysis_model = NULL,
            analysis_lock_owner = NULL, analysis_lock_until = NULL
            WHERE movie_id = ? AND snapshot_id = ?""",
            [
                snapshot["snapshotId"],
                json.dumps(snapshot["data"], ensure_ascii=False),
                snapshot["movieId"],
                previous_id,
            ],
        )
