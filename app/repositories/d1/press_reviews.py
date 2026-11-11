"""Bản tin sau công chiếu trong D1 dùng chung với Cinemark Worker."""

import json

from sqlalchemy import Column, Integer, Table, Text, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateTable

from app.clients.d1 import d1_strict
from app.repositories.d1.sql import d1_exec, metadata

press = Table(
    "movie_press_reviews",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("snapshot_id", Text, nullable=False),
    Column("fetched_at", Text, nullable=False),
    Column("next_refresh_at", Text, nullable=False),
    Column("response_json", Text, nullable=False),
    Column("analysis_text", Text),
    Column("analysis_at", Text),
    Column("analysis_model", Text),
    Column("analysis_lock_owner", Text),
    Column("analysis_lock_until", Integer),
)

# Nội dung đổi thì bỏ phân tích cũ và khoá đang giữ.
_CLEAR_ANALYSIS = dict.fromkeys(
    ("analysis_text", "analysis_at", "analysis_model", "analysis_lock_owner", "analysis_lock_until")
)


async def ensure_table() -> None:
    with d1_strict():
        await d1_exec(CreateTable(press, if_not_exists=True))


async def load(movie_id: str) -> dict | None:
    with d1_strict():
        rows = await d1_exec(select(press).where(press.c.movie_id == movie_id).limit(1))
    if not rows:
        return None
    row = rows[0]
    analysis = None
    if row["analysis_text"]:
        analysis = {
            "text": row["analysis_text"],
            "generatedAt": row["analysis_at"],
            "model": row["analysis_model"],
            "provider": "Cinemark AI",
        }
    return {
        "movieId": row["movie_id"],
        "snapshotId": row["snapshot_id"],
        "generatedAt": row["fetched_at"],
        "nextRefreshAt": row["next_refresh_at"],
        "data": json.loads(row["response_json"]),
        "analysis": analysis,
    }


async def save(snapshot: dict) -> None:
    """Ghi bản mới; chỉ đè khi bản này lấy sau bản đang lưu."""
    stmt = insert(press).values(
        movie_id=snapshot["movieId"],
        snapshot_id=snapshot["snapshotId"],
        fetched_at=snapshot["generatedAt"],
        next_refresh_at=snapshot["nextRefreshAt"],
        response_json=json.dumps(snapshot["data"], ensure_ascii=False),
    )
    new = stmt.excluded
    stmt = stmt.on_conflict_do_update(
        index_elements=[press.c.movie_id],
        set_={
            "snapshot_id": new.snapshot_id,
            "fetched_at": new.fetched_at,
            "next_refresh_at": new.next_refresh_at,
            "response_json": new.response_json,
            **_CLEAR_ANALYSIS,
        },
        where=new.fetched_at > press.c.fetched_at,
    )
    with d1_strict():
        await d1_exec(stmt)


async def update_extracted(snapshot: dict, previous_id: str) -> None:
    """Bổ sung nội dung vào bản đã lưu, không đổi lịch lấy tin hoặc ghi đè bản mới hơn."""
    stmt = (
        update(press)
        .where(press.c.movie_id == snapshot["movieId"], press.c.snapshot_id == previous_id)
        .values(
            snapshot_id=snapshot["snapshotId"],
            response_json=json.dumps(snapshot["data"], ensure_ascii=False),
            **_CLEAR_ANALYSIS,
        )
    )
    with d1_strict():
        await d1_exec(stmt)
