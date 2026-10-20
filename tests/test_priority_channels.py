"""priority_channels: kênh Threads ưu tiên -> làm mới follower + xếp job channel_posts của spider-hub với từ khoá Threads
của phim, ghi trạng thái vào result."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from app.clients import kafka
from app.services import channel_profiles, task_queue
from app.services import priority_channels as pc


def _patch(monkeypatch, *, threads_keywords: list[dict], tags: list[dict], publish_ok: bool = True):
    updates: list[list[Any]] = []
    published: list[dict[str, Any]] = []

    async def fake_d1(sql: str, params: list[Any] | None = None, **_kw: Any):
        if sql.startswith("CREATE TABLE"):
            return []
        if "FROM priority_channels WHERE crawled_at IS NULL" in sql:
            return [
                {"platform": "threads", "handle": "minnnsb", "movie_id": "m1"},
                {"platform": "threads", "handle": "nomovie", "movie_id": None},
            ]
        if "k.platform = 'threads'" in sql:
            return threads_keywords
        if "SELECT DISTINCT keyword" in sql:
            return tags
        if sql.startswith("UPDATE priority_channels"):
            updates.append(params or [])
            return []
        raise AssertionError(sql)

    async def fake_followers(platform: str, limit: int, handles: list[str] | None = None) -> dict[str, int]:
        return {"fetched": len(handles or [])}

    async def fake_terms(keyword_id: str) -> list[str]:
        return ["Trại Buôn Người"]

    async def fake_publish(**kwargs: Any) -> bool:
        published.append(kwargs)
        return publish_ok

    monkeypatch.setattr(pc, "d1_query", fake_d1)
    monkeypatch.setattr(pc, "_table_ready", False)
    monkeypatch.setattr(channel_profiles, "refresh_public_followers", fake_followers)
    monkeypatch.setattr(kafka, "_match_terms_for_keyword", fake_terms)
    monkeypatch.setattr(kafka, "publish_threads_channel_posts_request", fake_publish)
    return updates, published


def test_threads_channel_queues_posts_job(monkeypatch) -> None:
    updates, published = _patch(
        monkeypatch,
        threads_keywords=[{"id": "kw-threads", "keyword": "Trại Buôn Người"}],
        tags=[{"keyword": "#traibuonnguoi"}, {"keyword": "#TBN"}],
    )
    assert asyncio.run(pc.process_pending()) == {"processed": 2}
    assert published == [
        {
            "username": "minnnsb",
            "keyword_id": "kw-threads",
            "keyword": "Trại Buôn Người",
            "match_terms": ["Trại Buôn Người"],
            "match_hashtags": ["TBN", "traibuonnguoi"],
            "max_pages": 3,
            "days": 60,
        }
    ]
    results = {params[3]: json.loads(params[1]) for params in updates}
    assert results["minnnsb"]["posts_job"] == "queued" and results["minnnsb"]["profile"] == {"fetched": 2}
    assert results["nomovie"]["posts_job"] == "no_movie"
    assert all(params[0] for params in updates)  # crawled_at ghi khi đã xếp job


def test_threads_channel_without_threads_keyword(monkeypatch) -> None:
    updates, published = _patch(monkeypatch, threads_keywords=[], tags=[])
    asyncio.run(pc.process_pending())
    assert not published
    results = {params[3]: json.loads(params[1]) for params in updates}
    assert results["minnnsb"]["posts_job"] == "no_threads_keyword"


def test_hashtag_keyword_not_sent_as_substring_keyword(monkeypatch) -> None:
    _, published = _patch(
        monkeypatch, threads_keywords=[{"id": "kw-tag", "keyword": "#traibuonnguoi"}], tags=[], publish_ok=False
    )
    asyncio.run(pc.process_pending())
    assert published[0]["keyword"] is None and published[0]["keyword_id"] == "kw-tag"


def test_task_label_channel_posts() -> None:
    assert task_queue.task_label({"type": "channel_posts", "username": "@minnnsb"}) == "@minnnsb"
