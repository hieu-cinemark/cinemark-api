"""Exercises scripts/replay_dropped_posts.py end to end against a fake D1
(no real Cloudflare D1 database involved - d1_query is patched to an
in-memory table). The thing actually worth verifying here isn't "does SQL
run" but the data-safety contract: a dropped_posts row must only ever be
deleted once persist_post has confirmed the post is safely back in D1 - if
persist_post reports failure (D1 still down, most likely for a
d1_write_failed row), the row must survive so a later re-run can retry it.
Losing that row instead would defeat the entire point of archiving drops in
the first place."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

from scripts.replay_dropped_posts import replay


def _make_fake_d1(rows: list[dict[str, Any]]) -> tuple[AsyncMock, list[str]]:
    """A stand-in for app.services.d1.d1_query: answers a
    `SELECT ... FROM dropped_posts` with `rows`, and records the id of every
    `DELETE FROM dropped_posts WHERE id = ?` it's asked to run."""
    deleted_ids: list[str] = []

    async def fake_d1_query(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]] | None:
        if sql.strip().upper().startswith("SELECT"):
            return rows
        if sql.strip().upper().startswith("DELETE"):
            deleted_ids.append(params[0])
            return []
        raise AssertionError(f"unexpected SQL in test: {sql}")

    return AsyncMock(side_effect=fake_d1_query), deleted_ids


DROPPED_ROW = {
    "id": "dropped_abc123",
    "platform": "tiktok",
    "keyword_id": "kw_1",
    "raw_json": '{"platform": "tiktok", "post_id": "p1", "keyword_id": "kw_1", "external_id": "p1"}',
}

FAKE_KEYWORD = {"id": "kw_1", "movie_id": "movie_1", "platform": "tiktok", "keyword": "some movie"}


async def test_replay_deletes_row_once_persist_succeeds() -> None:
    """The happy path: persist_post confirms the write, so the row is
    replayed and removed from dropped_posts."""
    fake_d1_query, deleted_ids = _make_fake_d1([DROPPED_ROW])

    with (
        patch("scripts.replay_dropped_posts.d1_query", fake_d1_query),
        patch("scripts.replay_dropped_posts.get_keyword", AsyncMock(return_value=FAKE_KEYWORD)),
        patch("scripts.replay_dropped_posts.get_post_mapper", return_value=lambda payload: {"external_id": payload["post_id"]}),
        patch("scripts.replay_dropped_posts.persist_post", AsyncMock(return_value=True)) as mock_persist,
    ):
        await replay(reason="d1_write_failed", platform=None, dry_run=False)

    mock_persist.assert_awaited_once()
    assert deleted_ids == ["dropped_abc123"]


async def test_replay_keeps_row_when_persist_post_fails() -> None:
    """The case this whole feature exists for: D1 is still down, so
    persist_post returns False again - the dropped_posts row must NOT be
    deleted, or that post's data is gone for good."""
    fake_d1_query, deleted_ids = _make_fake_d1([DROPPED_ROW])

    with (
        patch("scripts.replay_dropped_posts.d1_query", fake_d1_query),
        patch("scripts.replay_dropped_posts.get_keyword", AsyncMock(return_value=FAKE_KEYWORD)),
        patch("scripts.replay_dropped_posts.get_post_mapper", return_value=lambda payload: {"external_id": payload["post_id"]}),
        patch("scripts.replay_dropped_posts.persist_post", AsyncMock(return_value=False)) as mock_persist,
    ):
        await replay(reason="d1_write_failed", platform=None, dry_run=False)

    mock_persist.assert_awaited_once()
    assert deleted_ids == []


async def test_replay_skips_row_with_no_registered_mapper() -> None:
    """A row for a platform whose mapper got unregistered since the drop -
    skipped, not deleted, and persist_post is never even attempted."""
    fake_d1_query, deleted_ids = _make_fake_d1([DROPPED_ROW])

    with (
        patch("scripts.replay_dropped_posts.d1_query", fake_d1_query),
        patch("scripts.replay_dropped_posts.get_keyword", AsyncMock(return_value=FAKE_KEYWORD)),
        patch("scripts.replay_dropped_posts.get_post_mapper", return_value=None),
        patch("scripts.replay_dropped_posts.persist_post", AsyncMock(return_value=True)) as mock_persist,
    ):
        await replay(reason="mapper", platform=None, dry_run=False)

    mock_persist.assert_not_awaited()
    assert deleted_ids == []


async def test_dry_run_never_persists_or_deletes() -> None:
    """--dry-run must be a true no-op against both D1 writes."""
    fake_d1_query, deleted_ids = _make_fake_d1([DROPPED_ROW])

    with (
        patch("scripts.replay_dropped_posts.d1_query", fake_d1_query),
        patch("scripts.replay_dropped_posts.get_keyword", AsyncMock(return_value=FAKE_KEYWORD)),
        patch("scripts.replay_dropped_posts.get_post_mapper", return_value=lambda payload: {"external_id": payload["post_id"]}),
        patch("scripts.replay_dropped_posts.persist_post", AsyncMock(return_value=True)) as mock_persist,
    ):
        await replay(reason="d1_write_failed", platform=None, dry_run=True)

    mock_persist.assert_not_awaited()
    assert deleted_ids == []


async def test_platform_filter_is_included_in_the_select() -> None:
    """--platform must actually narrow the SQL, not just be accepted and
    ignored - this is exactly the kind of thing the earlier missing-space
    string-concat bug (`"AND platform = ?"` glued onto the previous clause)
    would have broken silently."""
    seen_sql: list[str] = []

    async def fake_d1_query(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]] | None:
        seen_sql.append(sql)
        return []

    with (
        patch("scripts.replay_dropped_posts.d1_query", AsyncMock(side_effect=fake_d1_query)),
        patch("scripts.replay_dropped_posts.get_keyword", AsyncMock()),
        patch("scripts.replay_dropped_posts.get_post_mapper", return_value=None),
        patch("scripts.replay_dropped_posts.persist_post", AsyncMock()),
    ):
        await replay(reason="d1_write_failed", platform="tiktok", dry_run=False)

    assert len(seen_sql) == 1
    sql = seen_sql[0]
    assert "WHERE reason = ?" in sql
    # The exact bug this guards against: `sql += "AND platform = ?"` (no
    # leading space) glues onto the previous token instead of producing a
    # syntactically valid clause.
    assert " AND platform = ?" in sql
