"""Lượt xem reel Facebook: message kind="metrics" chỉ cập nhật view_count; mapping lấy view_count của payload."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from app.services.platforms import get_post_mapper
from app.workers.ingest_consumer import main as ingest


async def test_metrics_message_only_updates_views() -> None:
    update = AsyncMock(return_value=True)
    mapper = AsyncMock()
    with patch.object(ingest, "update_post_views", update), patch.object(ingest, "get_post_mapper", mapper):
        await ingest.handle_post({"platform": "facebook", "kind": "metrics", "post_id": "123", "view_count": 6500})
    update.assert_awaited_once_with("facebook", "123", 6500)
    mapper.assert_not_called()


async def test_metrics_message_without_views_is_ignored() -> None:
    update = AsyncMock()
    with patch.object(ingest, "update_post_views", update):
        await ingest.handle_post({"platform": "facebook", "kind": "metrics", "post_id": "123", "view_count": 0})
    update.assert_not_awaited()


def test_facebook_mapping_reads_view_count() -> None:
    mapper = get_post_mapper("facebook")
    assert mapper({"post_id": "1", "view_count": 6500})["view_count"] == 6500
    assert mapper({"post_id": "1"})["view_count"] == 0
