"""app/ai/tasks/report.py: Bee viết report (Kira đã bỏ), thử lại Bee một lần với temperature lệch khi lời gọi lỗi hoặc
trả về thứ không dùng được."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

from app.ai.tasks.report import generate_narrative, generate_topics_and_verbatims

TOPICS = json.dumps({"top_10_topics": [{"topic_name": "Diễn xuất"}], "top_10_verbatims": []})
COMMENTS = [{"id": "c1", "message": "Phim hay", "reactions_count": 3, "sentiment": "positive"}]


async def test_bee_writes_report() -> None:
    bee = AsyncMock(return_value=TOPICS)
    with patch("app.ai.tasks.report.call_bee", bee):
        result = await generate_topics_and_verbatims("Phim A", COMMENTS)
    assert result["provider"] == "bee" and result["top_10_topics"][0]["topic_name"] == "Diễn xuất"
    assert bee.await_count == 1


async def test_bee_error_is_retried_with_shifted_temperature() -> None:
    bee = AsyncMock(side_effect=[RuntimeError("bee down"), TOPICS])
    with patch("app.ai.tasks.report.call_bee", bee):
        result = await generate_topics_and_verbatims("Phim A", COMMENTS)
    assert result["provider"] == "bee"
    first, second = (call.kwargs["temperature"] for call in bee.await_args_list)
    assert second != first


async def test_truncated_json_is_retried() -> None:
    bee = AsyncMock(side_effect=['{"top_10_topics": [{"topic_na', json.dumps({"analysis": "Khán giả khen diễn xuất."})])
    with patch("app.ai.tasks.report.call_bee", bee):
        analysis = await generate_narrative(
            "Phim A", positive_percent=70, negative_percent=10, neutral_percent=20, topic_names=["Diễn xuất"]
        )
    assert analysis == "Khán giả khen diễn xuất."


async def test_two_failures_return_none() -> None:
    bee = AsyncMock(side_effect=RuntimeError("bee down"))
    with patch("app.ai.tasks.report.call_bee", bee):
        assert await generate_topics_and_verbatims("Phim A", COMMENTS) is None
    assert bee.await_count == 2
