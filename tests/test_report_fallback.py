"""app/ai/tasks/report.py: khi chọn Bee thì Bee viết report, Kira thay thế khi lời gọi
Bee lỗi hoặc trả về thứ không dùng được, và dashboard có thể trỏ report thẳng sang
Kira."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

from app.ai.tasks.report import generate_narrative, generate_topics_and_verbatims

TOPICS = json.dumps({"top_10_topics": [{"topic_name": "Diễn xuất"}], "top_10_verbatims": []})
COMMENTS = [{"id": "c1", "message": "Phim hay", "reactions_count": 3, "sentiment": "positive"}]


def _providers(*, active: str, bee, kira):
    return (
        patch("app.ai.tasks.report.active_report_provider", AsyncMock(return_value=active)),
        patch("app.ai.tasks.report.call_bee", bee),
        patch("app.ai.tasks.report.call_kira", kira),
    )


async def test_bee_writes_and_kira_is_not_called() -> None:
    bee, kira = AsyncMock(return_value=TOPICS), AsyncMock()
    p1, p2, p3 = _providers(active="bee", bee=bee, kira=kira)
    with p1, p2, p3:
        result = await generate_topics_and_verbatims("Phim A", COMMENTS)
    assert result["provider"] == "bee" and result["top_10_topics"][0]["topic_name"] == "Diễn xuất"
    kira.assert_not_awaited()


async def test_bee_error_falls_back_to_kira() -> None:
    bee, kira = AsyncMock(side_effect=RuntimeError("bee down")), AsyncMock(return_value=TOPICS)
    p1, p2, p3 = _providers(active="bee", bee=bee, kira=kira)
    with p1, p2, p3:
        result = await generate_topics_and_verbatims("Phim A", COMMENTS)
    assert result["provider"] == "kira"
    assert kira.await_args.kwargs["force"] is True


async def test_truncated_bee_json_falls_back_to_kira() -> None:
    bee, kira = AsyncMock(return_value='{"top_10_topics": [{"topic_na'), AsyncMock(return_value=TOPICS)
    p1, p2, p3 = _providers(active="bee", bee=bee, kira=kira)
    with p1, p2, p3:
        result = await generate_topics_and_verbatims("Phim A", COMMENTS)
    assert result["provider"] == "kira"


async def test_dashboard_kira_setting_skips_bee() -> None:
    bee, kira = AsyncMock(), AsyncMock(return_value=json.dumps({"analysis": "Khán giả khen diễn xuất."}))
    p1, p2, p3 = _providers(active="kira", bee=bee, kira=kira)
    with p1, p2, p3:
        analysis = await generate_narrative(
            "Phim A", positive_percent=70, negative_percent=10, neutral_percent=20, topic_names=["Diễn xuất"]
        )
    assert analysis == "Khán giả khen diễn xuất."
    bee.assert_not_awaited()


async def test_both_providers_failing_returns_none() -> None:
    bee, kira = AsyncMock(side_effect=RuntimeError("bee down")), AsyncMock(side_effect=RuntimeError("kira down"))
    p1, p2, p3 = _providers(active="bee", bee=bee, kira=kira)
    with p1, p2, p3:
        assert await generate_topics_and_verbatims("Phim A", COMMENTS) is None
