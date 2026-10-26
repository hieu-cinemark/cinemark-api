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


async def test_large_sample_is_split_into_parts_then_merged() -> None:
    """Mẫu lớn: mỗi phần một lời gọi, rồi lời gọi gộp chọn bằng chứng theo khoá - id thật được giữ nguyên."""
    from app.ai.tasks.report import TOPICS_BATCH_COMMENTS

    comments = [
        {"id": f"real{i}", "message": f"bình luận {i}", "reactions_count": 1000 - i, "sentiment": "positive"}
        for i in range(TOPICS_BATCH_COMMENTS + 1)
    ]

    def part(evidence_alias: str) -> str:
        return json.dumps(
            {
                "top_10_topics": [
                    {
                        "topic_name": "Phản diện",
                        "sentiment": "Positive",
                        "insight_summary": "...",
                        "evidence_comments": [{"id": evidence_alias, "text": "hay", "likes": 5}],
                    }
                ],
                "top_10_verbatims": [{"id": evidence_alias, "text": "hay", "likes": 5, "why_it_matters": "x"}],
            }
        )

    merged = json.dumps(
        {
            "top_10_topics": [
                {
                    "topic_name": "Phản diện gây sốt",
                    "sentiment": "Positive",
                    "insight_summary": "Gộp 2 phần",
                    "evidence_keys": ["e1", "e2", "e99"],
                }
            ],
            "top_10_verbatims": [{"key": "v2", "why_it_matters": "nhiều like"}],
        }
    )
    bee = AsyncMock(side_effect=[part("c1"), part("c2"), merged])
    with patch("app.ai.tasks.report.call_bee", bee):
        result = await generate_topics_and_verbatims("Phim A", comments)

    assert bee.await_count == 3  # 2 phần + 1 lần gộp
    assert [call.kwargs["task"] for call in bee.await_args_list] == ["topics", "topics", "topics_merge"]
    topic = result["top_10_topics"][0]
    assert topic["topic_name"] == "Phản diện gây sốt"
    # phần 1 nhận comment chẵn (real0, real2...), phần 2 nhận comment lẻ: c1 của phần 1 = real0, c2 của phần 2 = real3
    assert [e["id"] for e in topic["evidence_comments"]] == ["real0", "real3"]  # e99 không tồn tại -> bỏ
    assert (
        result["top_10_verbatims"][0]["id"] == "real3"
        and result["top_10_verbatims"][0]["why_it_matters"] == "nhiều like"
    )


async def test_one_failed_part_still_merges_the_rest() -> None:
    from app.ai.tasks.report import TOPICS_BATCH_COMMENTS

    comments = [
        {"id": f"real{i}", "message": f"bình luận {i}", "reactions_count": 1, "sentiment": "positive"}
        for i in range(TOPICS_BATCH_COMMENTS + 1)
    ]
    ok = json.dumps(
        {"top_10_topics": [{"topic_name": "Nhạc", "evidence_comments": [{"id": "c1"}]}], "top_10_verbatims": []}
    )
    # phần 1 lỗi cả 2 lần thử, phần 2 được -> chỉ một phần thành công thì dùng thẳng, không gọi gộp
    bee = AsyncMock(side_effect=[RuntimeError("x"), RuntimeError("x"), ok])
    with patch("app.ai.tasks.report._BATCH_CONCURRENCY", 1), patch("app.ai.tasks.report.call_bee", bee):
        result = await generate_topics_and_verbatims("Phim A", comments)
    assert bee.await_count == 3
    assert result["top_10_topics"][0]["topic_name"] == "Nhạc"


async def test_topics_without_verbatims_or_with_trailing_text_are_accepted() -> None:
    topics_only = json.dumps({"top_10_topics": [{"topic_name": "Nhạc phim"}]})
    with patch("app.ai.tasks.report.call_bee", AsyncMock(return_value=topics_only)):
        result = await generate_topics_and_verbatims("Phim A", COMMENTS)
    assert result["top_10_topics"][0]["topic_name"] == "Nhạc phim" and result["top_10_verbatims"] == []

    trailing = TOPICS + '\n\n{"note": "thêm"}'
    with patch("app.ai.tasks.report.call_bee", AsyncMock(return_value=trailing)):
        result = await generate_topics_and_verbatims("Phim A", COMMENTS)
    assert result["top_10_topics"][0]["topic_name"] == "Diễn xuất"
