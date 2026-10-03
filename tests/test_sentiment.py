"""Exercises app/ai/tasks/sentiment.py's batched Kira classifier and its
fail-open contract: classify_sentiments must never raise - a Kira outage, a
malformed or partial reply, or a too-short comment all just come back as
None for the affected comments."""

from __future__ import annotations

import json
import re
from unittest.mock import AsyncMock, patch

from app.ai.tasks import sentiment
from app.ai.tasks.sentiment import classify_sentiment, classify_sentiments

LONG = "Phim này hay quá, xem xong muốn coi lại lần nữa"


def _reply(*labels: str) -> str:
    return json.dumps({"results": [{"i": i, "sentiment": s} for i, s in enumerate(labels, 1)]})


def _kira(reply: str | Exception):
    call = AsyncMock(side_effect=reply) if isinstance(reply, Exception) else AsyncMock(return_value=reply)
    return (
        patch("app.ai.tasks.sentiment.call_kira", call),
        patch("app.ai.tasks.sentiment.kira_is_enabled", AsyncMock(return_value=True)),
    )


async def test_batch_maps_results_by_number_and_skips_short() -> None:
    call, configured = _kira(_reply("negative", "positive"))
    with call as mock_call, configured:
        result = await classify_sentiments(["Kịch bản dở quá, phí tiền vé", "ok", None, LONG])
    assert result == ["negative", None, None, "positive"]
    prompt = mock_call.await_args.kwargs["user_prompt"]
    assert "1. Kịch bản dở quá" in prompt and "2. Phim này hay" in prompt and "ok" not in prompt.split("COMMENTS:")[1]


async def test_splits_into_batches_of_batch_size() -> None:
    calls: list[str] = []

    async def fake_call_kira(**kwargs):
        calls.append(kwargs["user_prompt"])
        count = len(re.findall(r"^\d+\. ", kwargs["user_prompt"], flags=re.MULTILINE))
        return _reply(*["neutral"] * count)

    with (
        patch.object(sentiment, "BATCH_SIZE", 3),
        patch("app.ai.tasks.sentiment.call_kira", fake_call_kira),
        patch("app.ai.tasks.sentiment.kira_is_enabled", AsyncMock(return_value=True)),
    ):
        result = await classify_sentiments([f"{LONG} {n}" for n in range(7)])
    assert len(calls) == 3  # 3 + 3 + 1
    assert result == ["neutral"] * 7


async def test_partial_reply_leaves_missing_as_none() -> None:
    call, configured = _kira(json.dumps({"results": [{"i": 2, "sentiment": "positive"}, {"i": 9, "sentiment": "x"}]}))
    with call, configured:
        assert await classify_sentiments([LONG, LONG]) == [None, "positive"]


async def test_kira_switched_off_returns_none_without_calling() -> None:
    with (
        patch("app.ai.tasks.sentiment.call_kira", AsyncMock()) as mock_call,
        patch("app.ai.tasks.sentiment.kira_is_enabled", AsyncMock(return_value=False)),
    ):
        assert await classify_sentiments([LONG]) == [None]
    mock_call.assert_not_awaited()


async def test_bad_shape_fails_open() -> None:
    call, configured = _kira('{"sentiment": "positive"}')
    with call, configured:
        assert await classify_sentiments([LONG, LONG]) == [None, None]


async def test_exception_fails_open() -> None:
    call, configured = _kira(RuntimeError("down"))
    with call, configured:
        assert await classify_sentiment(LONG) is None


async def test_single_wrapper() -> None:
    call, configured = _kira(_reply("positive"))
    with call, configured:
        assert await classify_sentiment(LONG) == "positive"
    assert await classify_sentiment("ok") is None


async def test_unnumbered_results_map_by_order_when_counts_match() -> None:
    # Seen live: a stale one-comment system prompt made Kira drop "i".
    call, configured = _kira(json.dumps({"results": [{"sentiment": "positive"}, {"sentiment": "negative"}]}))
    with call, configured:
        assert await classify_sentiments([LONG, LONG]) == ["positive", "negative"]
