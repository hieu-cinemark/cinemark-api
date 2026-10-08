"""Kiểm tra bộ phân loại Kira theo lô của app/ai/tasks/sentiment.py và hợp đồng
fail-open của nó: classify_sentiments không bao giờ được raise - Kira sập, câu trả lời
sai định dạng hoặc thiếu, hay comment quá ngắn đều chỉ trả về None cho các comment bị
ảnh hưởng."""

from __future__ import annotations

import json
import re
from unittest.mock import AsyncMock, patch

import pytest

from app.ai.tasks import sentiment
from app.ai.tasks.sentiment import classify_comments, classify_sentiment, classify_sentiments


@pytest.fixture(autouse=True)
def _no_redis_cache(monkeypatch):
    """Cache nhãn theo nội dung nằm trên Redis thật - test không được đọc/ghi vào đó (và không được thấy
    nhãn mà test trước ghi)."""

    async def empty(messages):
        return [None] * len(messages)

    async def skip(pairs):
        return None

    monkeypatch.setattr(sentiment, "cached_comment_labels", empty)
    monkeypatch.setattr(sentiment, "remember_comment_labels", skip)


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
    assert (
        "1. Kịch bản dở quá" in prompt
        and "2. Phim này hay" in prompt
        and "ok" not in prompt.split("COMMENTS (")[1].split("FILMS (")[0]
    )


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
    # Đã thấy thực tế: một system prompt cũ cho một comment làm Kira bỏ mất "i".
    call, configured = _kira(json.dumps({"results": [{"sentiment": "positive"}, {"sentiment": "negative"}]}))
    with call, configured:
        assert await classify_sentiments([LONG, LONG]) == ["positive", "negative"]


async def test_aspects_and_stage_are_parsed_and_unknown_keys_dropped() -> None:
    reply = json.dumps(
        {
            "results": [
                {
                    "i": 1,
                    "sentiment": "negative",
                    "aspects": ["kich_ban:-", "dien_xuat:+", "made_up:+", "kich_ban:-"],
                    "stage": "da_xem",
                },
                {"i": 2, "sentiment": "positive", "aspects": [{"a": "quang_ba", "p": "+"}], "stage": "hong"},
                {"i": 3, "sentiment": "neutral"},
            ]
        }
    )
    call, configured = _kira(reply)
    with call as mock_call, configured:
        result = await classify_comments([LONG, LONG, LONG, "@Nguyễn Văn An"])
    assert result == [
        {"sentiment": "negative", "aspects": ["kich_ban:-", "dien_xuat:+"], "stage": "da_xem"},
        {"sentiment": "positive", "aspects": ["quang_ba:+"], "stage": "hong"},
        {"sentiment": "neutral", "aspects": [], "stage": "khac"},  # thiếu aspects/stage -> rỗng/khác, vẫn giữ sentiment
        {"sentiment": "neutral", "aspects": [], "stage": "khac"},  # chỉ tag bạn bè: luật, không gọi Kira
    ]
    prompt = mock_call.await_args.kwargs["user_prompt"]
    assert '"dien_xuat"' in prompt and '"da_xem"' in prompt  # danh sách khía cạnh/giai đoạn nằm trong user prompt


async def test_film_context_is_tagged_per_comment_and_described_once() -> None:
    film = {
        "title": "Mẹ Mìn",
        "director": "Jack Carry On",
        "cast": '["Minh Hằng", "Đại Nghĩa"]',
        "released_at": "2026-10-23",
    }
    call, configured = _kira(_reply("positive", "positive", "neutral"))
    with call as mock_call, configured:
        await classify_comments([LONG, f"{LONG} 2", f"{LONG} 3"], [film, film, None])
    prompt = mock_call.await_args.kwargs["user_prompt"]
    assert "1. [F1] Phim này hay" in prompt and "2. [F1] " in prompt and "3. Phim này hay" in prompt
    assert prompt.count("TARGET FILM: Mẹ Mìn") == 1
    assert "Cast: Minh Hằng, Đại Nghĩa" in prompt and "Release date: 2026-10-23" in prompt
