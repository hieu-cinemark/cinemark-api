"""Model nhà trồng trước, ca khó mới gọi AI: ViSoBERT đủ chắc thì lấy luôn; phần còn lại hỏi chat LLM (nhà cung cấp đang chọn)."""

from __future__ import annotations

import pytest

from app.ai.local import visobert
from app.ai.tasks import sentiment

MESSAGES: list[str | None] = ["Phim hay quá trời luôn mọi người ơi", "Không biết nữa, để xem đã rồi tính"]


@pytest.fixture
def wired(monkeypatch):
    calls: list[list[str]] = []

    async def fake_predict(texts):
        return [("positive", 0.95) if "hay" in t else ("neutral", 0.4) for t in texts]

    async def fake_batch(messages, films):
        calls.append(list(messages))
        return [{"sentiment": "neutral", "aspects": [], "stage": "hong", "source": "llm"} for _ in messages]

    async def no_cache(messages):
        return [None] * len(messages)

    async def remember(_):
        return None

    async def enabled():
        return True

    monkeypatch.setattr(visobert, "predict", fake_predict)
    monkeypatch.setattr(sentiment, "_classify_batch", fake_batch)
    monkeypatch.setattr(sentiment, "ai_is_enabled", enabled)
    monkeypatch.setattr(sentiment, "cached_comment_labels", no_cache)
    monkeypatch.setattr(sentiment, "remember_comment_labels", remember)
    return calls


@pytest.mark.asyncio
async def test_confident_visobert_skips_llm(wired):
    labels = await sentiment.classify_comments(MESSAGES)
    assert labels[0]["source"] == "visobert" and labels[0]["sentiment"] == "positive"
    assert labels[1]["stage"] == "hong"  # ca khó -> LLM
    assert wired == [[MESSAGES[1]]]  # chỉ câu không chắc tới LLM


@pytest.mark.asyncio
async def test_insights_pass_skips_visobert(wired):
    await sentiment.classify_comments(MESSAGES, home_model=False)
    assert wired == [MESSAGES]
