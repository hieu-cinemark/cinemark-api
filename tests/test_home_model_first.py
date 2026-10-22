"""Model nhà trồng trước, ca khó mới gọi Bee: ViSoBERT đủ chắc thì lấy luôn; phần còn lại gọi model quyết định của Bee
(bình luận typesafe/jev-1.14, nội dung bài openai/gpt-6-luna-decisions)."""

from __future__ import annotations

import pytest

from app.ai import jev, visobert
from app.ai.decisions import COMMENT_MODEL_DEFAULT, POST_MODEL_DEFAULT
from app.ai.tasks import sentiment
from app.core.config import settings

MESSAGES = ["Phim hay quá trời luôn mọi người ơi", "Không biết nữa, để xem đã rồi tính"]


@pytest.fixture
def wired(monkeypatch):
    monkeypatch.setattr(settings, "classifier", "jev")
    calls: list[tuple[list[str], str | None]] = []

    async def fake_predict(texts):
        return [("positive", 0.95) if "hay" in t else ("neutral", 0.4) for t in texts]

    async def fake_jev(messages, films, *, kind="comment"):
        calls.append((messages, None if kind == "comment" else kind))
        return [{"sentiment": "neutral", "aspects": [], "stage": "hong"} for _ in messages]

    async def no_cache(messages):
        return [None] * len(messages)

    async def remember(_):
        return None

    monkeypatch.setattr(visobert, "predict", fake_predict)
    monkeypatch.setattr(sentiment, "classify_comments_jev", fake_jev)
    monkeypatch.setattr(sentiment, "cached_comment_labels", no_cache)
    monkeypatch.setattr(sentiment, "remember_comment_labels", remember)
    return calls


@pytest.mark.asyncio
async def test_confident_visobert_skips_bee(wired):
    labels = await sentiment.classify_comments(MESSAGES)
    assert labels[0]["source"] == "visobert" and labels[0]["sentiment"] == "positive"
    assert labels[1]["stage"] == "hong"  # ca khó -> Bee
    assert wired == [([MESSAGES[1]], None)]  # chỉ câu không chắc tới Bee, model bình luận mặc định


@pytest.mark.asyncio
async def test_posts_use_openai_model(wired):
    await sentiment.classify_comments(MESSAGES, kind="post")
    assert wired[0][1] == "post"


@pytest.mark.asyncio
async def test_insights_pass_skips_visobert(wired):
    await sentiment.classify_comments(MESSAGES, home_model=False)
    assert wired == [(MESSAGES, None)]


def test_models():
    assert COMMENT_MODEL_DEFAULT == "typesafe/jev-1.14"
    assert POST_MODEL_DEFAULT == "openai/gpt-6-luna-decisions"
    assert jev.jev_is_configured()
