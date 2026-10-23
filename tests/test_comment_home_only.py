"""settings.comment_ai_enabled=False: bình luận chỉ qua luật -> cache -> ViSoBERT (mọi độ tự tin), không gọi AI."""

import asyncio

from app.ai.tasks import sentiment
from app.core.config import settings


def _setup(monkeypatch, predictions):
    monkeypatch.setattr(settings, "comment_ai_enabled", False)

    async def no_cache(messages):
        return [None] * len(messages)

    async def predict(texts):
        return predictions(texts)

    async def ai_called(*args, **kwargs):
        raise AssertionError("AI không được gọi cho bình luận")

    monkeypatch.setattr(sentiment, "cached_comment_labels", no_cache)
    monkeypatch.setattr(sentiment.visobert, "predict", predict)
    monkeypatch.setattr(sentiment, "classify_comments_jev", ai_called)
    monkeypatch.setattr(sentiment, "_classify_batch", ai_called)


def test_low_confidence_comments_still_labelled_by_visobert(monkeypatch):
    _setup(monkeypatch, lambda texts: [("negative", 0.41)] * len(texts))
    labels = asyncio.run(sentiment.classify_comments(["kịch bản hơi rối, xem không hiểu lắm"]))
    assert labels == [{"sentiment": "negative", "aspects": [], "stage": "khac", "source": "visobert"}]


def test_model_unavailable_leaves_comments_unlabelled_without_ai(monkeypatch):
    _setup(monkeypatch, lambda texts: None)
    assert asyncio.run(sentiment.classify_comments(["phim này hay thật sự, xem hai lần"])) == [None]
