import httpx
import pytest

from app.ai import jev
from app.ai.tasks import sentiment
from app.core.config import settings

LONG = "Phim hay quá, diễn viên diễn tốt, cuối tuần đi xem lần 2"


def _answers(sentiment="positive", s_conf=0.9, stage="da_xem", st_conf=0.8, aspects=None):
    answers = {
        "sentiment": {"type": "choice", "choice": sentiment, "confidence": s_conf},
        "stage": {"type": "choice", "choice": stage, "confidence": st_conf},
    }
    for key in jev.ASPECTS:
        choice, conf = (aspects or {}).get(key, ("khong", 0.99))
        answers[f"a_{key}"] = {"type": "choice", "choice": choice, "confidence": conf}
    return {"model": "jev-1.13.0", "answers": answers, "usage": {"input_tokens": 1600, "output_tokens": 100}}


def test_label_thresholds(monkeypatch):
    label = jev._label(_answers(aspects={"dien_xuat": ("khen", 0.9), "kich_ban": ("che", 0.3)})["answers"])
    assert label == {"sentiment": "positive", "aspects": ["dien_xuat:+"], "stage": "da_xem"}
    low = jev._label(_answers(s_conf=0.4, st_conf=0.3)["answers"])
    assert low["sentiment"] == "neutral" and low["stage"] == "khac"
    assert jev._label({"sentiment": {"choice": "weird"}}) is None


@pytest.mark.asyncio
async def test_classify_uses_jev_then_falls_back(monkeypatch):
    monkeypatch.setattr(settings, "classifier", "jev")
    monkeypatch.setattr(settings, "jev_api_key", "k")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read().decode()
        seen.append(body)
        if "FAIL" in body:
            return httpx.Response(400, json={"error": "bad"})
        return httpx.Response(200, json=_answers())

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(jev.httpx, "AsyncClient", lambda **kw: real_client(transport=transport, **kw))

    async def no_cache(messages):
        return [None] * len(messages)

    async def remember(_):
        return None

    async def kira_off():
        return False

    monkeypatch.setattr(sentiment, "cached_comment_labels", no_cache)
    monkeypatch.setattr(sentiment, "remember_comment_labels", remember)
    monkeypatch.setattr(sentiment, "kira_is_enabled", kira_off)
    monkeypatch.setattr(jev.asyncio, "sleep", lambda *_: _noop())

    labels = await sentiment.classify_comments([LONG, LONG + " FAIL"], [{"title": "Trại Buôn Người"}, None])
    assert labels[0] == {"sentiment": "positive", "aspects": [], "stage": "da_xem"}
    assert labels[1] is None  # Jev lỗi, Kira tắt -> chưa gán, lần quét sau thử lại
    assert any("Phim: Trại Buôn Người" in body for body in seen)


async def _noop():
    return None
