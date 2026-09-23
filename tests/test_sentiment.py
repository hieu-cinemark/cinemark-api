"""Exercises app/kira/sentiment.py's fail-open contract: classify_sentiment
must never raise - a backend outage, a malformed response, or a too-short
comment should all just come back as None so callers (handle_comment,
the backfill script) can carry on without the sentiment column."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import openai

from app.kira.sentiment import classify_sentiment


async def test_short_message_skips_backends() -> None:
    with (
        patch("app.kira.sentiment._classify_phobert", AsyncMock()) as mock_phobert,
        patch("app.kira.sentiment._classify_bee", AsyncMock()) as mock_bee,
    ):
        result = await classify_sentiment("ok")

    assert result is None
    mock_phobert.assert_not_awaited()
    mock_bee.assert_not_awaited()


async def test_none_message_returns_none() -> None:
    result = await classify_sentiment(None)
    assert result is None


async def test_phobert_backend_returns_label() -> None:
    with (
        patch("app.kira.sentiment.settings") as mock_settings,
        patch("app.kira.sentiment._classify_phobert", AsyncMock(return_value="positive")) as mock_phobert,
        patch("app.kira.sentiment._classify_bee", AsyncMock()) as mock_bee,
    ):
        mock_settings.sentiment_backend = "phobert"
        result = await classify_sentiment("Phim này hay quá, xem xong muốn coi lại lần nữa")

    assert result == "positive"
    mock_phobert.assert_awaited_once()
    mock_bee.assert_not_awaited()


async def test_phobert_http_success(monkeypatch) -> None:
    from app.kira import sentiment as sentiment_mod

    class _Resp:
        status_code = 200

        def json(self):
            return {"label": "negative", "confidence": 0.9}

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            return _Resp()

    monkeypatch.setattr(sentiment_mod, "httpx", MagicMock(AsyncClient=_Client))
    monkeypatch.setattr(sentiment_mod.settings, "sentiment_backend", "phobert")
    monkeypatch.setattr(sentiment_mod.settings, "phobert_url", "http://127.0.0.1:8090")
    monkeypatch.setattr(sentiment_mod.settings, "phobert_timeout_seconds", 5.0)

    result = await classify_sentiment("Kịch bản dở quá, xem phí tiền vé")
    assert result == "negative"


async def test_bee_backend_parses_json() -> None:
    with (
        patch("app.kira.sentiment.settings") as mock_settings,
        patch("app.kira.sentiment.call_bee", AsyncMock(return_value='{"sentiment": "negative"}')),
        patch("app.kira.sentiment.bee_is_configured", return_value=True),
    ):
        mock_settings.sentiment_backend = "bee"
        result = await classify_sentiment("Kịch bản dở quá, xem phí tiền vé")

    assert result == "negative"


async def test_auto_falls_back_to_bee_when_phobert_none() -> None:
    with (
        patch("app.kira.sentiment.settings") as mock_settings,
        patch("app.kira.sentiment._classify_phobert", AsyncMock(return_value=None)),
        patch("app.kira.sentiment._classify_bee", AsyncMock(return_value="neutral")) as mock_bee,
    ):
        mock_settings.sentiment_backend = "auto"
        result = await classify_sentiment("Không biết nên khen hay chê phim này luôn")

    assert result == "neutral"
    mock_bee.assert_awaited_once()


async def test_bee_malformed_json_returns_none() -> None:
    with (
        patch("app.kira.sentiment.settings") as mock_settings,
        patch("app.kira.sentiment.call_bee", AsyncMock(return_value="not json at all")),
        patch("app.kira.sentiment.bee_is_configured", return_value=True),
    ):
        mock_settings.sentiment_backend = "bee"
        result = await classify_sentiment("Không biết nên khen hay chê phim này luôn")

    assert result is None


async def test_bee_rate_limit_returns_none() -> None:
    async def raise_rate_limit(**kwargs):
        raise openai.RateLimitError("rate limited", response=AsyncMock(), body=None)

    with (
        patch("app.kira.sentiment.settings") as mock_settings,
        patch("app.kira.sentiment.call_bee", AsyncMock(side_effect=raise_rate_limit)),
        patch("app.kira.sentiment.bee_is_configured", return_value=True),
    ):
        mock_settings.sentiment_backend = "bee"
        result = await classify_sentiment("Diễn viên đóng đạt lắm, ủng hộ phim Việt")

    assert result is None
