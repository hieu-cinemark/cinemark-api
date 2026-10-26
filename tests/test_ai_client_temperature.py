"""app/ai/client.py _invoke: model từ chối temperature (Bee claude-haiku-5-5, 26/10/2026) -> gọi lại không temperature."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.ai.client import _invoke


def _chunk(text: str | None, finish: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        usage=None, choices=[SimpleNamespace(delta=SimpleNamespace(content=text), finish_reason=finish)]
    )


def _stream_error(message: str):
    def gen():
        raise RuntimeError(message)
        yield  # pragma: no cover

    return gen()


def _client(*streams) -> MagicMock:
    client = MagicMock()
    client.chat.completions.create.side_effect = list(streams)
    return client


def test_temperature_error_in_stream_retries_without_temperature() -> None:
    client = _client(_stream_error("[400]: `temperature` is deprecated for this model."), iter([_chunk("ok", "stop")]))
    with patch("app.ai.client._client_for", return_value=client):
        result = _invoke(MagicMock(), model="m", system_prompt="s", user_prompt="u", temperature=0.3, max_tokens=10)
    assert result.choices[0].message.content == "ok"
    first, second = (call.kwargs for call in client.chat.completions.create.call_args_list)
    assert first["temperature"] == 0.3 and "temperature" not in second


def test_other_errors_are_not_retried() -> None:
    client = _client(_stream_error("[500]: upstream down"))
    with patch("app.ai.client._client_for", return_value=client), pytest.raises(RuntimeError, match="upstream"):
        _invoke(MagicMock(), model="m", system_prompt="s", user_prompt="u", temperature=0.3, max_tokens=10)
    assert client.chat.completions.create.call_count == 1
