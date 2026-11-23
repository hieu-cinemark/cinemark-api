from types import SimpleNamespace

from app.ai.providers import client as cl


def _cfg(**extra):
    return cl.ProviderConfig(
        key="meta", base_url="https://api.meta.ai/v1", api_key="k", model="muse-ember-1.0", **extra
    )


def test_base_url_strips_pasted_endpoint():
    for url in (
        "https://api.meta.ai/v1/responses",
        "https://api.meta.ai/v1/chat/completions/",
        "https://api.meta.ai/v1",
    ):
        assert (
            cl.ProviderConfig(key="m", base_url=url, api_key="k", model="x").openai_base_url == "https://api.meta.ai/v1"
        )


class _Stream:
    def __init__(self, captured):
        self.captured = captured

    def create(self, **params):
        self.captured.append(params)
        delta = SimpleNamespace(content="xin chào")
        yield SimpleNamespace(usage=None, choices=[SimpleNamespace(delta=delta, finish_reason="stop")])


def _invoke(monkeypatch, cfg, max_tokens):
    captured = []
    fake = SimpleNamespace(chat=SimpleNamespace(completions=_Stream(captured)))
    monkeypatch.setattr(cl, "client_for", lambda c: fake)
    out = cl._invoke(cfg, model=cfg.model, system_prompt="s", user_prompt="u", temperature=0.0, max_tokens=max_tokens)
    return captured[0], out.choices[0].message.content


def test_reasoning_provider_sends_effort_and_token_headroom(monkeypatch):
    params, content = _invoke(monkeypatch, _cfg(reasoning_effort="low"), 120)
    assert params["reasoning_effort"] == "low"
    assert params["max_tokens"] == 120 + cl.REASONING_TOKEN_HEADROOM
    assert content == "xin chào"


def test_plain_provider_unchanged(monkeypatch):
    params, _ = _invoke(monkeypatch, _cfg(), 120)
    assert "reasoning_effort" not in params and params["max_tokens"] == 120
