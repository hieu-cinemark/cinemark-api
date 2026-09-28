"""app/kira/post_relevance.py with Kira and Redis stubbed out."""

from __future__ import annotations

import asyncio
import json

import pytest

from app.core.config import settings
from app.kira import post_relevance

MOVIE = {"title": "Án Mạng Karaoke", "director": "X", "cast": "A, B", "released_at": "2026-10-02"}


class _FakeRedis:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    async def incr(self, key: str) -> int:
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]

    async def expire(self, key: str, seconds: int) -> None:
        return None


@pytest.fixture
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    redis = _FakeRedis()
    import app.services.redis as redis_module

    monkeypatch.setattr(redis_module, "get_redis_client", lambda: redis)
    return redis


def _run(**kwargs):
    defaults = {"content": "Và đây là Lan Trinh trong Án Mạng Xém Hoàn Hảo", "movie": MOVIE,
                "keyword": "Án Mạng Karaoke", "platform": "threads",
                "other_titles": ["Án Mạng Karaoke", "Án Mạng Xém Hoàn Hảo"]}  # fmt: skip
    return asyncio.run(post_relevance.classify_post_relevance_kira(**{**defaults, **kwargs}))


def _reply(monkeypatch: pytest.MonkeyPatch, reply: str | Exception, seen: list | None = None) -> None:
    async def fake_call_kira(**kwargs):
        if seen is not None:
            seen.append(kwargs)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(post_relevance, "call_kira", fake_call_kira)


def test_maps_kira_labels(monkeypatch, fake_redis) -> None:
    seen: list = []
    _reply(monkeypatch, json.dumps({"classification": "irrelevant", "score": 0.05, "reason": "different film"}), seen)
    result = _run()
    assert result == {"label": "not_related", "confidence": 0.05, "reason": "different film"}
    call = seen[0]
    assert call["task"] == "post_relevance"
    assert "force" not in call  # respects the dashboard's Kira on/off toggle
    assert "TARGET FILM: Án Mạng Karaoke" in call["user_prompt"]
    assert "OTHER TRACKED FILMS (not the target): Án Mạng Xém Hoàn Hảo" in call["user_prompt"]


def test_daily_cap_stops_calls(monkeypatch, fake_redis) -> None:
    monkeypatch.setattr(settings, "kira_post_relevance_daily_cap", 2)
    seen: list = []
    _reply(monkeypatch, json.dumps({"classification": "relevant", "score": 0.9}), seen)
    results = [_run() for _ in range(3)]
    assert [r["label"] if r else None for r in results] == ["related", "related", None]
    assert len(seen) == 2


def test_cap_zero_disables(monkeypatch, fake_redis) -> None:
    monkeypatch.setattr(settings, "kira_post_relevance_daily_cap", 0)
    seen: list = []
    _reply(monkeypatch, "{}", seen)
    assert _run() is None and seen == []


@pytest.mark.parametrize(
    "reply", [RuntimeError("kira_temporarily_disabled"), "not json", '{"classification": "maybe"}']
)
def test_failures_fail_open(monkeypatch, fake_redis, reply) -> None:
    _reply(monkeypatch, reply)
    assert _run() is None


def test_empty_content_skips_call(monkeypatch, fake_redis) -> None:
    seen: list = []
    _reply(monkeypatch, "{}", seen)
    assert _run(content="   ") is None and seen == []
