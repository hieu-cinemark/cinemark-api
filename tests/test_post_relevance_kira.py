"""app/ai/tasks/post_relevance.py with Kira and Redis stubbed out."""

from __future__ import annotations

import asyncio
import json
import re

import pytest

from app.ai.tasks import post_relevance
from app.core.config import settings

MOVIE = {"title": "Án Mạng Karaoke", "director": "X", "cast": "A, B", "released_at": "2026-10-02"}
OTHER = ["Án Mạng Karaoke", "Án Mạng Xém Hoàn Hảo"]


class _FakeRedis:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    async def incr(self, key: str) -> int:
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]

    async def expire(self, key: str, seconds: int) -> None:
        return None


@pytest.fixture(autouse=True)
def fast_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(post_relevance, "BATCH_WINDOW_S", 0.05)


@pytest.fixture
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    redis = _FakeRedis()
    import app.clients.redis as redis_module

    monkeypatch.setattr(redis_module, "get_redis_client", lambda: redis)
    return redis


def _classify(content: str = "Và đây là Lan Trinh trong Án Mạng Xém Hoàn Hảo", **kwargs):
    defaults = {
        "content": content,
        "movie": MOVIE,
        "keyword": "Án Mạng Karaoke",
        "platform": "threads",
        "other_titles": OTHER,
    }
    return post_relevance.classify_post_relevance_kira(**{**defaults, **kwargs})


def _reply_with(monkeypatch: pytest.MonkeyPatch, answer, seen: list | None = None) -> None:
    """answer: an Exception, a raw string, or fn(post_count) -> results list."""

    async def fake_call_kira(**kwargs):
        if seen is not None:
            seen.append(kwargs)
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, str):
            return answer
        count = len(re.findall(r"^\[\d+\] TARGET FILM", kwargs["user_prompt"], flags=re.MULTILINE))
        return json.dumps({"results": answer(count)})

    monkeypatch.setattr(post_relevance, "call_kira", fake_call_kira)


def test_concurrent_posts_share_one_call(monkeypatch, fake_redis) -> None:
    seen: list = []
    labels = ["irrelevant", "relevant", "uncertain"]
    _reply_with(
        monkeypatch,
        lambda n: [{"i": i + 1, "classification": labels[i], "score": 0.1 * (i + 1), "reason": "r"} for i in range(n)],
        seen,
    )

    async def run():
        return await asyncio.gather(*(_classify(f"bài {n}") for n in range(3)))

    results = asyncio.run(run())
    assert [r["label"] for r in results] == ["not_related", "related", "uncertain"]
    assert len(seen) == 1
    call = seen[0]
    assert call["task"] == "post_relevance"
    assert "force" not in call  # respects the dashboard's Kira on/off toggle
    assert "[1] TARGET FILM: Án Mạng Karaoke" in call["user_prompt"]
    assert (
        "OTHER TRACKED FILMS (a post about one of these is not about its target): Án Mạng Xém Hoàn Hảo"
        in call["user_prompt"]
    )
    assert '"results"' in call["user_prompt"]


def test_batches_split_at_batch_size(monkeypatch, fake_redis) -> None:
    monkeypatch.setattr(post_relevance, "BATCH_SIZE", 2)
    seen: list = []
    _reply_with(
        monkeypatch, lambda n: [{"i": i + 1, "classification": "relevant", "score": 0.9} for i in range(n)], seen
    )

    async def run():
        return await asyncio.gather(*(_classify(f"bài {n}") for n in range(5)))

    assert all(r["label"] == "related" for r in asyncio.run(run()))
    assert len(seen) == 3  # 2 + 2 + 1


def test_missing_entry_fails_open_for_that_post_only(monkeypatch, fake_redis) -> None:
    _reply_with(
        monkeypatch,
        lambda n: [{"i": 2, "classification": "relevant", "score": 0.8}, {"i": 1, "classification": "maybe"}],
    )

    async def run():
        return await asyncio.gather(_classify("a"), _classify("b"))

    first, second = asyncio.run(run())
    assert first is None and second["label"] == "related"


def test_daily_cap_counts_posts(monkeypatch, fake_redis) -> None:
    monkeypatch.setattr(settings, "kira_post_relevance_daily_cap", 2)
    _reply_with(monkeypatch, lambda n: [{"i": i + 1, "classification": "relevant", "score": 0.9} for i in range(n)])

    async def run():
        return await asyncio.gather(*(_classify(f"bài {n}") for n in range(3)))

    assert sum(result is None for result in asyncio.run(run())) == 1


def test_cap_zero_disables(monkeypatch, fake_redis) -> None:
    monkeypatch.setattr(settings, "kira_post_relevance_daily_cap", 0)
    seen: list = []
    _reply_with(monkeypatch, "{}", seen)
    assert asyncio.run(_classify()) is None and seen == []


@pytest.mark.parametrize(
    "answer", [RuntimeError("kira_temporarily_disabled"), "not json", '{"classification": "relevant"}']
)
def test_failures_fail_open(monkeypatch, fake_redis, answer) -> None:
    _reply_with(monkeypatch, answer)
    assert asyncio.run(_classify()) is None


def test_empty_content_skips_call(monkeypatch, fake_redis) -> None:
    seen: list = []
    _reply_with(monkeypatch, "{}", seen)
    assert asyncio.run(_classify("   ")) is None and seen == []
