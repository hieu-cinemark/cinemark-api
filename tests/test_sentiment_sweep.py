"""app/workers/ingest_consumer/sentiment_sweep.py with D1 and Bee stubbed."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.workers.ingest_consumer import sentiment_sweep


@pytest.fixture
def d1(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, list]]:
    calls: list[tuple[str, list]] = []
    rows = [{"id": f"c{n}", "message": f"bình luận số {n}"} for n in range(5)]

    async def fake_d1_query(sql: str, params: list | None = None, **_):
        calls.append((sql, params or []))
        return rows if sql.startswith("SELECT") else []

    monkeypatch.setattr(sentiment_sweep, "d1_query", fake_d1_query)
    return calls


def _labels(monkeypatch: pytest.MonkeyPatch, labels: list[str | None]) -> None:
    async def fake_classify(messages):
        return labels[: len(messages)]

    monkeypatch.setattr(sentiment_sweep, "classify_sentiments", fake_classify)


async def test_classifies_and_saves_grouped_by_label(monkeypatch, d1) -> None:
    _labels(monkeypatch, ["positive", "negative", None, "positive", "neutral"])
    since = datetime(2026, 9, 26, 8, tzinfo=UTC)
    result = await sentiment_sweep.classify_pending(limit=10, since=since, exclude={"c9"})

    select_sql, select_params = d1[0]
    assert "sentiment IS NULL" in select_sql and "scraped_at >= ?" in select_sql
    assert select_params == ["2026-09-26T08:00:00+00:00", 11]  # limit + len(exclude)
    updates = {params[0]: params[1:] for sql, params in d1[1:]}
    assert updates == {"positive": ["c0", "c3"], "negative": ["c1"], "neutral": ["c4"]}
    assert all("AND sentiment IS NULL" in sql for sql, _ in d1[1:])  # never overwrite
    assert result["selected"] == 5 and result["classified"] == 4 and result["failed_ids"] == ["c2"]


async def test_excluded_ids_are_skipped(monkeypatch, d1) -> None:
    _labels(monkeypatch, ["neutral"] * 5)
    result = await sentiment_sweep.classify_pending(limit=10, exclude={"c0", "c1"})
    assert result["selected"] == 3


async def test_dry_run_writes_nothing(monkeypatch, d1) -> None:
    _labels(monkeypatch, ["neutral"] * 5)
    result = await sentiment_sweep.classify_pending(limit=10, dry_run=True)
    assert len(d1) == 1 and result["classified"] == 5


async def test_select_failure_raises(monkeypatch) -> None:
    async def failing(*_a, **_k):
        return None

    monkeypatch.setattr(sentiment_sweep, "d1_query", failing)
    with pytest.raises(RuntimeError):
        await sentiment_sweep.classify_pending(limit=10)
