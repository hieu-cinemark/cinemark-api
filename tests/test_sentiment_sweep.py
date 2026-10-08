"""app/workers/ingest_consumer/sentiment_sweep.py với D1 và AI được thay bằng bản giả."""

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

    async def columns_ready() -> None:
        return None

    monkeypatch.setattr(sentiment_sweep, "d1_query", fake_d1_query)
    monkeypatch.setattr(sentiment_sweep, "ensure_comment_insight_columns", columns_ready)
    return calls


def _labels(monkeypatch: pytest.MonkeyPatch, labels: list[str | None]) -> None:
    async def fake_classify(messages, films=None):
        return [
            {"sentiment": label, "aspects": ["dien_xuat:+"] if label == "positive" else [], "stage": "da_xem"}
            if label
            else None
            for label in labels[: len(messages)]
        ]

    monkeypatch.setattr(sentiment_sweep, "classify_comments", fake_classify)


async def test_classifies_and_saves_one_update_per_batch(monkeypatch, d1) -> None:
    _labels(monkeypatch, ["positive", "negative", None, "positive", "neutral"])
    since = datetime(2026, 9, 26, 8, tzinfo=UTC)
    result = await sentiment_sweep.classify_pending(limit=10, since=since, exclude={"c9"})

    select_sql, select_params = d1[0]
    assert "sentiment IS NULL" in select_sql and "scraped_at >= ?" in select_sql
    assert select_params == ["2026-09-26T08:00:00+00:00", 11]  # limit + len(exclude)
    assert len(d1) == 2  # 1 SELECT + 1 UPDATE cho cả lô
    update_sql, update_params = d1[1]
    assert "AND sentiment IS NULL" in update_sql  # không bao giờ ghi đè
    assert "sentiment = CASE id" in update_sql and "insights_classified_at = CURRENT_TIMESTAMP" in update_sql
    assert update_params[-4:] == ["c0", "c1", "c3", "c4"]
    pairs = update_params[:-4]
    assert ["c0", '["dien_xuat:+"]'] == pairs[0:2] and ["c3", '["dien_xuat:+"]'] == pairs[4:6]
    assert ["c0", "positive", "c1", "negative"] == pairs[16:20]
    assert result["selected"] == 5 and result["classified"] == 4 and result["failed_ids"] == ["c2"]
    assert result["with_aspects"] == 2


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


async def test_insights_only_keeps_existing_sentiment(monkeypatch, d1) -> None:
    _labels(monkeypatch, ["positive"] * 5)
    result = await sentiment_sweep.classify_pending(limit=10, insights_only=True, movie_id="movie_1")

    select_sql, select_params = d1[0]
    assert "c.sentiment IS NOT NULL AND c.insights_classified_at IS NULL" in select_sql
    assert "post_id IN (SELECT id FROM posts WHERE movie_id = ?)" in select_sql and select_params[0] == "movie_1"
    update_sql, _ = d1[1]
    assert "sentiment = CASE" not in update_sql and "sentiment_classified_at" not in update_sql
    assert "AND insights_classified_at IS NULL" in update_sql
    assert result["classified"] == 5
