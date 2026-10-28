"""query_discovery._prune: tắt sớm câu AI không ra bài nào sau EARLY_PRUNE_DAYS; câu đủ PRUNE_AFTER_DAYS dọn như cũ."""

import asyncio
from datetime import UTC, datetime, timedelta

from app.services.discovery import query_discovery as qd


def _row(kid, days_old, found):
    at = (datetime.now(tz=UTC) - timedelta(days=days_old)).isoformat()
    return {"movie_id": "m1", "tag": f"threads:{kid}", "display": kid, "source": "query", "seen": 0,
            "keyword_id": kid, "decided_at": at, "found": found}


def _run(monkeypatch, rows, yields):
    disabled: list[str] = []
    recorded: list[tuple[str, str]] = []

    async def fake_d1(sql, params=None, **_):
        if sql.startswith("SELECT d.movie_id"):
            return rows
        if sql.startswith("UPDATE keywords SET enabled = 0"):
            disabled.append(params[0])
        return []

    async def fake_yields(_platform):
        return yields

    async def fake_record(movie_id, row, verdict, status, keyword_id):
        recorded.append((keyword_id, verdict["reason"]))

    monkeypatch.setattr(qd, "d1_query", fake_d1)
    monkeypatch.setattr(qd, "keyword_yields", fake_yields)
    monkeypatch.setattr(qd, "_record", fake_record)
    pruned = asyncio.run(qd._prune(dry_run=False))
    return pruned, disabled, recorded


def test_early_prune_only_when_nothing_matched(monkeypatch):
    rows = [_row("zero", 1.5, 0), _row("matched", 1.5, 0), _row("found", 1.5, 2)]
    yields = {"zero": {"matched": 0, "dropped": 29}, "matched": {"matched": 3, "dropped": 10}}
    pruned, disabled, recorded = _run(monkeypatch, rows, yields)
    assert disabled == ["zero"] and pruned == 1
    assert "không ra bài nào khớp" in recorded[0][1]


def test_full_age_rules_unchanged(monkeypatch):
    rows = [_row("few", 4, 1), _row("enough", 4, 10)]
    pruned, disabled, _ = _run(monkeypatch, rows, {})
    assert disabled == ["few"] and pruned == 1


def test_threads_only_gets_template_queries():
    assert qd.ADD_PLATFORMS == ("facebook", "threads")
    assert qd.TEMPLATE_ONLY_PLATFORMS == ("threads",)
    queries = qd.candidate_queries({"title": "Huyết Thống", "cast": None})
    assert "Huyết Thống trailer" in queries and "phim Huyết Thống" in queries


def test_no_prune_before_release(monkeypatch):
    upcoming = (datetime.now(tz=UTC) + timedelta(days=10)).date().isoformat()
    just_out = (datetime.now(tz=UTC) - timedelta(days=1)).date().isoformat()
    rows = [
        {**_row("pre", 4, 0), "released_at": upcoming},
        {**_row("fresh", 4, 0), "released_at": just_out},
        {**_row("old", 4, 0), "released_at": "2026-09-01"},
    ]
    pruned, disabled, _ = _run(monkeypatch, rows, {})
    assert disabled == ["old"] and pruned == 1


def test_threads_backfill_copies_facebook_template_queries(monkeypatch):
    inserted: list[str] = []

    async def fake_d1(sql, params=None, **_):
        if sql.startswith("INSERT INTO keywords"):
            inserted.append(params[2])
        return []

    async def fake_record(*_args):
        pass

    monkeypatch.setattr(qd, "d1_query", fake_d1)
    monkeypatch.setattr(qd, "_record", fake_record)
    now = datetime.now(tz=UTC)
    movies = [{"id": "m1", "title": "Huyết Thống", "released_at": now.date().isoformat()}]
    keywords = [
        {"movie_id": "m1", "platform": "threads", "keyword": "Huyết Thống", "enabled": 1},
        {"movie_id": "m1", "platform": "facebook", "keyword": "phim Huyết Thống", "enabled": 1},
        {"movie_id": "m1", "platform": "facebook", "keyword": "đi xem Huyết Thống", "enabled": 0},
    ]
    decisions = [
        {"movie_id": "m1", "tag": "facebook:huyetthongtrailer", "display": "Huyết Thống trailer", "verdict": "film"},
        {"movie_id": "m1", "tag": "facebook:reviewhuyetthong", "display": "review Huyết Thống", "verdict": "other"},
    ]
    added = asyncio.run(qd._backfill_threads_templates(movies, keywords, decisions, now, dry_run=False))
    # câu FB đang bật + câu Kira đã chấm "film"; bỏ câu FB đã tắt và câu bị chấm khác
    assert added == 2 and sorted(inserted) == ["Huyết Thống trailer", "phim Huyết Thống"]
