"""Bản D1 của nhóm bảng vận hành (d1_ops): từ khoá lọc, lịch cào, cleanup, discovery, Omar - chạy SQL thật trên SQLite trong bộ nhớ."""

import asyncio
import sqlite3
from datetime import date, datetime

import pytest

from app.core.errors import ConflictError, NotFoundError
from app.repositories.d1.sql import compile_sql
from app.services.accounts.config_db import cleanup, d1_ops, d1_store, omar, schedules


@pytest.fixture
def d1(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row

    async def fake_exec(stmt, **kwargs):
        sql, params = compile_sql(stmt)
        cursor = db.execute(sql, params)
        rows = [] if cursor.description is None else [dict(r) for r in cursor.fetchall()]
        db.commit()
        return rows

    monkeypatch.setattr(d1_store, "d1_exec", fake_exec)
    d1_store.reset_for_tests()
    d1_ops.reset_for_tests()


def run(coro):
    return asyncio.run(coro)


def test_filter_keywords_crud_and_conflicts(d1):
    created = run(
        schedules.create_filter_keyword({"keyword": "trailer", "category": "movie_relevant", "enabled": True})
    )
    assert created["enabled"] is True and isinstance(created["created_at"], datetime)
    with pytest.raises(ConflictError):
        run(schedules.create_filter_keyword({"keyword": "trailer", "category": "movie_relevant", "enabled": True}))
    other = run(schedules.create_filter_keyword({"keyword": "spam", "category": "spam_offtopic", "enabled": False}))
    assert other["enabled"] is False

    assert [r["keyword"] for r in run(schedules.list_filter_keywords())] == ["trailer", "spam"]
    assert [r["keyword"] for r in run(schedules.list_filter_keywords("spam_offtopic"))] == ["spam"]

    updated = run(schedules.update_filter_keyword(other["id"], {"enabled": True}))
    assert updated["enabled"] is True
    with pytest.raises(ConflictError):
        run(schedules.update_filter_keyword(other["id"], {"keyword": "trailer", "category": "movie_relevant"}))
    with pytest.raises(NotFoundError):
        run(schedules.update_filter_keyword(999, {"enabled": False}))
    run(schedules.delete_filter_keyword(created["id"]))
    with pytest.raises(NotFoundError):
        run(schedules.delete_filter_keyword(created["id"]))


def test_crawl_schedules_defaults_upsert_and_trigger(d1):
    run(schedules.ensure_default_crawl_schedules({"facebook", "threads"}))
    rows = {r["platform"]: r for r in run(schedules.list_crawl_schedules())}
    assert rows["facebook"]["run_time"] == "07:00" and rows["facebook"]["enabled"] is True
    assert rows["facebook"]["nurture_before"] is False and rows["facebook"]["last_triggered_date"] is None

    saved = run(schedules.upsert_crawl_schedule("facebook", run_time="09:30", enabled=False, nurture_before=True))
    assert saved["run_time"] == "09:30" and saved["enabled"] is False and saved["nurture_before"] is True
    # ensure_default không đè lịch người vận hành đã đặt
    run(schedules.ensure_default_crawl_schedules({"facebook"}))
    assert {r["platform"]: r for r in run(schedules.list_crawl_schedules())}["facebook"]["run_time"] == "09:30"

    run(schedules.mark_crawl_schedule_triggered("facebook", "2026-11-23"))
    assert {r["platform"]: r for r in run(schedules.list_crawl_schedules())}["facebook"]["last_triggered_date"] == date(
        2026, 11, 23
    )


def test_comment_crawl_schedules(d1):
    run(schedules.ensure_default_comment_crawl_schedules({"facebook"}))
    row = run(schedules.list_comment_crawl_schedules())[0]
    assert row["run_time"] == "08:00" and row["enabled"] is False and row["top_n"] == 15
    saved = run(schedules.upsert_comment_crawl_schedule("facebook", run_time="10:00", enabled=True, top_n=30))
    assert saved["enabled"] is True and saved["top_n"] == 30


def test_cleanup_settings_merge_and_history(d1):
    assert run(cleanup.get_cleanup_settings()) == {"settings": {}, "updated_at": None}
    run(cleanup.upsert_cleanup_settings({"batch_size": 500, "enabled": True}))
    merged = run(cleanup.upsert_cleanup_settings({"batch_size": 800, "enabled": None}))
    assert merged["settings"] == {"batch_size": 800, "enabled": True}

    first = run(cleanup.record_cleanup_run_start(dry_run=True, triggered_by="manual"))
    run(
        cleanup.record_cleanup_run_finish(
            first, summary={"posts": 3, "comments": 5, "batches": 1, "remaining_posts": 9}
        )
    )
    second = run(cleanup.record_cleanup_run_start(dry_run=False, triggered_by="schedule"))
    run(cleanup.record_cleanup_run_finish(second, summary={}, error="boom"))
    history = run(cleanup.list_cleanup_run_history(10))
    assert [h["id"] for h in history] == [second, first]
    assert history[1]["dry_run"] is True and history[1]["posts_deleted"] == 3 and history[1]["remaining_posts"] == 9
    assert history[0]["error"] == "boom" and isinstance(history[0]["finished_at"], datetime)
    assert len(run(cleanup.list_cleanup_run_history(1))) == 1


def test_discovery_and_omar(d1):
    assert run(omar.get_discovery_settings()) == {"enabled": True, "updated_at": None}
    assert run(omar.set_discovery_enabled(False))["enabled"] is False
    assert run(omar.get_discovery_settings())["enabled"] is False

    assert run(omar.get_omar_api_settings()) is None
    assert run(omar.upsert_omar_api_key("key-1"))["api_key"] == "key-1"
    run(omar.set_omar_limits(requests_per_minute=30, concurrency=4))
    run(omar.set_omar_limits(daily_requests=-5))
    row = run(omar.get_omar_api_settings())
    assert (row["api_key"], row["requests_per_minute"], row["daily_requests"], row["concurrency"]) == (
        "key-1",
        30,
        0,
        4,
    )
    assert run(omar.omar_requests_cap()) == 30
    assert run(omar.upsert_omar_api_key(None))["api_key"] == ""


def test_tables_created_once(d1, monkeypatch):
    seen: list[str] = []
    real = d1_store.d1_exec

    async def counting(stmt, **kwargs):
        sql, _ = compile_sql(stmt)
        seen.append(sql)
        return await real(stmt, **kwargs)

    monkeypatch.setattr(d1_store, "d1_exec", counting)
    run(schedules.list_crawl_schedules())
    run(schedules.list_crawl_schedules())
    creates = [s for s in seen if s.lstrip().upper().startswith("CREATE")]
    assert len(creates) == 8 and all("IF NOT EXISTS" in s for s in creates)  # 7 bảng + 1 index
