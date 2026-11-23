"""Bản D1 của bảng cấu hình (d1_store): chạy SQL do SQLAlchemy dựng trên SQLite trong bộ nhớ thay cho D1, kiểm tra DDL, upsert,
RETURNING và kiểu dữ liệu trả về giống bản Postgres (bool, dict, datetime)."""

import asyncio
import sqlite3
from datetime import datetime

import pytest

from app.core.config import settings
from app.repositories.d1.sql import compile_sql
from app.services.accounts.config_db import ai_config, d1_store
from app.services.accounts.config_db import proxy_settings as proxy_config


@pytest.fixture
def d1(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    calls: list[str] = []

    async def fake_exec(stmt, **kwargs):
        assert kwargs.get("database") == "config"
        sql, params = compile_sql(stmt)
        calls.append(sql)
        cursor = db.execute(sql, params)
        rows = [] if cursor.description is None else [dict(r) for r in cursor.fetchall()]
        db.commit()
        return rows

    monkeypatch.setattr(d1_store, "d1_exec", fake_exec)
    monkeypatch.setattr(settings, "config_db_backend", "d1")
    d1_store.reset_for_tests()
    ai_config._forget_ai_settings()
    return calls


def run(coro):
    return asyncio.run(coro)


def test_ai_settings_default_then_upsert(d1):
    row = run(ai_config.get_ai_settings())
    assert row["enabled"] is False and row["prompts"] == {} and row["active_provider"] == ""
    assert isinstance(row["updated_at"], datetime)

    saved = run(ai_config.upsert_ai_settings(enabled=True, prompts={"review": "Tóm tắt đánh giá"}))
    assert saved["enabled"] is True and saved["prompts"] == {"review": "Tóm tắt đánh giá"}

    run(ai_config.set_active_ai_provider("bee"))
    assert run(ai_config.get_ai_settings())["active_provider"] == "bee"


def test_ai_providers_keep_secret_when_not_sent(d1):
    first = run(ai_config.upsert_ai_provider("bee", base_url="https://a", api_key="sk-1", model="haiku"))
    assert first["api_key"] == "sk-1" and first["reasoning_effort"] == ""

    # api_key=None giữ secret cũ, reasoning_effort được ghi khi truyền vào
    again = run(
        ai_config.upsert_ai_provider("bee", base_url="https://b", api_key=None, model="sonnet", reasoning_effort="low")
    )
    assert again["api_key"] == "sk-1" and again["base_url"] == "https://b" and again["reasoning_effort"] == "low"

    assert [p["key"] for p in run(ai_config.list_ai_providers())] == ["bee"]
    assert run(ai_config.get_ai_provider("khong-co")) is None
    assert run(ai_config.delete_ai_provider("bee")) is True
    assert run(ai_config.delete_ai_provider("bee")) is False


def test_proxy_settings_round_trip(d1):
    assert run(proxy_config.get_proxy_settings()) == {"settings": {}, "updated_at": None}
    saved = run(proxy_config.upsert_proxy_settings({"fb_daily_request_cap": 200, "ghi_chu": "tiếng Việt"}))
    assert saved["settings"] == {"fb_daily_request_cap": 200, "ghi_chu": "tiếng Việt"}
    loaded = run(proxy_config.get_proxy_settings())
    assert loaded["settings"]["fb_daily_request_cap"] == 200 and isinstance(loaded["updated_at"], datetime)
    run(proxy_config.upsert_proxy_settings({"fb_daily_request_cap": 300}))
    assert run(proxy_config.get_proxy_settings())["settings"] == {"fb_daily_request_cap": 300}


def test_accuracy_runs_latest_wins(d1):
    assert run(ai_config.get_latest_accuracy_run()) is None
    run(ai_config.insert_accuracy_run(labeled_at="2026-11-01", sample_size=100, metrics={"acc": 0.8}, note=None))
    run(ai_config.insert_accuracy_run(labeled_at="2026-11-10", sample_size=200, metrics={"acc": 0.9}, note="lần 2"))
    latest = run(ai_config.get_latest_accuracy_run())
    assert latest["sample_size"] == 200 and latest["metrics"] == {"acc": 0.9} and latest["note"] == "lần 2"
    assert latest["labeled_at"].isoformat() == "2026-11-10"


def test_tables_created_once(d1):
    run(ai_config.get_ai_settings())
    run(ai_config.get_ai_settings())
    creates = [sql for sql in d1 if sql.lstrip().upper().startswith("CREATE TABLE")]
    assert len(creates) == 4 and all("IF NOT EXISTS" in sql for sql in creates)


def test_default_backend_is_postgres():
    assert type(settings).model_fields["config_db_backend"].default == "postgres"
