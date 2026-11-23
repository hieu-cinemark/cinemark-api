"""Bản D1 của tài khoản/proxy/nguồn proxy/sự kiện tài khoản (d1_accounts): SQL SQLAlchemy chạy thật trên SQLite trong bộ nhớ."""

import asyncio
import sqlite3
from datetime import datetime

import pytest

from app.core.errors import ConflictError, NotFoundError
from app.repositories.d1.sql import compile_sql
from app.services.accounts.config_db import accounts, d1_accounts, d1_store
from app.services.accounts.config_db import proxy_settings as proxy_config


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
    d1_accounts.reset_for_tests()
    return db


def run(coro):
    return asyncio.run(coro)


def _new_account(**extra):
    return run(
        accounts.create_account(
            {
                "platform": "facebook",
                "account_id": "100",
                "email": "a@x.com",
                "password": "pw",
                "cookie": "c_user=1",
                "enabled": True,
                **extra,
            }
        )
    )


def test_account_create_update_and_secret_masking(d1):
    row = _new_account()
    assert row["enabled"] is True and row["pool_status"] == "active" and row["consecutive_failures"] == 0
    assert isinstance(row["created_at"], datetime) and row["cooldown_until"] is None

    full = run(accounts.list_accounts("facebook"))[0]
    assert full["cookie"] == "c_user=1"
    masked = run(accounts.list_accounts("facebook", with_secrets=False))[0]
    assert masked["cookie"] == "" and masked["has_cookie"] is True and masked["has_token"] is False

    # chỉ đổi cookie mới đẩy updated_at lên
    before = row["updated_at"]
    run(accounts.update_account(row["id"], {"email": "b@x.com"}))
    assert run(accounts.get_account(row["id"]))["updated_at"] == before
    updated = run(accounts.update_account(row["id"], {"cookie": "c_user=2"}))
    assert updated["updated_at"] > before and updated["cookie"] == "c_user=2"
    with pytest.raises(NotFoundError):
        run(accounts.update_account(999, {"cookie": "x"}))
    with pytest.raises(NotFoundError):
        run(accounts.update_account(row["id"], {"khong_co": 1}))


def test_set_enabled_resets_pool_state_and_check_result_keeps_dead(d1):
    row = _new_account()
    d1.execute(
        "UPDATE platform_accounts SET status='checkpoint', consecutive_failures=3, cooldown_until='2099-01-01T00:00:00.000000+00:00' WHERE id=?",
        (row["id"],),
    )
    off = run(accounts.set_account_enabled(row["id"], False))
    assert off["enabled"] is False and off["pool_status"] == "checkpoint"
    on = run(accounts.set_account_enabled(row["id"], True))
    assert on["pool_status"] == "active" and on["consecutive_failures"] == 0 and on["cooldown_until"] is None

    ok = run(accounts.update_account_check_result(row["id"], status="ok"))
    assert ok["last_check_status"] == "ok" and isinstance(ok["last_checked_at"], datetime)
    # 'dead' đã ghi sau lần cập nhật cuối thì kiểm tra thụ động không được ghi đè
    d1.execute(
        "UPDATE platform_accounts SET last_check_status='dead', last_checked_at='2099-01-01T00:00:00.000000+00:00' WHERE id=?",
        (row["id"],),
    )
    still = run(accounts.update_account_check_result(row["id"], status="ok"))
    assert still["last_check_status"] == "dead"
    with pytest.raises(NotFoundError):
        run(accounts.update_account_check_result(999, status="ok"))


def test_delete_account_records_event(d1):
    row = _new_account()
    run(accounts.delete_account(row["id"]))
    events = d1.execute("SELECT platform, account_key, event FROM account_events").fetchall()
    assert [tuple(e) for e in events] == [("facebook", "a@x.com", "account_deleted")]
    with pytest.raises(NotFoundError):
        run(accounts.delete_account(row["id"]))


def test_count_crawl_ready_accounts(d1):
    _new_account(account_id="1")
    _new_account(account_id="2", enabled=False)
    cooling = _new_account(account_id="3")
    d1.execute(
        "UPDATE platform_accounts SET cooldown_until='2099-01-01T00:00:00.000000+00:00' WHERE id=?", (cooling["id"],)
    )
    threads = run(accounts.create_account({"platform": "threads", "account_id": "t1", "enabled": True}))
    d1.execute("UPDATE platform_accounts SET nurture_only=1 WHERE id=?", (threads["id"],))
    assert run(accounts.count_crawl_ready_accounts("facebook")) == 1
    assert run(accounts.count_crawl_ready_accounts("threads")) == 0


def test_proxies_join_provider_and_reset_on_edit(d1):
    run(proxy_config.upsert_proxy_provider("webtui", api_url="https://reset", token="tok"))
    proxy = run(
        accounts.create_proxy(
            {"platform": "facebook", "proxy_url": "1.2.3.4:80", "provider_key": "webtui", "login_use_proxy": True}
        )
    )
    assert proxy["login_use_proxy"] is True and proxy["enabled"] is True and proxy["ip_type"] == "unknown"
    d1.execute("UPDATE platform_proxies SET status='degraded', consecutive_failures=4 WHERE id=?", (proxy["id"],))

    listed = run(accounts.list_proxies("facebook"))[0]
    assert (
        listed["pool_status"] == "degraded" and listed["provider_key"] == "webtui" and "provider_last_status" in listed
    )
    assert len(run(accounts.list_proxies())) == 1 and run(accounts.list_proxies("tiktok")) == []

    fixed = run(accounts.update_proxy(proxy["id"], {"username": "u"}))
    assert fixed["pool_status"] == "active" and fixed["consecutive_failures"] == 0 and fixed["username"] == "u"
    with pytest.raises(NotFoundError):
        run(accounts.update_proxy(999, {"username": "u"}))
    run(accounts.delete_proxy(proxy["id"]))
    with pytest.raises(NotFoundError):
        run(accounts.delete_proxy(proxy["id"]))


def test_proxy_providers_keep_token_count_and_delete_guards(d1):
    run(proxy_config.upsert_proxy_provider("a", api_url="https://a", token="secret"))
    kept = run(proxy_config.upsert_proxy_provider("a", api_url="https://a2", token=None))
    assert kept["token"] == "secret" and kept["api_url"] == "https://a2"
    run(proxy_config.upsert_proxy_provider("b", api_url="https://b", token="t"))

    proxy = run(accounts.create_proxy({"platform": "facebook", "proxy_url": "9.9.9.9:1", "provider_key": "a"}))
    listed = {p["key"]: p for p in run(proxy_config.list_proxy_providers())}
    assert listed["a"]["proxy_count"] == 1 and listed["b"]["proxy_count"] == 0

    with pytest.raises(ConflictError, match="still used"):
        run(proxy_config.delete_proxy_provider("a"))
    run(proxy_config.upsert_proxy_settings({"tiktok_synthetic_provider": "b"}))
    with pytest.raises(ConflictError, match="tiktok_synthetic_provider"):
        run(proxy_config.delete_proxy_provider("b"))
    run(proxy_config.upsert_proxy_settings({}))
    run(proxy_config.delete_proxy_provider("b"))
    with pytest.raises(NotFoundError):
        run(proxy_config.delete_proxy_provider("b"))
    run(accounts.delete_proxy(proxy["id"]))
    run(proxy_config.delete_proxy_provider("a"))
