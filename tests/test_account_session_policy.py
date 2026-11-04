"""Cổng cookie lúc nhập, chính sách ghim proxy, trạng thái dead không bị ghi đè (04/11/2026)."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.api.routes import settings as routes
from app.core.errors import CookieRefusedError, ProxyPolicyError
from app.services.accounts import platform_config_db as db
from app.services.accounts.cookie_quality import assess_cookie
from app.services.accounts.user_agent import merge_user_agent

NOW = 1_800_000_000
MAC_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/156.0.0.0 Safari/537.36"


def cookie(age_days=5, user_agent=MAC_UA, **extra):
    parts = {"c_user": "123", "xs": f"33:abc:2:{NOW - age_days * 86400}:-1:1", "datr": "d", "sb": "s", **extra}
    return merge_user_agent("; ".join(f"{k}={v}" for k, v in parts.items()), user_agent)


def test_real_browser_cookie_passes_and_facts_hold_no_values():
    result = assess_cookie(cookie(), "123", now=NOW)
    assert result.problems == []
    assert result.facts["xs_age_days"] == 5
    assert "abc" not in str(result.facts)


@pytest.mark.parametrize(
    "value,problem",
    [
        (cookie(376), "xs_too_old"),
        (
            cookie(user_agent="Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 Chrome/137.0.1697.129"),
            "user_agent_impossible_chrome_build",
        ),
        ("c_user=123; xs=a; datr=d", "missing_user_agent"),
        (merge_user_agent("c_user=123; xs=a", MAC_UA), "missing_datr"),
        (cookie(), "c_user_mismatch"),
    ],
)
def test_bad_cookie_problems(value, problem):
    account = "999" if problem == "c_user_mismatch" else "123"
    assert problem in assess_cookie(value, account, now=NOW).problems


def test_json_export_is_read_and_broken_json_is_refused():
    exported = '[{"name":"c_user","value":"123"},{"name":"xs","value":"a"},{"name":"datr","value":"d"}]'
    assert assess_cookie(merge_user_agent(exported, MAC_UA), "123", now=NOW).problems == []
    assert assess_cookie("[{broken", "123", now=NOW).problems == ["cookie_unreadable"]


def test_route_refuses_facebook_only_and_allows_empty_cookie():
    with pytest.raises(CookieRefusedError, match="missing_xs"):
        routes._refuse_bad_cookie("facebook", "123", merge_user_agent("c_user=123; datr=d", MAC_UA))
    routes._refuse_bad_cookie("facebook", "123", "")
    routes._refuse_bad_cookie("threads", "someone", "sessionid=x")


def target(**fields):
    return {"platform": "facebook", "ip_type": "static", "proxy_platform": "facebook", "others": 0, **fields}


@pytest.mark.parametrize(
    "fields,fragment",
    [
        ({"ip_type": "rotating"}, "rotating_ip"),
        ({"platform": "threads", "ip_type": "rotating"}, "rotating_ip"),
        ({"others": 1}, "shared_ip"),
        ({"platform": "threads", "others": 1}, "shared_ip"),
        ({"proxy_platform": "public"}, "public_proxy"),
        ({"ip_type": "unknown"}, "ip_type_not_static"),
    ],
)
def test_pin_refusals(fields, fragment):
    assert db.proxy_pin_refusal(target(**fields)) == fragment


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"platform": "threads", "ip_type": "unknown", "proxy_platform": "threads"},
        {"platform": "tiktok", "ip_type": "rotating", "others": 3},
    ],
)
def test_pin_allowed(fields):
    assert db.proxy_pin_refusal(target(**fields)) is None


class Cur:
    def __init__(self, rows):
        self.rows, self.sql = list(rows), []

    async def execute(self, sql, params=None):
        self.sql.append((str(sql), params))

    async def fetchone(self):
        return self.rows.pop(0)


class Connection:
    def __init__(self, cur):
        self._cur = cur
        self.commit = AsyncMock()

    def cursor(self):
        return self

    async def __aenter__(self):
        return self._cur

    async def __aexit__(self, *args):
        return False


class Ctx:
    def __init__(self, cur):
        self.conn = Connection(cur)

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        return False


def use_db(monkeypatch, rows):
    cur = Cur(rows)
    monkeypatch.setattr(db, "_ensure_pool_columns", AsyncMock())
    monkeypatch.setattr(db, "_connect", lambda: Ctx(cur))
    return cur


async def test_refused_pin_changes_nothing(monkeypatch):
    cur = use_db(monkeypatch, [target(ip_type="rotating")])
    with pytest.raises(ProxyPolicyError):
        await db.set_account_proxy(1, 15)
    assert not any(sql.startswith("UPDATE") for sql, _ in cur.sql)


async def test_pin_does_not_look_like_a_new_cookie(monkeypatch):
    cur = use_db(monkeypatch, [target(), {"id": 1}])
    await db.set_account_proxy(1, 18)
    update = next(sql for sql, _ in cur.sql if sql.startswith("UPDATE"))
    assert "updated_at = now()" not in update


async def test_health_check_cannot_overwrite_dead(monkeypatch):
    cur = use_db(monkeypatch, [None])
    monkeypatch.setattr(db, "get_account", AsyncMock(return_value={"id": 1, "last_check_status": "dead"}))
    row = await db.update_account_check_result(1, status="unknown")
    assert row["last_check_status"] == "dead"
    assert "NOT (last_check_status IS NOT DISTINCT FROM 'dead'" in cur.sql[0][0]


async def test_only_cookie_changes_touch_updated_at(monkeypatch):
    cur = use_db(monkeypatch, [{"id": 1}, {"id": 1}])
    await db.update_account(1, {"password": "x"})
    await db.update_account(1, {"cookie": "c_user=1"})
    assert "updated_at = now()" not in cur.sql[0][0]
    assert "updated_at = now()" in cur.sql[1][0]


async def test_resaving_the_same_cookie_is_not_a_new_session(monkeypatch):
    stored = cookie()
    monkeypatch.setattr(
        routes.db,
        "get_account",
        AsyncMock(return_value={"id": 1, "platform": "facebook", "account_id": "123", "cookie": stored}),
    )
    update = AsyncMock()
    monkeypatch.setattr(routes.db, "update_account", update)
    monkeypatch.setattr(routes.AccountOut, "masked", staticmethod(lambda row: row))
    payload = SimpleNamespace(model_dump=lambda exclude_unset: {"cookie": stored})
    await routes.update_account(1, payload)
    update.assert_not_called()


async def test_set_session_client_records_the_change(monkeypatch):
    row = {"id": 1, "platform": "facebook", "account_id": "123"}
    monkeypatch.setattr(routes.db, "set_session_client", AsyncMock(return_value=row))
    events = AsyncMock()
    monkeypatch.setattr(routes.db, "record_account_event", events)
    monkeypatch.setattr(routes.AccountOut, "masked", staticmethod(lambda r: r))
    await routes.set_session_client(1, routes.AccountSetSessionClient(client="http"))
    assert events.await_args.kwargs == {"client": "http"}
