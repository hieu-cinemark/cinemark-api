"""Cổng cookie lúc nhập, chính sách ghim proxy, trạng thái dead không bị ghi đè (04/11/2026)."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.api.routes import settings as routes
from app.core.errors import CookieRefusedError
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


def test_cookie_without_user_agent_can_be_saved_but_other_problems_still_refuse():
    routes._refuse_bad_cookie("facebook", "123", "c_user=123; xs=a:b:2:1794032314:-1:-1; datr=d")
    with pytest.raises(CookieRefusedError, match="missing_xs") as err:
        routes._refuse_bad_cookie("facebook", "123", "c_user=123; datr=d")
    assert "missing_user_agent" not in str(err.value)


async def test_resaving_the_same_cookie_is_not_a_new_session(monkeypatch):
    stored = cookie()
    monkeypatch.setattr(
        routes.accounts_db,
        "get_account",
        AsyncMock(return_value={"id": 1, "platform": "facebook", "account_id": "123", "cookie": stored}),
    )
    update = AsyncMock()
    monkeypatch.setattr(routes.accounts_db, "update_account", update)
    monkeypatch.setattr(routes.AccountOut, "masked", staticmethod(lambda row: row))
    payload = SimpleNamespace(model_dump=lambda exclude_unset: {"cookie": stored})
    await routes.update_account(1, payload)
    update.assert_not_called()
