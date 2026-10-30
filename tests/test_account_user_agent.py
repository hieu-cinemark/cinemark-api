"""app/services/accounts/user_agent.py: ô User-Agent của tài khoản <-> cookie giả useragent=<base64> (30/10/2026)."""

import base64

from app.services.accounts.user_agent import merge_user_agent, split_user_agent

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"


def test_merge_then_split_roundtrip():
    cookie = merge_user_agent("c_user=1; xs=2", UA)
    assert cookie.startswith("c_user=1; xs=2; useragent=")
    assert split_user_agent(cookie) == ("c_user=1; xs=2", UA)


def test_merge_replaces_existing_user_agent():
    old = "c_user=1; useragent=" + base64.b64encode(b"old").decode() + "; xs=2"
    assert split_user_agent(merge_user_agent(old, UA)) == ("c_user=1; xs=2", UA)


def test_split_without_user_agent_and_bad_value():
    assert split_user_agent("c_user=1; xs=2") == ("c_user=1; xs=2", "")
    assert split_user_agent("c_user=1; useragent=%%%") == ("c_user=1", "")
    assert merge_user_agent("", UA).startswith("useragent=")
