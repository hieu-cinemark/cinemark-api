"""Retaining a Facebook recipe must not report an old token as healthy."""

import json
from unittest.mock import AsyncMock

import pytest

from app.services.accounts import platform_token


@pytest.mark.parametrize("age,expected", [(100, 21500), (22000, None)])
async def test_facebook_freshness_uses_refresh_time_not_recipe_ttl(monkeypatch, age, expected):
    client = AsyncMock()
    client.get.side_effect = ['"test"', json.dumps({"captured_at": 1, "http_refreshed_at": 50000 - age})]
    client.ttl.return_value = 604800
    monkeypatch.setattr(platform_token, "get_redis_client", lambda: client)
    monkeypatch.setattr(platform_token.time, "time", lambda: 50000)
    assert await platform_token.get_token_status("facebook") == ("test", expected)


async def test_threads_keeps_existing_ttl_behavior(monkeypatch):
    client = AsyncMock()
    client.get.return_value = '"test"'
    client.ttl.return_value = 3600
    monkeypatch.setattr(platform_token, "get_redis_client", lambda: client)
    assert await platform_token.get_token_status("threads") == ("test", 3600)
