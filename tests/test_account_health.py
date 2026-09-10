"""evaluate_account_health() is pure signal-reading (Redis + the row's own
`enabled` flag) with no network calls to any platform - see
app/services/account_health.py's module docstring for why that's
deliberate. These tests mock Redis/get_token_status and check the status
string for each branch, since that's exactly what ends up written to
platform_accounts.last_check_status and shown on the dashboard."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import redis.exceptions

from app.services.account_health import evaluate_account_health


def _account(**overrides: object) -> dict[str, object]:
    base = {"id": 1, "platform": "tiktok", "account_id": "device_abc", "enabled": True}
    return {**base, **overrides}


async def test_disabled_account_short_circuits_without_touching_redis() -> None:
    with patch("app.services.account_health.get_redis_client") as mock_redis:
        status = await evaluate_account_health(_account(enabled=False))

    assert status == "disabled"
    mock_redis.assert_not_called()


async def test_tiktok_no_block_streak_is_ok() -> None:
    fake_client = AsyncMock()
    fake_client.get.return_value = None
    with patch("app.services.account_health.get_redis_client", return_value=fake_client):
        status = await evaluate_account_health(_account())

    assert status == "ok"
    fake_client.get.assert_awaited_once_with("social_crawler:tiktok_block_streak:device_abc")


async def test_tiktok_with_recorded_block_is_warning() -> None:
    # The value itself doesn't matter - evaluate_account_health only checks
    # whether the key exists (see account_health.py's _check_tiktok), same
    # as a real Redis GET on a key client.py created with INCR.
    fake_client = AsyncMock()
    fake_client.get.return_value = "1"
    with patch("app.services.account_health.get_redis_client", return_value=fake_client):
        status = await evaluate_account_health(_account())

    assert status == "warning"


async def test_redis_error_propagates_instead_of_being_swallowed() -> None:
    """Deliberate, not an oversight - mirrors platform_token.get_token_status,
    which has the same no-try/except shape. A Redis blip should surface as a
    failed check (500 from the route, nothing written to last_check_status)
    rather than silently reporting a wrong status like "ok"."""
    fake_client = AsyncMock()
    fake_client.get.side_effect = redis.exceptions.ConnectionError("redis unreachable")
    with (
        patch("app.services.account_health.get_redis_client", return_value=fake_client),
        pytest.raises(redis.exceptions.ConnectionError),
    ):
        await evaluate_account_health(_account())


async def test_facebook_active_account_with_live_session_is_ok() -> None:
    with patch("app.services.account_health.get_token_status", AsyncMock(return_value=("main", 3600))):
        status = await evaluate_account_health(_account(platform="facebook", account_id="main"))

    assert status == "ok"


async def test_facebook_active_account_with_expired_session_is_warning() -> None:
    with patch("app.services.account_health.get_token_status", AsyncMock(return_value=("main", None))):
        status = await evaluate_account_health(_account(platform="facebook", account_id="main"))

    assert status == "warning"


async def test_facebook_non_active_account_is_unknown() -> None:
    with patch("app.services.account_health.get_token_status", AsyncMock(return_value=("some_other_account", 3600))):
        status = await evaluate_account_health(_account(platform="facebook", account_id="main"))

    assert status == "unknown"


async def test_unrecognized_platform_is_unknown() -> None:
    status = await evaluate_account_health(_account(platform="instagram", account_id="whatever"))

    assert status == "unknown"
