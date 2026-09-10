"""scripts/check_account_health.py - the cron counterpart to the dashboard's
manual "Check" button. The one behavior worth locking in with a test isn't
"does it call evaluate_account_health" (trivial) but the alert-once
contract: Telegram gets pinged on a transition into warning/disabled, never
on every cycle a sustained problem sits there - see the module's own
docstring for why (an outage that stays broken for days must not repeat the
same alert every 6h)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from scripts.check_account_health import check


def _account(**overrides: object) -> dict[str, object]:
    base = {"id": 1, "platform": "tiktok", "account_id": "device_abc", "last_check_status": "ok"}
    return {**base, **overrides}


async def test_transition_into_warning_alerts_once() -> None:
    with (
        patch("scripts.check_account_health.list_accounts", AsyncMock(return_value=[_account(last_check_status="ok")])),
        patch("scripts.check_account_health.evaluate_account_health", AsyncMock(return_value="warning")),
        patch("scripts.check_account_health.update_account_check_result", AsyncMock()) as mock_update,
        patch("scripts.check_account_health.send_telegram_message", AsyncMock()) as mock_telegram,
    ):
        await check()

    mock_update.assert_awaited_once_with(1, status="warning")
    mock_telegram.assert_awaited_once()


async def test_staying_in_warning_does_not_re_alert() -> None:
    with (
        patch(
            "scripts.check_account_health.list_accounts", AsyncMock(return_value=[_account(last_check_status="warning")])
        ),
        patch("scripts.check_account_health.evaluate_account_health", AsyncMock(return_value="warning")),
        patch("scripts.check_account_health.update_account_check_result", AsyncMock()),
        patch("scripts.check_account_health.send_telegram_message", AsyncMock()) as mock_telegram,
    ):
        await check()

    mock_telegram.assert_not_awaited()


async def test_recovering_to_ok_does_not_alert() -> None:
    with (
        patch(
            "scripts.check_account_health.list_accounts", AsyncMock(return_value=[_account(last_check_status="warning")])
        ),
        patch("scripts.check_account_health.evaluate_account_health", AsyncMock(return_value="ok")),
        patch("scripts.check_account_health.update_account_check_result", AsyncMock()),
        patch("scripts.check_account_health.send_telegram_message", AsyncMock()) as mock_telegram,
    ):
        await check()

    mock_telegram.assert_not_awaited()


async def test_first_ever_check_landing_on_warning_still_alerts() -> None:
    """previous_status is None (never checked) - still a transition into a
    degraded state, not something to treat as "already known about"."""
    with (
        patch(
            "scripts.check_account_health.list_accounts", AsyncMock(return_value=[_account(last_check_status=None)])
        ),
        patch("scripts.check_account_health.evaluate_account_health", AsyncMock(return_value="disabled")),
        patch("scripts.check_account_health.update_account_check_result", AsyncMock()),
        patch("scripts.check_account_health.send_telegram_message", AsyncMock()) as mock_telegram,
    ):
        await check()

    mock_telegram.assert_awaited_once()
