"""scripts/check_account_health.py - bản cron tương ứng với nút "Check" bấm tay trên
dashboard. Hành vi đáng khoá lại bằng test không phải "nó có gọi
evaluate_account_health không" (quá hiển nhiên) mà là hợp đồng cảnh báo một lần:
Telegram chỉ được báo khi chuyển sang warning/disabled, không bao giờ ở mỗi chu kỳ mà
một vấn đề kéo dài vẫn còn đó - xem docstring module để biết lý do (một sự cố hỏng
nhiều ngày không được lặp lại cùng một cảnh báo mỗi 6 giờ)."""

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
            "scripts.check_account_health.list_accounts",
            AsyncMock(return_value=[_account(last_check_status="warning")]),
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
            "scripts.check_account_health.list_accounts",
            AsyncMock(return_value=[_account(last_check_status="warning")]),
        ),
        patch("scripts.check_account_health.evaluate_account_health", AsyncMock(return_value="ok")),
        patch("scripts.check_account_health.update_account_check_result", AsyncMock()),
        patch("scripts.check_account_health.send_telegram_message", AsyncMock()) as mock_telegram,
    ):
        await check()

    mock_telegram.assert_not_awaited()


async def test_first_ever_check_landing_on_warning_still_alerts() -> None:
    """previous_status là None (chưa từng kiểm tra) - vẫn là chuyển sang trạng thái xấu đi,
    không phải thứ coi là "đã biết rồi"."""
    with (
        patch("scripts.check_account_health.list_accounts", AsyncMock(return_value=[_account(last_check_status=None)])),
        patch("scripts.check_account_health.evaluate_account_health", AsyncMock(return_value="disabled")),
        patch("scripts.check_account_health.update_account_check_result", AsyncMock()),
        patch("scripts.check_account_health.send_telegram_message", AsyncMock()) as mock_telegram,
    ):
        await check()

    mock_telegram.assert_awaited_once()
