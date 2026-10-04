"""evaluate_account_health() chỉ thuần đọc tín hiệu (Redis + cờ `enabled` của chính dòng
đó), không gọi mạng tới nền tảng nào - xem docstring module của
app/services/account_health.py để biết vì sao đó là chủ đích. Các test này mock
Redis/get_token_status và kiểm tra chuỗi trạng thái cho từng nhánh, vì đó chính là thứ
được ghi vào platform_accounts.last_check_status và hiển thị trên dashboard."""

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
    # Bản thân giá trị không quan trọng - evaluate_account_health chỉ kiểm tra key có tồn
    # tại không (xem _check_tiktok trong account_health.py), giống một lệnh GET Redis thật
    # trên key mà client.py tạo bằng INCR.
    fake_client = AsyncMock()
    fake_client.get.return_value = "1"
    with patch("app.services.account_health.get_redis_client", return_value=fake_client):
        status = await evaluate_account_health(_account())

    assert status == "warning"


async def test_redis_error_propagates_instead_of_being_swallowed() -> None:
    """Có chủ đích, không phải sơ suất - giống platform_token.get_token_status, vốn cũng
    không có try/except. Redis trục trặc nên hiện ra thành lần kiểm tra thất bại (route trả
    500, không ghi gì vào last_check_status) thay vì âm thầm báo sai trạng thái như "ok"."""
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
