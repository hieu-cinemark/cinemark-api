"""Unit test cho cơ chế thử lại khi storage D1 của Cloudflare backoff trong d1_client.

Phủ bốn tình huống dẫn tới lỗi mà người vận hành đã báo
(scheduled_crawl_firing -> d1_request_failed status=429 code 7429 dây chuyền trong
khoảng 60s):

  1. Response bình thường (status 200) trả về các dòng, không thử lại.
  2. 7429 tạm thời rồi thành công: thử lại với backoff tăng dần và trả về các dòng.
  3. 7429 kéo dài quá max_retries: trả về None sau khi dùng hết ngân sách thử lại,
     kèm một log cảnh báo cuối cùng.
  4. 429 không phải 7429 (ví dụ rate-limit thật): trả về None ngay, không thử lại -
     các đường gọi hiện có coi None là thất bại và ta không muốn che một vấn đề quota
     thật sau một vòng thử lại âm thầm.

Mock thẳng httpx.AsyncClient.post để test chạy không cần mạng. Mỗi test kiểm tra cả
giá trị trả về LẪN số lần gọi HTTP đúng như mong đợi.
"""

from __future__ import annotations

import asyncio

# Cho `from app.clients.d1 import d1_query` chạy được mà không kích hoạt tác dụng phụ
# khi import app.* (settings, cấu hình logging, ...) bằng cách chèn thư mục gốc của repo
# vào sys.path trước khi import.
import os
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.clients import d1 as d1_client  # noqa: E402


class _FakeResp:
    """Vừa đủ phần httpx.Response mà d1_query đọc: .status_code, .text, .json()."""

    def __init__(self, status_code: int, body: dict[str, Any] | str):
        self.status_code = status_code
        if isinstance(body, dict):
            self._json = body
            self.text = ""
        else:
            self._json = {}
            self.text = body

    def json(self) -> dict[str, Any]:
        return self._json


def _make_client_mock(responses: list[_FakeResp]) -> AsyncMock:
    """Trả về một AsyncMock đóng vai httpx.AsyncClient có `.post()` lần lượt trả từng
    response trong `responses`. Đếm số lần gọi bằng một list thường để test kiểm tra được
    đã gọi HTTP bao nhiêu lần - dùng `mock.post.await_count` thì mock.post phải tự là
    thuộc tính Mock, nhưng ta ghi đè nó bằng một hàm thật (để tiêu thụ iterator), nên giữ
    bộ đếm riêng.
    """
    mock = AsyncMock()
    mock.is_closed = False
    call_count = {"n": 0}
    call_iter = iter(responses)

    async def _post(*_args: Any, **_kwargs: Any) -> _FakeResp:
        call_count["n"] += 1
        try:
            return next(call_iter)
        except StopIteration:
            raise AssertionError("d1_query made more HTTP calls than the test supplied responses for")

    mock.post = _post
    mock.call_count = call_count  # type: ignore[attr-defined]
    # d1_query đi vào _remote_sem; patch nó thành no-op để test không thực sự chạy
    # semaphore (vốn ở cấp module và dùng chung giữa các test).
    return mock


async def _run_with_client(mock_client: AsyncMock) -> list[dict[str, Any]] | None:
    """Gọi d1_query sau khi nối _get_http_client của module để trả về mock của ta. Thay
    semaphore remote bằng bản giả để một lần chạy test không dùng chung trạng thái với các
    test khác."""
    sem = asyncio.Semaphore(8)

    async def _fake_sem() -> Any:
        async with sem:
            yield

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=None)
    cm.__aexit__ = AsyncMock(return_value=None)

    with (
        patch.object(d1_client, "_get_http_client", AsyncMock(return_value=mock_client)),
        patch.object(d1_client, "_remote_sem", cm),
        patch.object(d1_client, "_configured", return_value=True),
        patch.object(d1_client, "_logged_db_target", True, create=True),
    ):
        # Reset cờ log info một lần của module để nó không âm thầm đi tắt qua
        # _get_http_client đã patch ở lần chạy test thứ hai.
        d1_client._logged_db_target = True
        return await d1_client.d1_query("SELECT 1", [], quiet=True)


async def test_healthy_response_no_retry() -> None:
    """Một lần 200 có dòng -> d1_query trả về các dòng, gọi HTTP đúng một lần. Chốt chặn hồi
    quy cho "logic thử lại vô tình nhân đôi tải lên các request bình thường"."""
    rows = [{"results": [{"n": 1}]}]
    mock = _make_client_mock([_FakeResp(200, {"success": True, "result": rows})])
    out = await _run_with_client(mock)
    assert out == [{"n": 1}], f"expected rows, got {out}"
    assert mock.call_count["n"] == 1
    print("OK test_healthy_response_no_retry")


async def test_7429_then_success() -> None:
    """Một lần 429 có mã 7429, rồi một lần 200. d1_query phải sleep một lần (base = 2s) và
    trả về các dòng. Tổng số lần gọi HTTP = 2."""
    mock = _make_client_mock(
        [
            _FakeResp(429, '{"success":false,"errors":[{"code":7429,"message":"..."}]}'),
            _FakeResp(200, {"success": True, "result": [{"results": [{"n": 7}]}]}),
        ]
    )
    # Patch asyncio.sleep để test không thực sự dừng 2s.
    with patch("asyncio.sleep", new=AsyncMock()) as fake_sleep:
        out = await _run_with_client(mock)
    assert out == [{"n": 7}], f"expected rows after retry, got {out}"
    assert mock.call_count["n"] == 2, f"expected 2 calls, got {mock.call_count['n']}"
    fake_sleep.assert_awaited_once()
    print("OK test_7429_then_success")


async def test_7429_persistent_exhausts_retries() -> None:
    """Cả 5 lần thử đều trả 7429. d1_query phải bỏ cuộc sau lần thử lại thứ 4 (mặc định
    max_retries=4 -> tổng 5 lần: 1 lần đầu + 4 lần thử lại) và trả về None. Số lần sleep
    phải là 4 (mỗi lần thử lại một lần), với backoff gấp đôi 2s, 4s, 8s, 16s."""
    bodies = [_FakeResp(429, '{"success":false,"errors":[{"code":7429,"message":"..."}]}')] * 5
    mock = _make_client_mock(bodies)
    sleeps: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    with patch("asyncio.sleep", new=AsyncMock(side_effect=_record_sleep)):
        out = await _run_with_client(mock)
    assert out is None, f"expected None after retries exhausted, got {out}"
    assert mock.call_count["n"] == 5, f"expected 5 attempts, got {mock.call_count['n']}"
    assert sleeps == [2.0, 4.0, 8.0, 16.0], f"unexpected backoff schedule: {sleeps}"
    print("OK test_7429_persistent_exhausts_retries")


async def test_non_7429_429_returns_immediately() -> None:
    """Một lần 429 với body khác (ví dụ rate-limit thật của Cloudflare, không có mã 7429)
    KHÔNG được thử lại - các đường xử lý lỗi hiện có coi None là thất bại và ta không
    muốn âm thầm che một vấn đề quota sau một vòng thử lại."""
    mock = _make_client_mock([_FakeResp(429, '{"success":false,"errors":[{"code":10000,"message":"Rate limit"}]}')])
    with patch("asyncio.sleep", new=AsyncMock()) as fake_sleep:
        out = await _run_with_client(mock)
    assert out is None, f"expected None, got {out}"
    assert mock.call_count["n"] == 1, f"expected 1 attempt (no retry), got {mock.call_count['n']}"
    fake_sleep.assert_not_awaited()
    print("OK test_non_7429_429_returns_immediately")


async def _main() -> int:
    await test_healthy_response_no_retry()
    await test_7429_then_success()
    await test_7429_persistent_exhausts_retries()
    await test_non_7429_429_returns_immediately()
    print("\nAll 4 retry-logic tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
