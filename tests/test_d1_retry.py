"""Unit tests for the Cloudflare D1 storage-backoff retry in d1_client.

Covers the four scenarios that drove the operator's reported failure
(scheduled_crawl_firing -> d1_request_failed status=429 code 7429
cascading for ~60s):

  1. Healthy response (status 200) returns rows, no retry.
  2. Transient 7429 then success: retries with exponential backoff and
     returns rows.
  3. Persistent 7429 past max_retries: returns None after the full
     retry budget is exhausted, with a final warning log.
  4. Non-7429 429 (e.g. a real rate-limit): returns None immediately,
     no retry - the existing caller paths treat None as failure and
     we don't want to mask a real quota problem behind a silent retry
     loop.

Mocks httpx.AsyncClient.post directly so the tests run without network
access. Each test asserts both the returned value AND that the
expected number of HTTP calls was made.
"""

from __future__ import annotations

import asyncio

# Make `from app.clients.d1 import d1_query` work without
# triggering app.* import side effects (settings, logging config, ...)
# by inserting the repo root on sys.path before importing.
import os
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.clients import d1 as d1_client  # noqa: E402


class _FakeResp:
    """Just enough of httpx.Response for d1_query's reads:
    .status_code, .text, .json()."""

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
    """Returns an AsyncMock standing in for httpx.AsyncClient whose
    `.post()` returns each response in `responses` in order. Tracks
    call count via a plain list so tests can assert how many HTTP
    calls were made - using `mock.post.await_count` requires mock.post
    to itself be a Mock attribute, but we override it with a real
    function (to consume the iterator), so we keep a separate counter.
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
    # The d1_query enters _remote_sem; patch it to a no-op so the test
    # doesn't actually run the semaphore (which is module-level and
    # shared across tests).
    return mock


async def _run_with_client(mock_client: AsyncMock) -> list[dict[str, Any]] | None:
    """Calls d1_query after wiring the module's _get_http_client to
    return our mock. Stubs out the remote semaphore so a test run
    doesn't share state with other tests."""
    sem = asyncio.Semaphore(8)

    async def _fake_sem() -> Any:
        async with sem:
            yield

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=None)
    cm.__aexit__ = AsyncMock(return_value=None)

    with patch.object(d1_client, "_get_http_client", AsyncMock(return_value=mock_client)), \
         patch.object(d1_client, "_remote_sem", cm), \
         patch.object(d1_client, "_configured", return_value=True), \
         patch.object(d1_client, "_logged_db_target", True, create=True):
        # Reset the module's one-time info-log flag so it doesn't
        # silently short-circuit our patched _get_http_client on the
        # second test run.
        d1_client._logged_db_target = True
        return await d1_client.d1_query("SELECT 1", [], quiet=True)


async def test_healthy_response_no_retry() -> None:
    """One 200 with rows -> d1_query returns the rows, makes exactly
    one HTTP call. Regression guard for "retry logic accidentally
    doubles the load on healthy requests"."""
    rows = [{"results": [{"n": 1}]}]
    mock = _make_client_mock([_FakeResp(200, {"success": True, "result": rows})])
    out = await _run_with_client(mock)
    assert out == [{"n": 1}], f"expected rows, got {out}"
    assert mock.call_count["n"] == 1
    print("OK test_healthy_response_no_retry")


async def test_7429_then_success() -> None:
    """One 429-with-code-7429, then a 200. d1_query should sleep once
    (base = 2s) and return rows. Total HTTP calls = 2."""
    mock = _make_client_mock(
        [
            _FakeResp(429, '{"success":false,"errors":[{"code":7429,"message":"..."}]}'),
            _FakeResp(200, {"success": True, "result": [{"results": [{"n": 7}]}]}),
        ]
    )
    # Patch asyncio.sleep so the test doesn't actually pause 2s.
    with patch("asyncio.sleep", new=AsyncMock()) as fake_sleep:
        out = await _run_with_client(mock)
    assert out == [{"n": 7}], f"expected rows after retry, got {out}"
    assert mock.call_count["n"] == 2, f"expected 2 calls, got {mock.call_count['n']}"
    fake_sleep.assert_awaited_once()
    print("OK test_7429_then_success")


async def test_7429_persistent_exhausts_retries() -> None:
    """All 5 attempts return 7429. d1_query should give up after the
    4th retry (default max_retries=4 -> 5 total attempts: 1 initial +
    4 retries) and return None. Sleep count should be 4 (one per
    retry), with backoff doubles 2s, 4s, 8s, 16s."""
    bodies = [
        _FakeResp(429, '{"success":false,"errors":[{"code":7429,"message":"..."}]}')
    ] * 5
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
    """A 429 with a different body (e.g. real Cloudflare rate-limit,
    no 7429 code) should NOT retry - the existing failure-handling
    paths treat None as failure and we don't want to silently mask a
    quota problem behind a retry loop."""
    mock = _make_client_mock(
        [_FakeResp(429, '{"success":false,"errors":[{"code":10000,"message":"Rate limit"}]}')]
    )
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
