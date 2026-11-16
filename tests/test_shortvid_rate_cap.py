"""Trần request/phút của ShortVid đặt ở tab ShortVid: _Pacer dùng số nhỏ hơn giữa gói và trần, đọc lại trần mỗi phút."""

import asyncio

from app.services.shortvid import shortvid_tiktok

_real_sleep = asyncio.sleep


def _gap_after_two_waits(monkeypatch, plan: int, cap: int) -> float:
    async def fake_cap() -> int:
        return cap

    monkeypatch.setattr(shortvid_tiktok, "shortvid_requests_cap", fake_cap)
    monkeypatch.setattr(shortvid_tiktok.asyncio, "sleep", lambda _s: _real_sleep(0))
    pacer = shortvid_tiktok._Pacer(plan)

    async def run() -> float:
        await pacer.wait()
        first = pacer.next_at
        await pacer.wait()
        return pacer.next_at - first

    return asyncio.run(run())


def test_cap_below_plan_slows_down(monkeypatch):
    assert abs(_gap_after_two_waits(monkeypatch, plan=150, cap=60) - (1.0 + 0.05)) < 1e-6


def test_zero_cap_or_cap_above_plan_uses_plan(monkeypatch):
    assert abs(_gap_after_two_waits(monkeypatch, plan=150, cap=0) - (0.4 + 0.05)) < 1e-6
    assert abs(_gap_after_two_waits(monkeypatch, plan=150, cap=500) - (0.4 + 0.05)) < 1e-6


def test_cap_change_applies_within_a_running_round(monkeypatch):
    caps = iter([0, 30])

    async def fake_cap() -> int:
        return next(caps)

    monkeypatch.setattr(shortvid_tiktok, "shortvid_requests_cap", fake_cap)
    monkeypatch.setattr(shortvid_tiktok, "_CAP_REFRESH_SECONDS", 0.0)
    monkeypatch.setattr(shortvid_tiktok.asyncio, "sleep", lambda _s: _real_sleep(0))
    pacer = shortvid_tiktok._Pacer(150)

    async def run() -> float:
        await pacer.wait()
        first = pacer.next_at
        await pacer.wait()
        return pacer.next_at - first

    assert abs(asyncio.run(run()) - (2.0 + 0.05)) < 1e-6
