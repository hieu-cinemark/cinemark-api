"""Trần request/phút của Omar đặt ở tab Omar: Pacer dùng số nhỏ hơn giữa gói và trần, đọc lại trần mỗi phút."""

import asyncio

from app.services.omar import omar_budget, omar_tiktok

_real_sleep = asyncio.sleep


def _gap_after_two_waits(monkeypatch, plan: int, cap: int) -> float:
    async def fake_cap() -> int:
        return cap

    monkeypatch.setattr(omar_budget, "omar_requests_cap", fake_cap)
    monkeypatch.setattr(omar_tiktok.asyncio, "sleep", lambda _s: _real_sleep(0))
    pacer = omar_budget.Pacer(plan)

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

    monkeypatch.setattr(omar_budget, "omar_requests_cap", fake_cap)
    monkeypatch.setattr(omar_budget, "_CAP_REFRESH_SECONDS", 0.0)
    monkeypatch.setattr(omar_tiktok.asyncio, "sleep", lambda _s: _real_sleep(0))
    pacer = omar_budget.Pacer(150)

    async def run() -> float:
        await pacer.wait()
        first = pacer.next_at
        await pacer.wait()
        return pacer.next_at - first

    assert abs(asyncio.run(run()) - (2.0 + 0.05)) < 1e-6


def test_plan_budget_follows_the_plan_when_no_user_cap():
    day = {"limit": 3000, "used": 500, "remaining": 2500, "resets_in": 3600}
    assert omar_tiktok.plan_budget(day, daily_cap=0, reserve=50, hour_cap=550) == (2450, 3600)


def test_plan_budget_user_daily_cap_lower_than_plan_wins():
    day = {"limit": 3000, "used": 500, "remaining": 2500, "resets_in": 3600}
    # Đặt 1.000 request/ngày, hôm nay đã dùng 500: còn 500, trừ 50 chừa chạy tay.
    assert omar_tiktok.plan_budget(day, daily_cap=1000, reserve=50, hour_cap=550) == (450, 3600)
    # Đã dùng quá số tự đặt thì không còn ngân sách (lượt tạm dừng tới lúc reset).
    assert omar_tiktok.plan_budget({**day, "used": 1200}, daily_cap=1000, reserve=50, hour_cap=550)[0] == 0


def test_plan_budget_user_cap_above_plan_is_ignored():
    day = {"limit": 3000, "used": 2900, "remaining": 100, "resets_in": 60}
    assert omar_tiktok.plan_budget(day, daily_cap=100_000, reserve=50, hour_cap=550) == (50, 60)


def test_plan_budget_without_usage_is_a_quarter_of_the_hourly_cap():
    assert omar_tiktok.plan_budget({}, daily_cap=0, reserve=50, hour_cap=550) == (137, None)
    assert omar_tiktok.plan_budget({}, daily_cap=100, reserve=50, hour_cap=550) == (100, None)
