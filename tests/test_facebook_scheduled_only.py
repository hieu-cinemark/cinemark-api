import asyncio
from unittest.mock import AsyncMock

import pytest

from app.services.crawl import scheduler


@pytest.mark.parametrize(
    "background,enabled,expected",
    [(False, True, False), (False, False, False), (True, False, False), (True, True, True)],
)
def test_background_work_needs_both_switches(monkeypatch, background, enabled, expected):
    monkeypatch.setattr(scheduler.settings, "facebook_background_crawl_enabled", background)
    schedules = AsyncMock(return_value=[{"platform": "facebook", "enabled": enabled}])
    monkeypatch.setattr(scheduler.db, "list_crawl_schedules", schedules)
    assert asyncio.run(scheduler._facebook_crawl_enabled()) is expected
    assert schedules.call_count == int(background)


def test_disabled_background_does_not_disable_the_explicit_daily_slot(monkeypatch):
    monkeypatch.setattr(scheduler.settings, "facebook_background_crawl_enabled", False)
    monkeypatch.setattr(scheduler, "datetime", AtSlot)
    monkeypatch.setattr(
        scheduler.db,
        "list_crawl_schedules",
        AsyncMock(
            return_value=[
                {
                    "platform": "facebook",
                    "enabled": True,
                    "run_time": "19:30",
                    "nurture_before": False,
                    "nurture_after": False,
                }
            ]
        ),
    )
    claim = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(scheduler, "_claim_crawl_slot", claim)
    jobs = []
    monkeypatch.setattr(scheduler, "_spawn", lambda coro: (jobs.append(coro), coro.close()))
    asyncio.run(scheduler._tick())
    asyncio.run(scheduler._tick())
    assert len(jobs) == 1
    assert claim.call_args.args == ("facebook", "2026-11-04", "19:30")


class AtSlot(scheduler.datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 4, 19, 30, tzinfo=tz)
