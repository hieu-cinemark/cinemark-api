"""Mốc tính lại của attack_detect --backfill-days: đều theo bước, bỏ mốc đã có dòng gần đó."""

from datetime import UTC, datetime, timedelta

from app.services.ai_jobs.attack_detect import backfill_points

T0 = datetime(2026, 11, 1, tzinfo=UTC)
STEP = timedelta(hours=6)


def test_points_every_step():
    assert backfill_points(T0, T0 + timedelta(hours=18), STEP, []) == [T0 + STEP * i for i in range(4)]


def test_skips_points_near_existing_rows():
    done = [T0 + timedelta(hours=6, minutes=40)]  # một nhịp thật lúc 06:40 -> bỏ mốc 06:00
    assert backfill_points(T0, T0 + timedelta(hours=12), STEP, done) == [T0, T0 + timedelta(hours=12)]
