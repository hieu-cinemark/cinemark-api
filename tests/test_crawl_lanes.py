import asyncio
import json
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services import crawl_lanes

VN = ZoneInfo("Asia/Ho_Chi_Minh")
TODAY = date(2026, 10, 27)

KEYWORDS = [
    {"id": "showing", "movie_id": "m1", "platform": "facebook", "keyword": "A", "released_at": "2026-10-23"},
    {"id": "upcoming", "movie_id": "m2", "platform": "facebook", "keyword": "B", "released_at": "2026-11-27"},
    {"id": "older", "movie_id": "m3", "platform": "facebook", "keyword": "C", "released_at": "2026-08-01"},
    {"id": "unknown", "movie_id": "m4", "platform": "facebook", "keyword": "D", "released_at": None},
]


class _FakeRedis:
    def __init__(self, states=None, last_queued=None):
        self.kv = {crawl_lanes.DEEP_STATE_KEY.format(keyword_id=k): json.dumps(v) for k, v in (states or {}).items()}
        self.hashes = {crawl_lanes.DEEP_LAST_QUEUED_KEY: dict(last_queued or {})}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    async def mget(self, keys):
        return [self.kv.get(key) for key in keys]

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value


def _patch(monkeypatch, redis):
    published = []

    async def fake_d1(sql, params=None, **_):
        return [dict(k) for k in KEYWORDS]

    async def fake_publish(**kwargs):
        published.append(kwargs)
        return True

    monkeypatch.setattr(crawl_lanes, "d1_query", fake_d1)
    monkeypatch.setattr(crawl_lanes, "publish_crawl_request", fake_publish)
    monkeypatch.setattr(crawl_lanes, "get_redis_client", lambda: redis)
    monkeypatch.setattr(settings, "facebook_crawl_lanes", True)
    monkeypatch.setattr(settings, "facebook_deep_hours", "06-22")
    monkeypatch.setattr(settings, "facebook_deep_jobs_per_account_hour", 1.0)
    monkeypatch.setattr(settings, "facebook_deep_jobs_max_per_hour", 60)
    _accounts(monkeypatch, 10)
    return published


def _accounts(monkeypatch, n):
    async def fake_count(platform):
        return n

    monkeypatch.setattr(crawl_lanes.platform_config_db, "count_crawl_ready_accounts", fake_count)


def test_stage_of():
    assert crawl_lanes.stage_of("2026-10-23", TODAY) == "showing"
    assert crawl_lanes.stage_of("2026-11-27", TODAY) == "upcoming"
    assert crawl_lanes.stage_of("2026-08-01", TODAY) == "older"
    assert crawl_lanes.stage_of(None, TODAY) == "unknown"


def test_fresh_lane_takes_showing_every_slot_and_upcoming_every_other(monkeypatch):
    _patch(monkeypatch, _FakeRedis())
    even = asyncio.run(crawl_lanes.fresh_keywords(0, TODAY))
    odd = asyncio.run(crawl_lanes.fresh_keywords(1, TODAY))
    assert [k["id"] for k in even] == ["showing", "upcoming"]
    assert [k["id"] for k in odd] == ["showing"]


def test_deep_tick_queues_by_stage_once_per_hour(monkeypatch):
    redis = _FakeRedis()
    published = _patch(monkeypatch, redis)
    _accounts(monkeypatch, 2)
    now = datetime(2026, 10, 27, 10, 25, tzinfo=VN)
    assert asyncio.run(crawl_lanes.run_deep_tick(now)) == 2
    assert [p["keyword_id"] for p in published] == ["showing", "upcoming"]
    assert all(p["lane"] == "deep" for p in published)
    assert published[0]["released_at"] == "2026-10-23"
    # Cùng giờ: không xếp lại.
    assert asyncio.run(crawl_lanes.run_deep_tick(now.replace(minute=50))) == 0


def test_deep_tick_respects_minute_and_hours(monkeypatch):
    _patch(monkeypatch, _FakeRedis())
    assert asyncio.run(crawl_lanes.run_deep_tick(datetime(2026, 10, 27, 10, 5, tzinfo=VN))) == 0
    assert asyncio.run(crawl_lanes.run_deep_tick(datetime(2026, 10, 27, 23, 30, tzinfo=VN))) == 0


def test_deep_tick_skips_exhausted_and_recently_queued(monkeypatch):
    redis = _FakeRedis(
        states={"showing": {"done": True, "resweep": "2026-10-26"}, "older": {"done": True, "resweep": "2026-10-10"}},
        last_queued={"upcoming": str(time.time() - 600)},
    )
    published = _patch(monkeypatch, redis)
    asyncio.run(crawl_lanes.run_deep_tick(datetime(2026, 10, 27, 10, 25, tzinfo=VN)))
    # showing: đào xong, vừa quét lại hôm qua; upcoming: vừa xếp 10 phút trước; older: đào xong nhưng tới hạn quét lại.
    assert [p["keyword_id"] for p in published] == ["older", "unknown"]


def test_deep_tick_disabled(monkeypatch):
    _patch(monkeypatch, _FakeRedis())
    monkeypatch.setattr(settings, "facebook_crawl_lanes", False)
    assert asyncio.run(crawl_lanes.run_deep_tick(datetime(2026, 10, 27, 10, 25, tzinfo=VN))) == 0


def test_deep_jobs_scale_with_accounts(monkeypatch):
    monkeypatch.setattr(settings, "facebook_deep_jobs_per_account_hour", 2.0)
    monkeypatch.setattr(settings, "facebook_deep_jobs_max_per_hour", 60)
    for accounts, jobs in ((0, 2), (3, 6), (20, 40), (100, 60)):
        _accounts(monkeypatch, accounts)
        assert asyncio.run(crawl_lanes.deep_jobs_this_hour()) == jobs


def test_facebook_max_pages_by_release_stage():
    assert crawl_lanes.facebook_max_pages("2026-11-27", TODAY) == 50  # chưa chiếu
    assert crawl_lanes.facebook_max_pages("2026-10-23", TODAY) == 100  # đang chiếu
    assert crawl_lanes.facebook_max_pages("2026-08-01", TODAY) == 100  # chiếu lâu rồi
    assert crawl_lanes.facebook_max_pages(None, TODAY) == 100  # chưa có ngày chiếu


def test_deep_jobs_carry_max_pages(monkeypatch):
    published = _patch(monkeypatch, _FakeRedis())
    _accounts(monkeypatch, 2)
    asyncio.run(crawl_lanes.run_deep_tick(datetime(2026, 10, 27, 10, 25, tzinfo=VN)))
    pages = {p["keyword_id"]: p["max_pages"] for p in published}
    assert pages == {"showing": 100, "upcoming": 50}
