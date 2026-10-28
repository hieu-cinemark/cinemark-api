"""crawl_jobs.get_running_jobs: Facebook chạy nhiều làn - mỗi làn ghi job đang chạy vào crawl_job:facebook:lane<N>."""

import asyncio
import json

from app.clients.redis import REDIS_KEY_PREFIX
from app.services.crawl import crawl_jobs


class _FakeRedis:
    def __init__(self, data):
        self.data = data

    async def scan_iter(self, match):
        prefix = match.rstrip("*")
        for key in list(self.data):
            if key.startswith(prefix):
                yield key

    async def mget(self, keys):
        return [self.data.get(key) for key in keys]


def test_running_jobs_include_lane_keys(monkeypatch):
    base = f"{REDIS_KEY_PREFIX}crawl_job:facebook"
    fake = _FakeRedis(
        {
            f"{base}:lane1": json.dumps({"run_id": "b", "keyword": "lane 1"}),
            base: json.dumps({"run_id": "a", "keyword": "lane 0"}),
        }
    )
    monkeypatch.setattr(crawl_jobs, "get_redis_client", lambda: fake)
    jobs = asyncio.run(crawl_jobs.get_running_jobs("facebook"))
    assert [job["run_id"] for job in jobs] == ["a", "b"]
    assert asyncio.run(crawl_jobs.get_running_job("facebook"))["run_id"] == "a"


def test_no_running_jobs(monkeypatch):
    monkeypatch.setattr(crawl_jobs, "get_redis_client", lambda: _FakeRedis({}))
    assert asyncio.run(crawl_jobs.get_running_jobs("threads")) == []
    assert asyncio.run(crawl_jobs.get_running_job("threads")) is None
