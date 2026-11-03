import json
from unittest.mock import AsyncMock

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from app.api.routes import platform_scraper
from app.clients import kafka
from app.clients.redis import REDIS_KEY_PREFIX
from app.services.crawl import crawl_jobs


@pytest.mark.parametrize(
    "mode,expected,replies", [(None, "anonymous", True), ("authenticated", "authenticated", False)]
)
async def test_facebook_publisher_sets_explicit_access_without_credentials(monkeypatch, mode, expected, replies):
    publish = AsyncMock(return_value=True)
    monkeypatch.setattr(kafka, "publish_action_request", publish)
    assert await kafka.publish_comments_crawl_request(
        platform="facebook", post_external_id="123", post_url="https://www.facebook.com/x/posts/123", access_mode=mode
    )
    payload = publish.call_args.args[2]
    assert payload["access_mode"] == expected
    assert payload["include_replies"] is replies
    assert not ({"account", "cookies", "fb_dtsg"} & payload.keys())


async def test_threads_publisher_keeps_its_original_contract(monkeypatch):
    publish = AsyncMock(return_value=True)
    monkeypatch.setattr(kafka, "publish_action_request", publish)
    await kafka.publish_comments_crawl_request(
        platform="threads", post_external_id="123", post_url="https://www.threads.com/x/post/123"
    )
    assert "access_mode" not in publish.call_args.args[2]
    assert "include_replies" not in publish.call_args.args[2]


def test_route_accepts_unlimited_anonymous_pages_and_explicit_reply_choice(monkeypatch):
    monkeypatch.setattr(
        platform_scraper,
        "get_post",
        AsyncMock(
            return_value={"platform": "facebook", "external_id": "123", "url": "https://www.facebook.com/x/posts/123"}
        ),
    )
    publish = AsyncMock(return_value=True)
    monkeypatch.setattr(platform_scraper, "publish_comments_crawl_request", publish)
    router = APIRouter()
    platform_scraper.build_comments_run_route(router, "facebook")
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    response = client.post("/posts/post1/comments/run?max_pages=0&access_mode=anonymous&include_replies=false")
    assert response.status_code == 200
    assert publish.call_args.kwargs["max_pages"] == 0
    assert publish.call_args.kwargs["access_mode"] == "anonymous"
    assert publish.call_args.kwargs["include_replies"] is False
    assert client.post("/posts/post1/comments/run?access_mode=authenticated&max_pages=0").status_code == 422


class Redis:
    def __init__(self, data):
        self.data = data

    async def scan_iter(self, match):
        for key in list(self.data):
            if key.startswith(match.rstrip("*")):
                yield key

    async def mget(self, keys):
        return [self.data.get(k) for k in keys]

    async def get(self, key):
        return self.data.get(key)

    async def delete(self, key):
        self.data.pop(key, None)


async def test_public_job_is_visible_and_can_be_removed_by_stop(monkeypatch):
    key = f"{REDIS_KEY_PREFIX}crawl_job:facebook:public"
    fake = Redis({key: json.dumps({"run_id": "public-run", "access_mode": "anonymous", "type": "comments"})})
    monkeypatch.setattr(crawl_jobs, "get_redis_client", lambda: fake)
    assert (await crawl_jobs.get_running_jobs("facebook"))[0]["run_id"] == "public-run"
    assert await crawl_jobs._remove_running_entry("facebook", "public-run")
    assert not fake.data


async def test_anonymous_publication_uses_a_topic_old_account_workers_cannot_consume(monkeypatch):
    producer = AsyncMock()
    monkeypatch.setattr(kafka, "_producer", producer)
    monkeypatch.setattr(kafka, "enqueue_published", AsyncMock())
    assert await kafka.publish_action_request("facebook", "comments", {"access_mode": "anonymous"})
    assert producer.send_and_wait.call_args.args[0] == "crawl_requests.facebook_public_comments"
    assert await kafka.publish_action_request("facebook", "comments", {"access_mode": "authenticated"})
    assert producer.send_and_wait.call_args.args[0] == "crawl_requests"
