"""Tên hàng đợi Redis ở cinemark-api (app/clients/crawl_queues.py) phải khớp spider-hub - lệch là job nằm im không ai đọc.

Đọc mã nguồn spider-hub nằm cạnh repo này (../spider-hub); không có thì bỏ qua (CI một repo).
"""

from pathlib import Path

import pytest

from app.clients import crawl_queues as q
from app.clients.redis import REDIS_KEY_PREFIX

HUB = Path(__file__).resolve().parents[2] / "spider-hub" / "social_crawler"

# khoá cinemark-api -> (file spider-hub, chuỗi phải có trong file)
EXPECTED = {
    q.PRIORITY_FACEBOOK: ("consumer/loop.py", 'PRIORITY_QUEUE_KEY_TMPL = "crawl_priority:{platform}"'),
    q.IDLE_FACEBOOK: ("consumer/loop.py", 'IDLE_QUEUE_KEY_TMPL = "crawl_idle:{platform}"'),
    q.PUBLIC_PAGES_FACEBOOK: ("consumer/loop.py", 'PUBLIC_PAGES_QUEUE_KEY = "crawl_public_pages:facebook"'),
    q.PUBLIC_PAGES_THREADS: ("consumer/jobs/threads_channel.py", 'QUEUE_KEY = "crawl_public_pages:threads"'),
    q.THREADS_PROFILES: ("constants/threads.py", 'PROFILE_QUEUE_REDIS_KEY = "threads:profile_queue"'),
}


@pytest.mark.skipif(not HUB.is_dir(), reason="không có repo spider-hub cạnh cinemark-api")
@pytest.mark.parametrize("key", list(EXPECTED))
def test_queue_key_matches_spider_hub(key: str) -> None:
    path, needle = EXPECTED[key]
    assert needle in (HUB / path).read_text(encoding="utf-8")
    # chuỗi trong spider-hub là phần sau tiền tố Redis
    bare = key.removeprefix(REDIS_KEY_PREFIX)
    assert bare in needle.replace("{platform}", "facebook")


@pytest.mark.skipif(not HUB.is_dir(), reason="không có repo spider-hub cạnh cinemark-api")
def test_youtube_queue_keys_match_spider_hub() -> None:
    text = (HUB / "consumer/jobs/youtube.py").read_text(encoding="utf-8")
    for key in q.YOUTUBE_QUEUE_KEYS:
        assert key.removeprefix(REDIS_KEY_PREFIX) in text
