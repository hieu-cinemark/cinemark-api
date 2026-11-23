"""Đặc tả hành vi của run_omar_round (lượt cào hashtag TikTok qua Omar, gọi API trả phí): chạy với các phụ thuộc giả để mọi
thay đổi cấu trúc hàm này phải giữ nguyên số request, thống kê, trạng thái lưu Redis và các đường bỏ qua."""

import asyncio
import json
from unittest.mock import AsyncMock

from app.services.omar import omar_tiktok as ot

RAW = {"id": "v"}


class FakeRedis:
    def __init__(self, state=None):
        self.state = dict(state or {})
        self.writes: list[tuple[str, str]] = []

    async def hgetall(self, key):
        return {k: v for k, v in self.state.items()}

    async def hset(self, key, field, value):
        self.state[field] = value
        self.writes.append((field, value))

    async def set(self, *a, **k):
        return True


class FakeClient:
    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _setup(monkeypatch, *, api_key="key", keywords=None, usage=None, pages=None, redis=None):
    redis = redis or FakeRedis()
    keywords = (
        keywords
        if keywords is not None
        else [
            {"id": "kw1", "keyword": "#alpha", "movie_id": "m1"},
            {"id": "kw2", "keyword": "#beta", "movie_id": "m2"},
        ]
    )
    pages = pages or {
        ("alpha", "0"): ([RAW], "c1", "h1"),
        ("alpha", "c1"): ([RAW], None, "h1"),
        ("beta", "0"): ([RAW], None, "h2"),
    }
    calls: list[tuple[str, str]] = []

    async def fetch_page(client, pacer, key, tag, cursor, hashtag_id=None):
        calls.append((tag, cursor))
        return pages[(tag, cursor)]

    async def publish(videos, kw_id, stats):
        stats["published"] = stats.get("published", 0) + len(videos)

    pause = AsyncMock()
    monkeypatch.setattr(ot, "get_omar_api_settings", AsyncMock(return_value={"api_key": api_key}))
    monkeypatch.setattr(ot, "get_enabled_keywords", AsyncMock(return_value=keywords))
    monkeypatch.setattr(ot, "get_redis_client", lambda: redis)
    monkeypatch.setattr(ot.httpx, "AsyncClient", FakeClient)
    monkeypatch.setattr(
        ot,
        "fetch_usage",
        AsyncMock(
            return_value=usage or {"day": {"remaining": 100, "resets_in": 3600, "used": 0}, "minute": {"limit": 600}}
        ),
    )
    monkeypatch.setattr(ot, "omar_limits", AsyncMock(return_value={"daily_requests": 0, "concurrency": 2}))
    monkeypatch.setattr(ot, "omar_requests_cap", AsyncMock(return_value=0))
    monkeypatch.setattr(ot, "_movie_priority", AsyncMock(return_value={"m1": 1.0, "m2": 5.0}))
    monkeypatch.setattr(ot, "fetch_page", fetch_page)
    monkeypatch.setattr(ot, "map_video", lambda raw: raw)
    monkeypatch.setattr(ot, "count_cooccur", AsyncMock())
    monkeypatch.setattr(ot, "publish_videos", publish)
    monkeypatch.setattr(ot, "fetch_hashtag_info", AsyncMock(return_value={"video_count": 10}))
    monkeypatch.setattr(ot.omar_comments, "process_queue", AsyncMock())
    monkeypatch.setattr(ot, "pause_after_round", pause)
    monkeypatch.setattr(ot, "_KEYWORD_GAP_SECONDS", 0.0)
    monkeypatch.setattr(ot, "_stop_requested", False)
    monkeypatch.setattr(ot, "manual_running", set())
    return redis, calls, pause


def run(live=None):
    return asyncio.run(ot.run_omar_round(live))


def test_new_hashtags_are_crawled_to_the_end_and_state_is_saved(monkeypatch):
    redis, calls, pause = _setup(monkeypatch)
    result = run()
    assert sorted(calls) == [("alpha", "0"), ("alpha", "c1"), ("beta", "0")]
    assert result["finished"] == 2 and result["videos"] == 3 and result["published"] == 3
    assert result["keywords"] == 2 and result["pending_keywords"] == 0 and result["failed"] == 0
    # 3 trang + 2 lần làm mới hashtag_info; mỗi request trừ một đơn vị ngân sách
    assert result["requests"] == 5 and result["budget"] == 50  # 100 còn lại - 50 dự trữ chạy tay
    state = {k: json.loads(v) for k, v in redis.state.items()}
    assert state["kw1"]["done_at"] and state["kw1"]["cursor"] is None and state["kw1"]["pages"] == 2
    assert state["kw2"]["done_at"] and state["kw2"]["pages"] == 1
    assert state["kw1"]["video_count"] == 10 and state["kw1"]["hashtag_id"] == "h1"
    pause.assert_awaited_once()


def test_live_stats_dict_is_updated_in_place(monkeypatch):
    _setup(monkeypatch)
    live: dict = {}
    result = run(live)
    assert live["finished"] == 2 and live["requests"] == result["requests"]


def test_without_key_or_keywords_the_round_is_skipped(monkeypatch):
    _setup(monkeypatch, api_key="")
    assert run()["skipped"] == "no_key"
    _setup(monkeypatch, keywords=[{"id": "kw9", "keyword": "plain-no-hash", "movie_id": "m"}])
    assert run()["skipped"] == "no_keywords"


def test_no_budget_skips_the_round_and_sets_a_quota_pause(monkeypatch):
    _setup(monkeypatch, usage={"day": {"remaining": 10, "resets_in": 600, "used": 90}, "minute": {"limit": 600}})
    set_pause = AsyncMock()
    monkeypatch.setattr(ot, "set_pause", set_pause)
    monkeypatch.setattr(ot.settings, "omar_daily_reserve", 50)
    result = run()
    assert result["skipped"] == "no_budget"
    assert set_pause.await_args.args[0] == "quota"


def test_finished_hashtag_is_not_recrawled_before_the_recrawl_gap(monkeypatch):
    from datetime import UTC, datetime

    done = {
        "kw1": json.dumps(
            {"done_at": datetime.now(tz=UTC).isoformat(), "pages": 4, "info_at": datetime.now(tz=UTC).isoformat()}
        )
    }
    _redis, calls, _ = _setup(monkeypatch, redis=FakeRedis(done))
    run()
    assert ("alpha", "0") not in calls and ("beta", "0") in calls


def test_first_page_error_on_a_never_crawled_hashtag_marks_it_missing(monkeypatch):
    """Hashtag chưa từng lấy được trang nào mà trang 1 lỗi = không có trên TikTok: đánh dấu missing, kiểm tra lại sau."""
    redis, calls, _ = _setup(monkeypatch)

    async def flaky(client, pacer, key, tag, cursor, hashtag_id=None):
        calls.append((tag, cursor))
        if tag == "beta":
            raise RuntimeError("boom")
        return ([RAW], None, "h1")

    monkeypatch.setattr(ot, "fetch_page", flaky)
    result = run()
    assert result["failed"] == 1 and result["missing"] == 1 and result["finished"] == 1
    state = {k: json.loads(v) for k, v in redis.state.items()}
    assert state["kw2"]["missing_at"] and "done_at" not in state["kw2"]
    assert state["kw1"]["done_at"]


def test_omar_stop_halts_the_round_and_is_reported(monkeypatch):
    _setup(monkeypatch)

    async def stop(*a, **k):
        raise ot.OmarStop("daily_limit: Daily limit reached")

    monkeypatch.setattr(ot, "fetch_page", stop)
    result = run()
    assert result["stopped"].startswith("daily_limit") and result["finished"] == 0
