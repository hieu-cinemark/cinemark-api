"""Job AI chuyển từ Worker (app/services/ai_jobs): tách kênh giống bản TS, kiểm tra câu trả lời AI, một lượt Review Angles."""

import json

import pytest

from app.ai.tasks import review_angles as tasks
from app.repositories.d1 import sql as d1_sql
from app.services.ai_jobs import review_angles
from app.services.ai_jobs.channels import channel_of_post, channel_url, extract_handle


@pytest.mark.parametrize(
    "platform,author,url,handle",
    [
        ("tiktok", "Ai Đó", "https://www.tiktok.com/@Khen.Phim/video/1", "khen.phim"),
        ("threads", "x", "https://www.threads.net/@abc?x=1", "abc"),
        ("tiktok", "Tên Hiển Thị", "", "tên hiển thị"),
        ("facebook", "Page", "https://www.facebook.com/ChiKhenPhimHay/posts/123", "chikhenphimhay"),
        ("facebook", "Page", "https://www.facebook.com/profile.php?id=1000123", "1000123"),
        ("facebook", "Fun Entertainment", "https://www.facebook.com/reel/1271689048497785/", "fun entertainment"),
    ],
)
def test_extract_handle_matches_worker(platform, author, url, handle):
    assert extract_handle(platform, author, url) == handle


def test_channel_of_post_groups_and_urls():
    group = channel_of_post(
        {"platform": "facebook", "author": "Ai", "url": "https://web.facebook.com/groups/ReviewPhim/posts/1"}
    )
    assert group == {"kind": "group", "handle": "groups/reviewphim"}
    assert channel_url("facebook", "group", "groups/reviewphim") == "https://www.facebook.com/groups/reviewphim"
    assert channel_url("facebook", "page", "123") == "https://www.facebook.com/profile.php?id=123"
    assert channel_url("tiktok", "creator", "abc") == "https://www.tiktok.com/@abc"


def test_fold_and_angle_id_match_worker():
    assert tasks.fold("  Đạo   DIỄN ") == "dao dien"
    # Giá trị tính bằng bản TS (sha256(JSON.stringify({aspect, name: fold(name)})).slice(0, 24)).
    assert tasks.angle_id_of("dien_xuat", "Diễn xuất  Đỉnh của Thu Trang") == "37cf08dff64b0747db06f15d"


def test_has_text_and_premiere_start():
    assert not tasks.has_text("https://x.com #CCEE3 @abc 🔥🔥")
    assert tasks.has_text("Phim hay #CCEE3")
    assert review_angles.premiere_start("2026-11-03") == "2026-11-02T17:00:00+00:00"


POSTS = [
    {"id": "p1", "caption": "Diễn xuất của Thu Trang rất tự nhiên, tiếng cười đến đều."},
    {"id": "p2", "caption": "Kịch bản đoạn giữa hơi lê thê nhưng cái kết cảm động."},
]


def _angle(**extra):
    base = {
        "name": "Diễn xuất tự nhiên",
        "description": "Khen diễn xuất",
        "aspect": "dien_xuat",
        "evidence": [{"postId": "p1", "quote": "Diễn xuất của Thu Trang rất tự nhiên"}],
    }
    return {**base, **extra}


def test_parse_batch_rules():
    ok = tasks.parse_batch({"summary": " Tóm tắt. ", "angles": [_angle()]}, POSTS, [])
    assert ok["summary"] == "Tóm tắt." and ok["angles"][0]["evidence"][0]["postId"] == "p1"
    invented = _angle(evidence=[{"postId": "p1", "quote": "Diễn xuất đỉnh nhất năm nay"}])
    assert tasks.parse_batch({"summary": "x", "angles": [invented]}, POSTS, []) is None
    assert tasks.parse_batch({"summary": "x", "angles": [_angle(aspect="doanh_thu")]}, POSTS, []) is None
    quote = {"postId": "p2", "quote": "Kịch bản đoạn giữa hơi lê thê"}
    three = [_angle(name=f"Góc {n}", aspect="kich_ban", evidence=[quote]) for n in "abc"]
    assert tasks.parse_batch({"summary": "x", "angles": three}, POSTS, []) is None
    catalog = [{"id": "abc", "name": "Diễn xuất tự nhiên", "aspect": "dien_xuat"}]
    reused = tasks.parse_batch({"summary": "x", "angles": [_angle(name="diễn xuất  TỰ NHIÊN")]}, POSTS, catalog)
    assert reused["angles"][0]["id"] == "abc"
    assert tasks.parse_batch({"summary": "x", "angles": [_angle(id="zzz")]}, POSTS, catalog) is None


class FakeRedis:
    def __init__(self):
        self.data = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return None
        self.data[key] = str(value)
        return True

    async def get(self, key):
        return self.data.get(key)

    async def delete(self, key):
        self.data.pop(key, None)

    async def exists(self, key):
        return int(key in self.data)

    async def incrby(self, key, n):
        self.data[key] = str(int(self.data.get(key, 0)) + n)
        return int(self.data[key])

    async def incr(self, key):
        return await self.incrby(key, 1)

    async def expire(self, key, ttl):
        return True


@pytest.fixture
def world(monkeypatch):
    redis, lake, calls = FakeRedis(), {}, []
    rows = [
        {
            "id": f"p{i}",
            "platform": "facebook" if i % 2 else "tiktok",
            "posted_at": f"2026-11-03T0{i}:00:00+00:00",
            "author": f"Kênh {i}",
            "url": f"https://www.tiktok.com/@kenh{i}/video/{i}",
            "content": "Diễn xuất của Thu Trang rất tự nhiên." if i != 3 else "#bosuabay 🔥",
            "like_count": i,
        }
        for i in range(1, 5)
    ]

    async def fake_d1(sql, params=None, **kwargs):
        if "FROM movies AS m" in sql and "LIMIT" in sql:
            return [{"title": "Phim", "released_at": "2026-11-06", "premiere_date": "2026-11-03"}]
        if "json_each" in sql:
            wanted = set(json.loads(params[0]))
            return [r for r in rows if r["id"] in wanted]
        if "p.posted_at >= ?" in sql:
            since = next(v for v in params if isinstance(v, str) and v[:4].isdigit())
            return [{k: r[k] for k in ("id", "platform", "posted_at")} for r in rows if r["posted_at"] >= since]
        return []

    async def get_object(key):
        if key not in lake:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return lake[key]

    async def put_object(key, body, content_type="x"):
        lake[key] = body

    async def fake_ai(**kwargs):
        calls.append(kwargs)
        posts = json.loads(kwargs["user_prompt"])["posts"]
        first = next(p for p in posts if "Diễn xuất" in p["caption"])
        return {
            "summary": "Ok.",
            "angles": [
                {
                    "name": "Diễn xuất tự nhiên",
                    "description": "d",
                    "aspect": "dien_xuat",
                    "evidence": [{"postId": first["id"], "quote": "Diễn xuất của Thu Trang"}],
                }
            ],
        }, "{}"

    monkeypatch.setattr(review_angles, "get_redis_client", lambda: redis)
    monkeypatch.setattr(d1_sql, "d1_query", fake_d1)
    monkeypatch.setattr(review_angles.lake, "get_object", get_object)
    monkeypatch.setattr(review_angles.lake, "put_object", put_object)
    monkeypatch.setattr(review_angles, "call_llm_json", fake_ai)
    monkeypatch.setattr(review_angles, "vn_day", lambda at=None: "2026-11-10")
    review_angles._movies.clear()
    return redis, lake, calls


async def test_batch_writes_state_in_worker_format_and_advances(world):
    redis, lake, calls = world
    result = await review_angles.analyze_next_batch("m1")
    assert result["read"] == 4 and result["analyzed"] == 3 and result["ai_called"]
    state = json.loads(lake["analysis/review-angles/v2/m1/state.json"])
    assert state["version"] == 2 and state["cursor"] == "2026-11-03T04:00:00+00:00"
    assert sum(state["analyzed"].values()) == 4
    angle = state["angles"][0]
    assert angle["id"] == tasks.angle_id_of("dien_xuat", "Diễn xuất tự nhiên")
    assert set(angle["refs"][0]) == {"id", "page", "platform", "source", "interactions", "views", "quote"}
    page = json.loads(lake["analysis/review-angles/v2/m1/page-0.json"])
    assert {p["id"] for p in page} == {"p1", "p2", "p4"}  # p3 chỉ có hashtag/emoji: đã đọc, không gửi AI
    assert page[0]["imageUrl"].startswith("/api/social-topic/review-angles/image?movieId=m1&postId=")
    assert "p3" not in json.loads(calls[0]["user_prompt"])["posts"].__repr__()
    again = await review_angles.analyze_next_batch("m1")
    assert again["read"] == 0 and len(calls) == 1  # hết bài mới, không gọi AI lại
    assert await redis.get("social_crawler:review_angles:lock:m1") is None


async def test_unparsed_answer_retries_then_skips(world, monkeypatch):
    _redis, lake, calls = world

    async def bad_ai(**kwargs):
        calls.append(kwargs)
        return {
            "summary": "x",
            "angles": [
                {
                    "name": "bịa",
                    "description": "d",
                    "aspect": "dien_xuat",
                    "evidence": [{"postId": "p1", "quote": "không có trong caption"}],
                }
            ],
        }, "{}"

    monkeypatch.setattr(review_angles, "call_llm_json", bad_ai)
    for _ in range(2):
        result = await review_angles.analyze_next_batch("m1")
        assert result["reason"] == "ai_unparsed" and result["read"] == 0
    third = await review_angles.analyze_next_batch("m1")
    assert third["read"] == 4  # hỏng lần 3: bỏ qua lô, phim không kẹt
    state = json.loads(lake["analysis/review-angles/v2/m1/state.json"])
    assert state["angles"] == [] and state["failures"] == {}


async def test_focus_mode_runs_until_done_and_clears_key(world):
    redis, _lake, _calls = world
    await redis.set(review_angles.REVIEW_FOCUS_KEY, json.dumps({"movieId": "m1", "tokenBudget": 10_000_000}))
    result = await review_angles.run_tick()
    assert result["stopped"] == "done" and result["read"] == 4
    assert not await redis.exists(review_angles.REVIEW_FOCUS_KEY)
    assert int(await redis.get("social_crawler:review_angles:tokens:m1")) > 0


def test_build_context_numbers_documents_and_respects_limit():
    from app.ai.retrieval import Document, ScoredDocument, build_context

    docs = [ScoredDocument(Document(id=str(i), text=f"Đoạn {i} " + "x" * 40), 1.0 - i / 10) for i in range(5)]
    context = build_context(docs, max_chars=120)
    assert context.startswith("[1] Đoạn 0") and "[2] Đoạn 1" in context and "[3]" not in context
