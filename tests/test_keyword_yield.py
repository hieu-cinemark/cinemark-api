import asyncio

from app.services import keyword_yield as ky


class _FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value


def _run(monkeypatch, yields, keywords, created="2026-09-01T00:00:00+00:00"):
    writes: list[str] = []
    redis = _FakeRedis()

    async def fake_yields(_platform):
        return yields

    async def fake_d1(sql, params=None, **_):
        if sql.startswith("SELECT id, created_at"):
            return [{"id": k["id"], "created_at": created} for k in keywords]
        writes.append(params[0])
        return []

    monkeypatch.setattr(ky, "keyword_yields", fake_yields)
    monkeypatch.setattr(ky, "d1_query", fake_d1)
    monkeypatch.setattr(ky, "get_redis_client", lambda: redis)
    keep, pages = asyncio.run(ky.plan_crawl("threads", keywords))
    return keep, pages, writes, redis


def _kw(kid, movie="m1"):
    return {"id": kid, "movie_id": movie, "platform": "threads", "keyword": kid}


def test_pauses_junk_but_keeps_last_keyword_of_a_film(monkeypatch):
    yields = {
        "junk": {"sample": 500, "ratio": 0.02},
        "good": {"sample": 500, "ratio": 0.9},
        "only": {"sample": 400, "ratio": 0.03},
    }
    keep, pages, writes, redis = _run(monkeypatch, yields, [_kw("junk"), _kw("good"), _kw("only", movie="m2")])
    assert [k["id"] for k in keep] == ["good", "only"]
    assert writes == ["junk"]
    assert "junk" in redis.hashes[ky.PAUSED_KEY]
    assert pages == {"only": ky.LOW_PAGES}  # từ khoá cuối của phim: không tắt, chỉ giảm trang


def test_low_yield_gets_fewer_pages_and_small_samples_are_left_alone(monkeypatch):
    yields = {"low": {"sample": 300, "ratio": 0.25}, "tiny": {"sample": 20, "ratio": 0.0}}
    keep, pages, writes, _ = _run(monkeypatch, yields, [_kw("low"), _kw("tiny")])
    assert len(keep) == 2 and writes == []
    assert pages == {"low": ky.LOW_PAGES}


def test_new_keywords_are_not_judged(monkeypatch):
    from datetime import UTC, datetime

    keep, pages, writes, _ = _run(
        monkeypatch, {"new": {"sample": 900, "ratio": 0.0}}, [_kw("new"), _kw("x")], created=datetime.now(tz=UTC).isoformat()
    )
    assert len(keep) == 2 and writes == [] and pages == {}
