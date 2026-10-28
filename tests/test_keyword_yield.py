import asyncio

from app.services.crawl import keyword_yield as ky


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
        "junk": {"sample": 500, "ratio": 0.02, "stored": 60},
        "good": {"sample": 500, "ratio": 0.9, "stored": 450},
        "only": {"sample": 400, "ratio": 0.03, "stored": 40},
    }
    keep, pages, writes, redis = _run(monkeypatch, yields, [_kw("junk"), _kw("good"), _kw("only", movie="m2")])
    assert [k["id"] for k in keep] == ["good", "only"]
    assert writes == ["junk"]
    assert "junk" in redis.hashes[ky.PAUSED_KEY]
    assert pages == {"only": ky.LOW_PAGES}  # từ khoá cuối của phim: không tắt, chỉ giảm trang


def test_low_yield_gets_fewer_pages_and_small_samples_are_left_alone(monkeypatch):
    yields = {"low": {"sample": 300, "ratio": 0.25, "stored": 80}, "tiny": {"sample": 20, "ratio": 0.0, "stored": 20}}
    keep, pages, writes, _ = _run(monkeypatch, yields, [_kw("low"), _kw("tiny")])
    assert len(keep) == 2 and writes == []
    # "tiny" chưa đủ mẫu để tắt, nhưng ít bài -> chỉ vài trang
    assert pages == {"low": ky.LOW_PAGES, "tiny": ky.LOW_VOLUME_PAGES}


def test_new_keywords_are_not_judged(monkeypatch):
    from datetime import UTC, datetime

    keep, pages, writes, _ = _run(
        monkeypatch, {"new": {"sample": 900, "ratio": 0.0}}, [_kw("new"), _kw("x")], created=datetime.now(tz=UTC).isoformat()
    )
    assert len(keep) == 2 and writes == [] and pages == {}


def test_low_volume_keyword_gets_few_pages_even_if_relevant(monkeypatch):
    yields = {"rare": {"sample": 200, "ratio": 0.9, "stored": 12}}
    keep, pages, _, _ = _run(monkeypatch, yields, [_kw("rare")])
    assert [k["id"] for k in keep] == ["rare"] and pages == {"rare": ky.LOW_VOLUME_PAGES}


def test_zero_yield_keywords_only_crawled_on_their_turn(monkeypatch):
    from datetime import UTC, datetime, timedelta

    ids = [f"z{i}" for i in range(30)]
    now = datetime.now(tz=UTC)
    on_turn = {kid for kid in ids if ky._zero_yield_turn(kid, now)}
    keep, pages, _, _ = _run(monkeypatch, {}, [_kw(kid) for kid in ids])
    assert {k["id"] for k in keep} == on_turn
    assert 0 < len(on_turn) < len(ids)
    assert all(pages[kid] == ky.LOW_VOLUME_PAGES for kid in on_turn)
    # mỗi từ khoá được cào đúng một lần trong ZERO_EVERY_DAYS ngày liên tiếp
    for kid in ids:
        turns = sum(ky._zero_yield_turn(kid, now + timedelta(days=d)) for d in range(ky.ZERO_EVERY_DAYS))
        assert turns == 1


def test_high_yield_keywords_queued_first(monkeypatch):
    yields = {
        "few": {"sample": 300, "ratio": 0.9, "stored": 200, "relevant": 180},
        "many": {"sample": 900, "ratio": 0.9, "stored": 800, "relevant": 720},
    }
    keep, _, _, _ = _run(monkeypatch, yields, [_kw("few"), _kw("many")])
    assert [k["id"] for k in keep] == ["many", "few"]
