"""Attack Detection: dấu hiệu A/B, quy tắc trạng thái, đọc embedding, bảng attack_signals (SQLite trong bộ nhớ)."""

import asyncio
import sqlite3

import numpy as np
from sqlalchemy import create_engine, select

from app.services.attack import data, store
from app.services.attack.signals import Signal, mask_name
from app.services.attack.similar import normalize, similar_clusters, similar_signal
from app.services.attack.spikes import find_spikes, spike_signal
from app.services.attack.verdict import decide

# --- A: chê dồn dập ---


def _hours(days, spike_at=None, spike=0, total=100):
    rows = []
    for d in range(days):
        for h in range(24):
            hour = f"2026-11-{1 + d:02d}T{h:02d}"
            rows.append({"hour": hour, "total": total, "negative": 10 + (spike if hour == spike_at else 0)})
    return rows


def test_spike_found_and_strong():
    spikes = find_spikes(_hours(16, "2026-11-16T20", 60), [])
    assert [s["hour"] for s in spikes] == ["2026-11-16T20"] and spikes[0]["strong"]
    assert spikes[0]["multiple"] == 7.0


def test_spike_near_event_is_not_strong():
    spikes = find_spikes(_hours(16, "2026-11-16T20", 60), ["2026-11-16T18"])
    assert spikes and not spikes[0]["strong"]


def test_viral_hour_with_same_share_is_not_a_spike():
    hours = _hours(16)
    hours[-4] = {**hours[-4], "total": 1000, "negative": 100}  # tổng tăng 10 lần, tỷ lệ chê vẫn 10%
    assert find_spikes(hours, []) == []


def test_below_min_count_is_ignored():
    assert find_spikes(_hours(16, "2026-11-16T20", 15), []) == []  # 25 < 30


def test_spike_signal_only_counts_recent_window():
    hours = _hours(16, "2026-11-15T10", 60)
    assert not spike_signal(hours, [], since_hour="2026-11-16T00").fired
    signal = spike_signal(hours, [], since_hour="2026-11-15T00")
    assert signal.fired and signal.window == ("2026-11-15T10", "2026-11-15T11")


# --- B: bình luận chê giống nhau ---


def _row(i, text, author=None, minute=0):
    return {
        "id": f"c{i}",
        "author_id": author or f"a{i}",
        "author_name": "Nguyễn Văn An",
        "post_id": f"p{i % 3}",
        "post_url": "u",
        "platform": "facebook",
        "posted_at": f"2026-11-16T10:{minute:02d}:00+00:00",
        "message": text,
        "likes": 1,
    }


def test_normalize_strips_accents_tags_and_punctuation():
    assert normalize("Phim DỞ quá!!! @ban https://x.y 😡") == "phim do qua"


def test_copy_paste_campaign_is_a_strong_cluster():
    rows = [_row(i, "Phim dở tệ, đừng ai đi xem, phí tiền!", minute=i) for i in range(20)]
    rows += [_row(100 + i, f"ý kiến riêng số {i} về phim này khá ổn") for i in range(10)]
    clusters = similar_clusters(rows, None)
    assert len(clusters) == 1
    c = clusters[0]
    assert (c["size"], c["accounts"], c["burst"], c["strong"]) == (20, 20, 1.0, True)
    assert c["evidence"][0]["author"] == "Ng*** V* A*"


def test_same_account_repeating_is_not_strong():
    rows = [_row(i, "Phim dở tệ, đừng ai đi xem, phí tiền!", author="one", minute=i) for i in range(20)]
    assert not similar_clusters(rows, None)[0]["strong"]  # 1 tài khoản


def test_cosine_links_paraphrases():
    rng = np.random.default_rng(0)
    base = rng.normal(size=8)
    vecs = np.stack([base + rng.normal(scale=0.01, size=8) for _ in range(6)] + [rng.normal(size=8) for _ in range(4)])
    vecs = vecs / np.linalg.norm(vecs, axis=1, keepdims=True)
    rows = [_row(i, f"câu chê khác chữ số {i} nhưng cùng ý") for i in range(10)]
    clusters = similar_clusters(rows, vecs.astype(np.float32))
    assert [c["size"] for c in clusters] == [6]


def test_similar_signal_window_in_vn_hours():
    rows = [_row(i, "Phim dở tệ, đừng ai đi xem, phí tiền!", minute=i) for i in range(20)]
    signal = similar_signal(rows, None)
    assert signal.fired and signal.strong and signal.window == ("2026-11-16T17", "2026-11-16T19")


# --- trạng thái ---


def _sig(key, fired=True, strong=True, window=("2026-11-16T17", "2026-11-16T19")):
    return Signal(key=key, fired=fired, strong=strong, score=5.0, window=window, detail={"clusters": [{"size": 9}]})


def test_decide_states():
    ok = {"comments_7d": 3000, "platforms": 2}
    assert decide([], {"comments_7d": 50})[0] == "insufficient"
    assert decide([_sig("spike", fired=False, strong=False)], ok)[:2] == ("clear", "high")
    assert decide([_sig("spike", strong=False)], ok)[0] == "clear"  # có biến động nhưng không mạnh
    assert decide([_sig("spike")], ok)[0] == "suspicious"
    assert decide([_sig("spike"), _sig("similar")], ok)[0] == "coordinated"


def test_two_strong_signals_at_different_times_stay_suspicious():
    late = ("2026-11-17T08", "2026-11-17T09")
    assert decide([_sig("spike"), _sig("similar", window=late)], {"comments_7d": 3000})[0] == "suspicious"


def test_mask_name():
    assert mask_name("Lê A") == "L* A*"
    assert mask_name(None) == "Ẩn danh"


# --- dữ liệu ---


def test_load_vectors_keeps_order_and_zero_fills(tmp_path):
    path = tmp_path / "e.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE comment_embeddings (comment_id TEXT PRIMARY KEY, movie_id TEXT, embedding BLOB)")
    vec = np.ones(data.EMBED_DIM, dtype=np.float16)
    conn.execute("INSERT INTO comment_embeddings VALUES ('b', 'm', ?)", (vec.tobytes(),))
    conn.commit()
    conn.close()
    out = data.load_vectors(["a", "b"], path)
    assert out.shape == (2, data.EMBED_DIM) and out[0].sum() == 0 and out[1].sum() == data.EMBED_DIM
    assert data.load_vectors(["a"], tmp_path / "missing.sqlite") is None


def test_coverage():
    hours = _hours(8)
    assert data.coverage(hours, [{"platform": "facebook"}, {"platform": "tiktok"}]) == {
        "comments_7d": 100 * 24 * 7,
        "platforms": 2,
    }


def test_store_save_and_prune_on_sqlite(monkeypatch):
    engine = create_engine("sqlite://")

    async def fake_exec(stmt, **kwargs):
        with engine.begin() as conn:
            conn.execute(stmt)
        return []

    monkeypatch.setattr(store, "d1_exec", fake_exec)
    monkeypatch.setattr(store, "_ready", False)
    result = {
        "movie_id": "m1",
        "computed_at": "2026-11-17T08:00:00+00:00",
        "window_start": "2026-11-15T08:00:00+00:00",
        "window_end": "2026-11-17T08:00:00+00:00",
        "status": "clear",
        "confidence": "high",
        "reasons": ["Bình luận chê tăng"],
        "signals": [_sig("spike").as_dict()],
    }
    assert asyncio.run(store.save(result))
    asyncio.run(store.save({**result, "computed_at": "2026-08-01T00:00:00+00:00"}))
    asyncio.run(store.prune())
    with engine.connect() as conn:
        rows = conn.execute(select(store.attack_signals.c.computed_at, store.attack_signals.c.reasons_json)).all()
    assert [r.computed_at for r in rows] == ["2026-11-17T08:00:00+00:00"]
    assert "Bình luận chê tăng" in rows[0].reasons_json
