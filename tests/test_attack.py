"""Attack Detection: dấu hiệu A/B, quy tắc trạng thái, đọc embedding, bảng attack_signals (SQLite trong bộ nhớ)."""

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta

import numpy as np
from sqlalchemy import create_engine, select

from app.services.attack import data, store
from app.services.attack.accounts import account_signal
from app.services.attack.claims import claims_signal
from app.services.attack.signals import Signal, mask_name
from app.services.attack.similar import normalize, similar_clusters, similar_signal
from app.services.attack.spikes import find_spikes, spike_signal
from app.services.attack.spread import spread_signal
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


# --- C: nhóm tài khoản hành vi giống nhau ---

T0 = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
TEXT = "Phim dở tệ, đừng ai đi xem, phí tiền!"


def _crow(i, author, post, minute, text=TEXT, platform="facebook"):
    return {
        "id": f"c{i}",
        "author_id": author,
        "author_name": "Nguyễn Văn An",
        "post_id": post,
        "post_url": f"https://www.facebook.com/page{post}/posts/1",
        "post_author": f"page{post}",
        "platform": platform,
        "posted_at": (T0 + timedelta(minutes=minute)).isoformat(),
        "message": text,
        "likes": 1,
    }


def _campaign(gap_minutes_between_accounts):
    """6 tài khoản cùng bình luận dưới 5 bài, mỗi bài cách nhau 1 giờ."""
    rows, n = [], 0
    for p in range(5):
        for a in range(6):
            rows.append(_crow(n, f"acc{a}", f"p{p}", p * 60 + a * gap_minutes_between_accounts))
            n += 1
    return rows


def test_accounts_strong_when_pairs_comment_close_together_on_many_posts():
    rows = _campaign(3)  # cách nhau 3 phút -> cặp L2; 5 bài trong 24 giờ -> L1
    signal = account_signal(rows, similar_clusters(rows, None))
    assert signal.fired and signal.strong and signal.score == 6
    assert all("author_id" not in e for e in signal.evidence)  # không lộ id thật


def test_accounts_not_strong_when_far_apart_in_time():
    rows = _campaign(180)  # cách nhau 3 giờ -> không có cặp L2, chỉ còn L1
    assert not account_signal(rows, similar_clusters(rows, None)).strong


def test_one_account_many_posts_is_not_a_group():
    rows = [_crow(i, "solo", f"p{i}", i * 90, text=f"ý kiến riêng khác nhau số {i}") for i in range(6)]
    assert not account_signal(rows, similar_clusters(rows, None)).fired


def test_accounts_ignore_rows_without_author_id():
    rows = [{**r, "author_id": None} for r in _campaign(3)]
    assert not account_signal(rows, similar_clusters(rows, None)).fired


def test_similar_clusters_expose_member_ids_but_signal_detail_does_not():
    rows = [_crow(i, f"u{i}", "p", i) for i in range(8)]
    clusters = similar_clusters(rows, None)
    assert sorted(clusters[0]["member_ids"]) == sorted(r["id"] for r in rows)
    assert "member_ids" not in similar_signal(rows, None, clusters=clusters).detail["clusters"][0]


# --- D: luận điểm lan rộng ---


def test_spread_single_post_is_common_source_and_not_fired():
    rows = [_crow(i, f"u{i}", "same", i) for i in range(10)]
    signal = spread_signal(rows, similar_clusters(rows, None))
    assert not signal.fired and signal.detail["spread"][0]["common_source"]


def test_spread_wide_is_fired_but_never_strong():
    rows = [_crow(i, f"u{i}", f"q{i % 4}", i * 2, platform="facebook" if i % 4 < 2 else "tiktok") for i in range(12)]
    for r in rows:
        if r["platform"] == "tiktok":
            r["post_url"] = f"https://www.tiktok.com/@creator{r['post_id']}/video/1"
    signal = spread_signal(rows, similar_clusters(rows, None))
    assert signal.fired and not signal.strong and signal.score == 4


# --- E: khẳng định cần kiểm tra ---


def test_claims_need_enough_comments_and_accounts_and_never_strong():
    rows = [_crow(i, f"u{i}", "p", i, text="Phim này đạo nhái hoàn toàn, sao chép phim Hàn") for i in range(6)]
    signal = claims_signal(rows)
    assert signal.fired and not signal.strong and signal.detail["claims"][0]["label"] == "Đạo nhái / sao chép"
    assert not claims_signal([_crow(1, "a", "p", 1, text="phim dở")]).fired


# --- trạng thái với C/D/E ---


def test_decide_coordinated_needs_overlapping_windows_for_accounts():
    ok = {"comments_7d": 3000, "platforms": 2}
    assert decide([_sig("similar"), _sig("accounts")], ok)[0] == "coordinated"
    assert decide([_sig("similar"), _sig("accounts", window=("2026-11-17T08", "2026-11-17T09"))], ok)[0] == "suspicious"
    status, _, reasons = decide([_sig("spread", strong=False), _sig("claims", strong=False)], ok)
    assert status == "clear" and len(reasons) == 2


# --- mốc sự kiện nhập tay và file thử ngưỡng ---


def test_load_events_adds_manual_marks_in_vn_hours(monkeypatch):
    async def fake_exec(stmt, **kwargs):
        text = str(stmt)
        if "movie_events" in text:
            return [{"at": "2026-11-16T13:00:00+00:00"}, {"at": "không phải ngày"}]
        return [{"released_at": "2026-11-15", "premiere_date": None}]

    monkeypatch.setattr(data, "d1_exec", fake_exec)
    assert asyncio.run(data.load_events("m1")) == ["2026-11-15T00", "2026-11-16T20"]


def test_manual_event_makes_a_nearby_spike_not_strong():
    spikes = find_spikes(_hours(16, "2026-11-16T21", 60), ["2026-11-16T20"])
    assert spikes and not spikes[0]["strong"]


def test_backtest_file_is_one_compact_line_per_point(tmp_path):
    from app.services.ai_jobs.attack_detect import append_backtest

    result = {
        "movie_id": "m1",
        "computed_at": "2026-11-17T08:00:00+00:00",
        "status": "clear",
        "confidence": "high",
        "reasons": ["x"],
        "signals": [_sig("spike").as_dict()],
    }
    append_backtest(result, tmp_path)
    append_backtest(result, tmp_path)
    lines = (tmp_path / "m1.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2 and '"spike"' in lines[0] and "evidence" not in lines[0]
