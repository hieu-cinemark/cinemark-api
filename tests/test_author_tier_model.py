"""Mô hình tầng tác giả (18/11/2026): đặc trưng dùng chung cho train và gắn nhãn, chọn tác giả chưa nhãn, ngưỡng tự áp."""

from __future__ import annotations

import asyncio

from app.ml import author_tier as at
from app.services.ai_jobs import author_model


def _post(**fields):
    return {
        "author": "Rạp ABC",
        "content": "Lịch chiếu phim mới",
        "movie_id": "m1",
        "keyword_id": "k1",
        "like_count": 10,
        "reply_count": 2,
        "view_count": 100,
        "posted_at": "2026-11-01T00:00:00Z",
        "relevance_label": "related",
        **fields,
    }


def test_fan_is_merged_into_outsider_and_unknown_tiers_are_dropped():
    assert at.normalize_tier("fan") == "outsider"
    assert at.normalize_tier(" PRO ") == "pro"
    assert at.normalize_tier("talent") is None


def test_author_features_count_movies_and_signal_shares():
    posts = [
        _post(movie_id="m1", is_ad=True, author_verified=False),
        _post(movie_id="m2", content="Review phim khác", is_ad=False),
        _post(movie_id="m2", content="Lịch chiếu phim mới", relevance_label="not_related"),
    ]
    row = at.author_features("tiktok", "rapabc", posts, {"followers": 5000, "niche": "film"})
    assert (row["n_posts"], row["n_relevant"], row["n_movies"]) == (3, 2, 2)
    assert row["captions"] == ["Lịch chiếu phim mới", "Review phim khác"]  # bài liên quan trước, không lặp
    assert row["is_ad"] == 0.5  # 1 trên 2 bài có cờ
    assert row["author_verified"] == 0.0
    assert row["is_branded"] is None  # không bài nào có cờ này: không biết, khác với "không"
    assert row["followers"] == 5000 and row["niche"] == "film"


def test_confident_keeps_only_predictions_at_or_above_the_threshold():
    predictions = [("outsider", 0.97), ("pro", 0.62), ("media", 0.9)]
    assert at.confident(predictions, 0.9) == [0, 2]


def test_pick_unlabeled_skips_labeled_handles_on_any_platform_and_authors_without_relevant_posts():
    authors = {
        ("tiktok", "a"): {"platform": "tiktok", "handle": "a", "n_relevant": 3},
        ("tiktok", "b"): {"platform": "tiktok", "handle": "b", "n_relevant": 2},
        ("facebook", "c"): {"platform": "facebook", "handle": "c", "n_relevant": 0},
    }
    labels = {("", "b"): "pro"}  # nhãn chung của handle chặn mọi nền tảng
    assert [row["handle"] for row in author_model.pick_unlabeled(authors, labels)] == ["a"]


def test_labelling_without_a_model_does_nothing(monkeypatch, tmp_path):
    monkeypatch.setattr(author_model, "MODEL_PATH", tmp_path / "missing.joblib")
    assert asyncio.run(author_model.label_with_model(threshold=0.9)) == {"skipped": "no_model"}


def test_train_and_predict_round_trip():
    """Featurizer + model train được trên vài dòng và predict trả (tầng, xác suất) - đúng kiểu job gắn nhãn dùng."""
    from sklearn.linear_model import LogisticRegression

    rows = []
    for i in range(12):
        tier = ("media", "outsider", "pro")[i % 3]
        captions = {
            "media": ["tin tức phim lịch chiếu"],
            "outsider": ["mình vừa xem phim hay"],
            "pro": ["hợp tác review"],
        }
        rows.append(
            {
                **at.author_features("tiktok", f"user{i}", [_post(content=captions[tier][0])]),
                "tier": tier,
            }
        )
    featurizer = at.Featurizer()
    featurizer.names.min_df = featurizer.captions.min_df = 1
    featurizer.fit(rows)
    model = LogisticRegression(max_iter=200).fit(featurizer.transform(rows), [r["tier"] for r in rows])
    predictions = at.predict({"featurizer": featurizer, "model": model}, rows[:3])
    assert [tier for tier, _ in predictions] == ["media", "outsider", "pro"]
    assert all(type(tier) is str for tier, _ in predictions)
    assert all(0 <= probability <= 1 for _, probability in predictions)
