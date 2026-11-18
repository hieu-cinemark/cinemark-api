"""Mô hình phân loại tầng tác giả (owned / pro / media / outsider) - phần dùng chung cho xuất dữ liệu, train và gắn nhãn (18/11/2026).

Module này chỉ dùng numpy / scipy / scikit-learn, không import gì khác của app: script train chạy bằng môi trường ML riêng
(`scripts/ml/author_tier_train.py`) cũng import được, và model lưu bằng joblib tham chiếu đúng `app.ml.author_tier.Featurizer`
nên nạp lại được ở cinemark-api (`scripts/jobs/author_tier_label.py`). Đổi đặc trưng ở đây thì phải train lại model.

Đặc trưng của một tác giả tính từ MỌI bài của họ trong bảng posts (author_features) - train và gắn nhãn dùng đúng cùng một hàm.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
from scipy.sparse import csr_matrix, hstack
from sklearn.feature_extraction.text import TfidfVectorizer

TIERS = ("owned", "pro", "media", "outsider")
# `fan` (fanclub diễn viên) - Worker cinemark tính như outsider ở mọi chỉ số; luật tên diễn viên của Worker vẫn gắn được nó.
TIER_ALIASES = {"fan": "outsider"}
PLATFORMS = ("facebook", "threads", "tiktok", "youtube")
NUMERIC = ("n_posts", "n_relevant", "n_movies", "n_keywords", "likes_med", "replies_med", "views_med", "followers")
# Tín hiệu tác giả trong raw_json (lưu từ 18/11/2026, app/repositories/d1/post_raw.RAW_JSON_KEEP): tỷ lệ bài có cờ.
SIGNALS = ("author_verified", "author_is_business", "author_is_seller", "is_ad", "is_branded")
MAX_CAPTIONS = 8
MODEL_SOURCE = "local:author_tier"


def normalize_tier(raw: Any) -> str | None:
    tier = TIER_ALIASES.get(str(raw or "").strip().lower(), str(raw or "").strip().lower())
    return tier if tier in TIERS else None


def _truthy(value: Any) -> bool:
    return value in (True, 1, "1", "true", "True")


def _median(values: list[float]) -> float | None:
    return float(statistics.median(values)) if values else None


def author_features(
    platform: str, handle: str, posts: list[dict[str, Any]], profile: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Một dòng đặc trưng từ các bài của tác giả. Mỗi bài cần: author, author_name, raw_followers, content, movie_id, keyword_id,
    like_count, reply_count, view_count, posted_at, relevance_label, và các khoá SIGNALS / author_signature nếu có."""
    profile = profile or {}
    relevant = [p for p in posts if p.get("relevance_label") in (None, "", "related")]
    ordered = relevant + [p for p in posts if p not in relevant]
    captions: list[str] = []
    for post in ordered:
        text = " ".join(str(post.get("content") or "").split())
        if text and text not in captions:
            captions.append(text)
        if len(captions) >= MAX_CAPTIONS:
            break
    names = Counter(str(p.get("author_name") or p.get("author") or "").strip() for p in posts)
    names.pop("", None)
    signatures = Counter(str(p.get("author_signature") or "").strip() for p in posts)
    signatures.pop("", None)
    followers = [float(p["raw_followers"]) for p in posts if str(p.get("raw_followers") or "").isdigit()]
    dates = sorted(str(p.get("posted_at") or "")[:10] for p in posts if p.get("posted_at"))
    row: dict[str, Any] = {
        "platform": platform,
        "handle": handle,
        "name": names.most_common(1)[0][0] if names else handle,
        "signature": signatures.most_common(1)[0][0] if signatures else "",
        "captions": captions,
        "n_posts": len(posts),
        "n_relevant": len(relevant),
        "n_movies": len({p.get("movie_id") for p in posts if p.get("movie_id")}),
        "n_keywords": len({p.get("keyword_id") for p in posts if p.get("keyword_id")}),
        "likes_med": _median([float(p["like_count"]) for p in posts if p.get("like_count") is not None]),
        "replies_med": _median([float(p["reply_count"]) for p in posts if p.get("reply_count") is not None]),
        "views_med": _median([float(p["view_count"]) for p in posts if p.get("view_count") is not None]),
        "followers": profile.get("followers") or (max(followers) if followers else None),
        "niche": profile.get("niche"),
        "first_at": dates[0] if dates else None,
        "last_at": dates[-1] if dates else None,
    }
    for signal in SIGNALS:
        known = [p for p in posts if p.get(signal) is not None]
        row[signal] = (sum(1 for p in known if _truthy(p.get(signal))) / len(known)) if known else None
    return row


def name_text(row: dict[str, Any]) -> str:
    return f"{row['handle']} {row.get('name') or ''} {row.get('signature') or ''}".lower()


def caption_text(row: dict[str, Any]) -> str:
    return " \n ".join(row.get("captions") or []).lower()


def numeric_features(rows: list[dict[str, Any]], niches: list[str]) -> csr_matrix:
    out = []
    for row in rows:
        values = [math.log1p(max(0.0, float(row.get(name) or 0))) for name in NUMERIC]
        values.append(1.0 if row.get("followers") in (None, "", 0) else 0.0)
        try:
            span = (date.fromisoformat(row["last_at"]) - date.fromisoformat(row["first_at"])).days
        except TypeError, ValueError, KeyError:
            span = 0
        values.append(math.log1p(max(0, span)))
        posts = max(1, int(row.get("n_posts") or 1))
        values.append(float(row.get("n_movies") or 0) / posts)  # trang tổng hợp: mỗi bài một phim khác
        for signal in SIGNALS:
            share = row.get(signal)
            values += [float(share or 0.0), 1.0 if share is None else 0.0]
        values += [1.0 if row.get("platform") == platform else 0.0 for platform in PLATFORMS]
        values += [1.0 if (row.get("niche") or "") == niche else 0.0 for niche in niches]
        out.append(values)
    return csr_matrix(np.asarray(out, dtype=np.float32))


class Featurizer:
    """TF-IDF ký tự (handle + tên + bio), TF-IDF từ (caption) và số liệu tác giả."""

    def __init__(self) -> None:
        self.names = TfidfVectorizer(
            analyzer="char_wb", ngram_range=(2, 4), min_df=3, max_features=60000, sublinear_tf=True
        )
        self.captions = TfidfVectorizer(ngram_range=(1, 2), min_df=3, max_features=120000, sublinear_tf=True)
        self.niches: list[str] = []

    def fit(self, rows: list[dict[str, Any]]) -> Featurizer:
        self.names.fit([name_text(r) for r in rows])
        self.captions.fit([caption_text(r) for r in rows])
        counts = Counter(r.get("niche") for r in rows if r.get("niche"))
        self.niches = [niche for niche, n in counts.most_common() if n >= 20]
        return self

    def transform(self, rows: list[dict[str, Any]]) -> csr_matrix:
        return hstack(
            [
                self.names.transform([name_text(r) for r in rows]),
                self.captions.transform([caption_text(r) for r in rows]),
                numeric_features(rows, self.niches) * 0.5,
            ]
        ).tocsr()


def load_bundle(path: Path) -> dict[str, Any]:
    import joblib

    return joblib.load(path)


def predict(bundle: dict[str, Any], rows: list[dict[str, Any]]) -> list[tuple[str, float]]:
    """(tầng, xác suất) cho từng dòng đặc trưng."""
    if not rows:
        return []
    proba = bundle["model"].predict_proba(bundle["featurizer"].transform(rows))
    classes = list(bundle["model"].classes_)
    best = proba.argmax(axis=1)
    # str(): classes_ là mảng numpy - np.str_ hiện lẫn vào log và giá trị ghi D1.
    return [(str(classes[i]), float(proba[row, i])) for row, i in enumerate(best)]


def confident(predictions: list[tuple[str, float]], threshold: float) -> list[int]:
    """Chỉ số các dự đoán đủ tự tin để tự áp nhãn."""
    return [i for i, (_tier, probability) in enumerate(predictions) if probability >= threshold]
