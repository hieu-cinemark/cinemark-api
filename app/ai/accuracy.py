"""Chấm độ chính xác của máy (nhãn cảm xúc + khía cạnh + giai đoạn khán giả, xem app/ai/aspects.py) so với nhãn
người làm - phần tính toán thuần của scripts/accuracy_sample.py, tách riêng để test được.

Nhãn người được nhập tay trong file CSV nên chấp nhận vài cách viết: cảm xúc "positive"/"tich_cuc"/"khen"/"+",
khía cạnh "dien_xuat:+, kich_ban:-", về phim "co"/"khong"."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

from app.ai.aspects import ASPECTS, parse_aspects, parse_stage

SENTIMENTS = ("positive", "negative", "neutral")
_SENTIMENT_ALIASES = {
    "positive": "positive",
    "tich_cuc": "positive",
    "tích cực": "positive",
    "khen": "positive",
    "+": "positive",
    "negative": "negative",
    "tieu_cuc": "negative",
    "tiêu cực": "negative",
    "che": "negative",
    "chê": "negative",
    "-": "negative",
    "neutral": "neutral",
    "trung_lap": "neutral",
    "trung lập": "neutral",
    "0": "neutral",
}
_YES = {"co", "có", "yes", "y", "1", "x", "true"}
_NO = {"khong", "không", "no", "n", "0", "false"}
# Khía cạnh có ít hơn bấy nhiêu nhãn người thì không báo F1 riêng (quá ít để có nghĩa).
MIN_TAGS_PER_ASPECT = 5


def parse_human_sentiment(value: str | None) -> str | None:
    return _SENTIMENT_ALIASES.get((value or "").strip().lower())


def parse_human_aspects(value: str | None) -> list[str]:
    return parse_aspects([part for part in re.split(r"[,;\n]+", value or "") if part.strip()])


def parse_human_yes_no(value: str | None) -> bool | None:
    text = (value or "").strip().lower()
    return True if text in _YES else False if text in _NO else None


def _f1(tp: int, fp: int, fn: int) -> dict[str, float | None]:
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = 2 * precision * recall / (precision + recall) if precision and recall else 0.0 if tp + fp + fn else None
    return {
        "precision": None if precision is None else round(precision, 2),
        "recall": None if recall is None else round(recall, 2),
        "f1": None if f1 is None else round(f1, 2),
    }


def compute_metrics(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    """`pairs`: mỗi phần tử {"human": {...}, "machine": {...}}, cả hai có "sentiment" (str), "aspects" (list
    "key:+"), "stage" (str); human còn có thể có "about_film" (bool | None). Comment máy chưa gán khía cạnh
    (machine["aspects"] là None) không được tính vào phần khía cạnh/giai đoạn."""
    n = len(pairs)
    sentiment_ok = sum(1 for p in pairs if p["human"]["sentiment"] == p["machine"]["sentiment"])
    per_sentiment = {}
    for label in SENTIMENTS:
        tp = sum(1 for p in pairs if p["human"]["sentiment"] == label and p["machine"]["sentiment"] == label)
        fp = sum(1 for p in pairs if p["human"]["sentiment"] != label and p["machine"]["sentiment"] == label)
        fn = sum(1 for p in pairs if p["human"]["sentiment"] == label and p["machine"]["sentiment"] != label)
        per_sentiment[label] = {**_f1(tp, fp, fn), "support": tp + fn}

    with_insights = [p for p in pairs if p["machine"].get("aspects") is not None]
    tp = fp = fn = 0
    per_aspect_counts: dict[str, Counter[str]] = {key: Counter() for key in ASPECTS}
    for p in with_insights:
        human, machine = set(p["human"]["aspects"]), set(p["machine"]["aspects"])
        tp += len(human & machine)
        fp += len(machine - human)
        fn += len(human - machine)
        for tag in human | machine:
            key = tag.rsplit(":", 1)[0]
            bucket = "tp" if tag in human and tag in machine else "fp" if tag in machine else "fn"
            per_aspect_counts[key][bucket] += 1
    per_aspect = {
        key: {**_f1(c["tp"], c["fp"], c["fn"]), "support": c["tp"] + c["fn"]}
        for key, c in per_aspect_counts.items()
        if c["tp"] + c["fn"] >= MIN_TAGS_PER_ASPECT
    }
    stage_ok = sum(1 for p in with_insights if parse_stage(p["human"]["stage"]) == p["machine"].get("stage"))

    about = [p["human"].get("about_film") for p in pairs if p["human"].get("about_film") is not None]
    return {
        "n": n,
        "sentiment_accuracy": round(sentiment_ok * 100 / n) if n else None,
        "sentiment": per_sentiment,
        "praise_f1": per_sentiment["positive"]["f1"],
        "criticism_f1": per_sentiment["negative"]["f1"],
        "aspects_n": len(with_insights),
        "aspects": _f1(tp, fp, fn),
        "per_aspect": per_aspect,
        "stage_accuracy": round(stage_ok * 100 / len(with_insights)) if with_insights else None,
        # % comment mà người chấm nói CÓ nói về phim - phần còn lại là nhiễu lọt qua các bộ lọc.
        "about_film_pct": round(sum(about) * 100 / len(about)) if about else None,
    }
