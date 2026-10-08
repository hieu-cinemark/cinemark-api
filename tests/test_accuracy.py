"""app/ai/accuracy.py - chấm nhãn máy so với nhãn người (scripts/accuracy_sample.py score)."""

from __future__ import annotations

from app.ai.accuracy import compute_metrics, parse_human_aspects, parse_human_sentiment, parse_human_yes_no


def _pair(
    human_sentiment,
    machine_sentiment,
    human_aspects=(),
    machine_aspects=(),
    human_stage="khac",
    machine_stage="khac",
    about=True,
):
    return {
        "human": {
            "sentiment": human_sentiment,
            "aspects": list(human_aspects),
            "stage": human_stage,
            "about_film": about,
        },
        "machine": {"sentiment": machine_sentiment, "aspects": list(machine_aspects), "stage": machine_stage},
    }


def test_human_inputs_accept_vietnamese_spellings() -> None:
    assert parse_human_sentiment(" Tich_cuc ") == "positive"
    assert parse_human_sentiment("chê") == "negative"
    assert parse_human_sentiment("") is None
    assert parse_human_aspects("dien_xuat:+, kich_ban:-; bịa:+") == ["dien_xuat:+", "kich_ban:-"]
    assert parse_human_yes_no("Có") is True and parse_human_yes_no("khong") is False and parse_human_yes_no("") is None


def test_metrics() -> None:
    pairs = [
        _pair("positive", "positive", ["dien_xuat:+"], ["dien_xuat:+"], "da_xem", "da_xem"),
        _pair("positive", "neutral", ["kich_ban:+"], [], "da_xem", "khac"),
        _pair("negative", "negative", ["kich_ban:-"], ["kich_ban:-", "san_phim:-"], "da_xem", "da_xem"),
        _pair("neutral", "neutral", about=False),
    ]
    pairs.append({**_pair("neutral", "neutral"), "machine": {"sentiment": "neutral", "aspects": None, "stage": None}})

    m = compute_metrics(pairs)

    assert m["n"] == 5
    assert m["sentiment_accuracy"] == 80  # 4/5
    assert m["sentiment"]["positive"] == {"precision": 1.0, "recall": 0.5, "f1": 0.67, "support": 2}
    assert m["praise_f1"] == 0.67 and m["criticism_f1"] == 1.0
    # Comment máy chưa gán khía cạnh không tính vào phần khía cạnh: tp=2 (dien_xuat:+, kich_ban:-), fp=1, fn=1.
    assert m["aspects_n"] == 4
    assert m["aspects"] == {"precision": 0.67, "recall": 0.67, "f1": 0.67}
    assert m["per_aspect"] == {}  # chưa khía cạnh nào đủ MIN_TAGS_PER_ASPECT nhãn người
    assert m["stage_accuracy"] == 75  # 3/4
    assert m["about_film_pct"] == 80  # 4/5 người chấm nói có
