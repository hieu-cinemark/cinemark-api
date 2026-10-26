"""Fine-tune ViSoBERT nhận diện khía cạnh khen/chê của comment (Review Angles) - nhiều nhãn, mỗi comment 0..n nhãn
"key:+/-" (app/ai/aspects.py).

scripts/aspect_probe.py (embedding cố định + một lớp tuyến tính) chỉ đạt micro F1 0,33-0,39; ở đây train CẢ model. Dữ
liệu: ~/cinemark-ml/aspects/comments.jsonl do aspect_probe tải (nhãn do AI gán; chấm tay 26/10: ~68-72% nhãn dương
đúng - đó là trần thực tế của model học từ chúng). Bỏ comment chỉ có sticker/emoji (dưới MIN_CHARS ký tự sau khi làm
sạch). Chia train/val/test THEO BÀI giống aspect_probe để so sánh được; ngưỡng từng nhãn chọn trên val, báo trên test.

    python -m scripts.aspect_finetune                    # 2 epoch, 30.000 mẫu âm
    python -m scripts.aspect_finetune --epochs 3 --neg 60000
    python -m scripts.aspect_finetune --calibrate        # chỉ chọn lại ngưỡng theo số đếm cho model đã train
"""

from __future__ import annotations

import argparse
import json
import random
import time
from collections import Counter
from pathlib import Path

import numpy as np
import torch

from scripts.aspect_probe import DATA_DIR, MIN_SUPPORT, SEED, _split, download
from scripts.topic_cluster_probe import normalize

BASE_MODEL = "5CD-AI/Vietnamese-Sentiment-visobert"
OUT_DIR = DATA_DIR / "visobert-aspects"
MAX_TOKENS = 128
MIN_CHARS = 10


def prf(pred: np.ndarray, gold: np.ndarray) -> tuple[float, float, float]:
    tp = float((pred * gold).sum())
    p = tp / max(pred.sum(), 1)
    r = tp / max(gold.sum(), 1)
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def _aspect_matrix(mat: np.ndarray, labels: list[str]) -> tuple[list[str], np.ndarray]:
    """Gộp nhãn "key:+"/"key:-" về khía cạnh "key" (bỏ khen/chê) - Review Angles chỉ xếp hạng theo số lượt nhắc."""
    aspects = sorted({label.split(":")[0] for label in labels})
    cols = [[j for j, label in enumerate(labels) if label.split(":")[0] == a] for a in aspects]
    return aspects, np.stack([mat[:, c].max(1) for c in cols], axis=1)


def _ranks(counts: np.ndarray) -> list[float]:
    order = np.argsort(-counts)
    ranks = np.empty(len(counts))
    ranks[order] = range(len(counts))
    return list(ranks)


def calibrate(
    out: Path, rows: list[dict], labels: list[str], y: np.ndarray, val_idx: list[int], test_idx: list[int]
) -> None:
    """Chọn lại ngưỡng từng nhãn trên val để SỐ ĐẾM sát thực tế (F1 không tụt quá 0,05 so với ngưỡng tốt nhất) - ngưỡng
    tối ưu F1 làm model đếm 'sạn phim' gấp ~3 lần thực tế, đẩy sai thứ hạng góc. Ghi count_thresholds vào
    aspect_labels.json, báo F1 / số đếm / tương quan thứ hạng trên test cho cả hai bộ ngưỡng."""
    from statistics import correlation

    from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

    meta = json.loads((out / "aspect_labels.json").read_text())
    assert meta["labels"] == labels, "bộ nhãn khác lúc train"
    f1_thresholds = np.array([meta["results"][-1]["thresholds"][label] for label in labels])
    device = "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer = PreTrainedTokenizerFast.from_pretrained(out)
    model = AutoModelForSequenceClassification.from_pretrained(out).to(device).eval()

    def probs(idx: list[int]) -> np.ndarray:
        chunks = []
        with torch.inference_mode():
            for start in range(0, len(idx), 128):
                enc = tokenizer(
                    [" ".join(rows[i]["text"].split()) for i in idx[start : start + 128]],
                    padding=True,
                    truncation=True,
                    max_length=MAX_TOKENS,
                    return_tensors="pt",
                ).to(device)
                chunks.append(torch.sigmoid(model(**enc).logits).float().cpu().numpy())
        return np.concatenate(chunks)

    p_val, p_test = probs(val_idx), probs(test_idx)
    y_val, y_test = y[val_idx], y[test_idx]
    grid = np.linspace(0.05, 0.95, 91)
    count_thresholds = []
    for j in range(len(labels)):
        f1s = [prf((p_val[:, j] >= t).astype(float), y_val[:, j])[2] for t in grid]
        floor = max(f1s) - 0.05
        gold = y_val[:, j].sum()
        candidates = [t for t, f in zip(grid, f1s, strict=True) if f >= floor]
        count_thresholds.append(min(candidates, key=lambda t, j=j: abs((p_val[:, j] >= t).sum() - gold)))
    count_thresholds = np.array(count_thresholds)

    aspects, gold = _aspect_matrix(y_test, labels)
    gold_counts = gold.sum(0)
    print(f"\n{'khía cạnh':12s} {'thật':>6s} {'cũ: đếm':>8s} {'F1':>5s} {'mới: đếm':>9s} {'F1':>5s}")
    summary = {}
    for name, thresholds in (("cũ", f1_thresholds), ("mới", count_thresholds)):
        _, pred = _aspect_matrix((p_test >= thresholds).astype(float), labels)
        f1 = [prf(pred[:, a], gold[:, a])[2] for a in range(len(aspects))]
        summary[name] = (pred.sum(0), f1, prf(pred, gold)[2], correlation(_ranks(gold_counts), _ranks(pred.sum(0))))
    for a, aspect in enumerate(aspects):
        print(
            f"{aspect:12s} {int(gold_counts[a]):6d} {int(summary['cũ'][0][a]):8d} {summary['cũ'][1][a]:5.2f} "
            f"{int(summary['mới'][0][a]):9d} {summary['mới'][1][a]:5.2f}"
        )
    for name in ("cũ", "mới"):
        counts = summary[name][0]
        print(
            f"ngưỡng {name}: micro F1 {summary[name][2]:.2f} · tương quan thứ hạng {summary[name][3]:.2f} · "
            f"thứ hạng {[aspects[a] for a in np.argsort(-counts)]}"
        )
    print(f"thứ hạng thật: {[aspects[a] for a in np.argsort(-gold_counts)]}")
    meta["count_thresholds"] = {label: round(float(t), 3) for label, t in zip(labels, count_thresholds, strict=True)}
    (out / "aspect_labels.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1))
    print(f"đã ghi count_thresholds vào {out / 'aspect_labels.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--neg", type=int, default=30000, help="số mẫu âm (không khía cạnh) dùng")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--lr", type=float, default=3e-5)
    parser.add_argument("--out", type=Path, default=OUT_DIR)
    parser.add_argument(
        "--calibrate", action="store_true", help="không train: chọn lại ngưỡng theo số đếm cho model đã lưu"
    )
    args = parser.parse_args()

    from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

    rows = [r for r in download(60000) if len(normalize(r["text"])) >= MIN_CHARS]
    pos = [r for r in rows if r["labels"]]
    neg = [r for r in rows if not r["labels"]]
    random.Random(SEED).shuffle(neg)
    rows = pos + neg[: args.neg]
    support = Counter(label for r in rows for label in r["labels"])
    labels = sorted(label for label, n in support.items() if n >= MIN_SUPPORT)
    index = {label: i for i, label in enumerate(labels)}
    y = np.zeros((len(rows), len(labels)), dtype=np.float32)
    for i, r in enumerate(rows):
        for label in r["labels"]:
            if label in index:
                y[i, index[label]] = 1
    split = _split(rows)
    train_idx = [i for i in range(len(rows)) if split[i] == 0]
    val_idx = [i for i in range(len(rows)) if split[i] == 1]
    test_idx = [i for i in range(len(rows)) if split[i] == 2]
    print(
        f"{len(rows)} comment ({len(pos)} có khía cạnh) · {len(labels)} nhãn · train {len(train_idx)} / val {len(val_idx)} / test {len(test_idx)}"
    )

    if args.calibrate:
        calibrate(args.out, rows, labels, y, val_idx, test_idx)
        return

    device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer = PreTrainedTokenizerFast.from_pretrained(BASE_MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(
        BASE_MODEL, num_labels=len(labels), problem_type="multi_label_classification", ignore_mismatched_sizes=True
    ).to(device)

    pos_count = torch.tensor(y[train_idx].sum(0))
    pos_weight = ((len(train_idx) - pos_count) / pos_count.clamp(min=1)).clamp(max=20).to(device)
    loss_fn = torch.nn.BCEWithLogitsLoss(pos_weight=pos_weight)

    def encode(idx: list[int]):
        return tokenizer(
            [" ".join(rows[i]["text"].split()) for i in idx],
            padding=True,
            truncation=True,
            max_length=MAX_TOKENS,
            return_tensors="pt",
        ).to(device)

    def predict(idx: list[int]) -> np.ndarray:
        model.eval()
        out = []
        with torch.inference_mode():
            for start in range(0, len(idx), 128):
                out.append(torch.sigmoid(model(**encode(idx[start : start + 128])).logits).float().cpu().numpy())
        model.train()
        return np.concatenate(out)

    def evaluate(tag: str) -> dict:
        p_val, p_test = predict(val_idx), predict(test_idx)
        y_val, y_test = y[val_idx], y[test_idx]
        grid = np.linspace(0.1, 0.9, 33)
        thresholds = np.array(
            [
                max(grid, key=lambda t, j=j: prf((p_val[:, j] >= t).astype(float), y_val[:, j])[2])
                for j in range(len(labels))
            ]
        )
        pred = (p_test >= thresholds).astype(float)
        print(f"\n=== {tag}: test {len(test_idx)} comment ===")
        print(f"{'nhãn':16s} {'mẫu test':>8s} {'P':>6s} {'R':>6s} {'F1':>6s}")
        per_label = {}
        for j, label in enumerate(labels):
            p, r, f = prf(pred[:, j], y_test[:, j])
            per_label[label] = round(f, 3)
            print(f"{label:16s} {int(y_test[:, j].sum()):8d} {p:6.2f} {r:6.2f} {f:6.2f}")
        micro = prf(pred, y_test)
        has_any = prf(pred.max(1), y_test.max(1))
        print(f"{'micro':16s} {int(y_test.sum()):8d} {micro[0]:6.2f} {micro[1]:6.2f} {micro[2]:6.2f}")
        print(
            f"{'có khía cạnh':16s} {int(y_test.max(1).sum()):8d} {has_any[0]:6.2f} {has_any[1]:6.2f} {has_any[2]:6.2f}",
            flush=True,
        )
        return {"tag": tag, "micro_f1": round(micro[2], 3), "has_aspect_f1": round(has_any[2], 3), "per_label": per_label,
                "thresholds": {label: round(float(t), 3) for label, t in zip(labels, thresholds, strict=True)}}  # fmt: skip

    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.01)
    steps = args.epochs * ((len(train_idx) + args.batch_size - 1) // args.batch_size)
    scheduler = torch.optim.lr_scheduler.LambdaLR(
        optimizer, lambda s: min(1.0, s / max(1, steps * 0.06)) * max(0.0, (steps - s) / steps)
    )
    rng = random.Random(SEED)
    model.train()
    step, started, results = 0, time.monotonic(), []
    for epoch in range(args.epochs):
        rng.shuffle(train_idx)
        for start in range(0, len(train_idx), args.batch_size):
            batch = train_idx[start : start + args.batch_size]
            logits = model(**encode(batch)).logits
            loss = loss_fn(logits, torch.tensor(y[batch], device=device))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()
            optimizer.zero_grad()
            step += 1
            if step % 100 == 0:
                rate = step / (time.monotonic() - started)
                print(
                    f"  epoch {epoch + 1} bước {step}/{steps} loss {loss.item():.3f} · còn ~{(steps - step) / rate / 60:.0f} phút",
                    flush=True,
                )
        results.append(evaluate(f"sau epoch {epoch + 1}"))
    args.out.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(args.out)
    tokenizer.save_pretrained(args.out)
    (args.out / "aspect_labels.json").write_text(
        json.dumps({"labels": labels, "results": results}, ensure_ascii=False, indent=1)
    )
    print(f"\nđã lưu {args.out} · tổng {(time.monotonic() - started) / 60:.0f} phút")


if __name__ == "__main__":
    main()
