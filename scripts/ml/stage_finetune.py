"""Fine-tune ViSoBERT thành model giai đoạn khán giả (hong / da_xem / khac) từ nhãn Haiku (2026-11-09,
scripts/ml/stage_label_haiku.py -> data/stage/haiku_labels.jsonl). Khởi tạo từ model cảm xúc đã quen bình luận phim
(models/visobert-cinemark), thay đầu phân loại 3 nhãn. Bộ kiểm tra là --eval-size câu giữ riêng. Model lưu ở models/visobert-stage/
(không commit), dùng bởi scripts/jobs/stage_label.py.

    python -m scripts.ml.stage_finetune --epochs 3
"""

from __future__ import annotations

import argparse
import json
import random
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_FILE = ROOT / "data" / "stage" / "haiku_labels_v2.jsonl"
INIT_MODEL = ROOT / "models" / "visobert-cinemark"
OUT_DIR = ROOT / "models" / "visobert-stage"
LABELS = ("hong", "da_xem", "khac")
MAX_TOKENS = 128


def train(rows: list[dict[str, str]], args: argparse.Namespace) -> None:
    import torch
    from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

    rng = random.Random(42)
    rng.shuffle(rows)
    eval_rows, train_rows = rows[: args.eval_size], rows[args.eval_size :]
    counts = Counter(r["label"] for r in train_rows)
    print(
        f"huấn luyện {len(train_rows)} ({dict(counts)}) · kiểm tra {len(eval_rows)} ({dict(Counter(r['label'] for r in eval_rows))})"
    )

    device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    # AutoTokenizer của transformers 5 nạp hỏng tokenizer XLM-R của ViSoBERT (xem scripts/jobs/visobert_label.py).
    tokenizer = PreTrainedTokenizerFast.from_pretrained(INIT_MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(
        INIT_MODEL,
        num_labels=len(LABELS),
        id2label=dict(enumerate(LABELS)),
        label2id={label: i for i, label in enumerate(LABELS)},
        ignore_mismatched_sizes=True,
    ).to(device)
    label_id = {label: i for i, label in enumerate(LABELS)}
    # "khac" chiếm đa số: cân lớp để model không đoán khac cho mọi câu.
    weights = torch.tensor([len(train_rows) / (len(LABELS) * max(1, counts[label])) for label in LABELS], device=device)
    loss_fn = torch.nn.CrossEntropyLoss(weight=weights)

    def batches(data: list[dict[str, str]], size: int):
        for i in range(0, len(data), size):
            chunk = data[i : i + size]
            enc = tokenizer(
                [" ".join(r["text"].split()) for r in chunk],
                padding=True,
                truncation=True,
                max_length=MAX_TOKENS,
                return_tensors="pt",
            ).to(device)
            yield enc, torch.tensor([label_id[r["label"]] for r in chunk], device=device)

    def evaluate() -> float:
        model.eval()
        preds: list[int] = []
        with torch.inference_mode():
            for enc, _ in batches(eval_rows, 128):
                preds.extend(model(**enc).logits.argmax(dim=-1).tolist())
        gold = [label_id[r["label"]] for r in eval_rows]
        acc = sum(p == g for p, g in zip(preds, gold, strict=True)) / max(1, len(gold))
        matrix = Counter((LABELS[g], LABELS[p]) for p, g in zip(preds, gold, strict=True))
        print(f"  khớp {acc:.1%}  (hàng = nhãn Haiku, cột = model)")
        print(" " * 10 + "".join(f"{s:>9}" for s in LABELS))
        for a in LABELS:
            print(f"  {a:<8}" + "".join(f"{matrix[(a, b)]:>9}" for b in LABELS))
        model.train()
        return acc

    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.01)
    steps = args.epochs * ((len(train_rows) + args.batch_size - 1) // args.batch_size)
    scheduler = torch.optim.lr_scheduler.LambdaLR(
        optimizer, lambda s: min(1.0, s / max(1, steps * 0.06)) * max(0.0, (steps - s) / steps)
    )
    model.train()
    step, started = 0, time.monotonic()
    for epoch in range(args.epochs):
        rng.shuffle(train_rows)
        for enc, labels in batches(train_rows, args.batch_size):
            loss = loss_fn(model(**enc).logits, labels)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()
            optimizer.zero_grad()
            step += 1
            if step % 50 == 0:
                rate = step / (time.monotonic() - started)
                print(
                    f"  epoch {epoch + 1} bước {step}/{steps} loss {loss.item():.3f} · còn ~{(steps - step) / rate / 60:.0f} phút",
                    flush=True,
                )
        print(f"sau epoch {epoch + 1}:")
        evaluate()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(OUT_DIR)
    tokenizer.save_pretrained(OUT_DIR)
    print(f"đã lưu {OUT_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--eval-size", type=int, default=600)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--lr", type=float, default=2e-5)
    args = parser.parse_args()
    rows = [json.loads(line) for line in DATA_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    train(rows, args)
