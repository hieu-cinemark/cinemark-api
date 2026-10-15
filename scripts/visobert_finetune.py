"""Fine-tune ViSoBERT sentiment bằng chính nhãn Jev/Kira đã có trong D1 - bản 5CD-AI có sẵn (huấn luyện trên đánh giá
sản phẩm) chỉ khớp ~40% với nhãn của mình trên bình luận phim (đo 2026-10-15: gần như không bao giờ trả neutral, chấm
"positive 1.00" cho câu chê thẳng). Model sau fine-tune lưu ở models/visobert-cinemark/ (không commit), dùng bởi
scripts/visobert_label.py.

Dữ liệu tải về data/visobert/labeled.jsonl (bình luận thật - chỉ ở máy local, không commit). Bộ kiểm tra là
--eval-size dòng ngẫu nhiên tách riêng trước khi lấy mẫu huấn luyện, giữ đúng phân bố nhãn thật.

Cách dùng:
    python -m scripts.visobert_finetune                      # tải dữ liệu (lần đầu), huấn luyện, đo trên bộ kiểm tra
    python -m scripts.visobert_finetune --per-class 20000 --epochs 2
    python -m scripts.visobert_finetune --refresh-data       # tải lại dữ liệu từ D1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from collections import Counter
from pathlib import Path

from app.repositories.d1.posts import MIN_CONTENT_LENGTH
from app.services.d1 import d1_query

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "data" / "visobert" / "labeled.jsonl"
OUT_DIR = ROOT / "models" / "visobert-cinemark"
BASE_MODEL = "5CD-AI/Vietnamese-Sentiment-visobert"
LABELS = ("negative", "positive", "neutral")  # đúng thứ tự id2label của BASE_MODEL (NEG, POS, NEU)
MAX_TOKENS = 128
FETCH_PAGE = 10000


async def download() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    last_id = ""
    while True:
        page = await d1_query(
            "SELECT id, message, sentiment FROM comments WHERE sentiment IN ('positive','negative','neutral') "
            f"AND length(trim(message)) >= {MIN_CONTENT_LENGTH} AND id > ? "
            "AND COALESCE(sentiment_source, '') != 'visobert' ORDER BY id LIMIT ?",
            [last_id, FETCH_PAGE],
            timeout=120.0,
        )
        if page is None:
            raise RuntimeError("download_failed")
        if not page:
            break
        rows.extend({"id": r["id"], "text": r["message"], "label": r["sentiment"]} for r in page)
        last_id = page[-1]["id"]
        print(f"  tải {len(rows)} dòng…", flush=True)
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    return rows


async def ensure_source_column() -> None:
    cols = {row.get("name") for row in await d1_query("PRAGMA table_info(comments)") or []}
    if "sentiment_source" not in cols:
        await d1_query("ALTER TABLE comments ADD COLUMN sentiment_source TEXT", quiet=True)
        await d1_query("ALTER TABLE comments ADD COLUMN sentiment_confidence REAL", quiet=True)


def train(rows: list[dict[str, str]], args: argparse.Namespace) -> None:
    import torch
    from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

    rng = random.Random(42)
    rng.shuffle(rows)
    eval_rows, pool = rows[: args.eval_size], rows[args.eval_size :]
    by_label: dict[str, list[dict[str, str]]] = {label: [] for label in LABELS}
    for row in pool:
        by_label[row["label"]].append(row)
    train_rows = [r for label in LABELS for r in by_label[label][: args.per_class]]
    rng.shuffle(train_rows)
    print(f"huấn luyện {len(train_rows)} ({dict(Counter(r['label'] for r in train_rows))}) · kiểm tra {len(eval_rows)}")

    device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer = PreTrainedTokenizerFast.from_pretrained(BASE_MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(BASE_MODEL).to(device)
    label_id = {label: i for i, label in enumerate(LABELS)}

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
        acc = sum(p == g for p, g in zip(preds, gold, strict=True)) / len(gold)
        matrix = Counter((LABELS[g], LABELS[p]) for p, g in zip(preds, gold, strict=True))
        print(f"  khớp {acc:.1%}")
        order = ("positive", "neutral", "negative")
        print(" " * 12 + "".join(f"{s:>10}" for s in order))
        for a in order:
            print(f"  {a:<10}" + "".join(f"{matrix[(a, b)]:>10}" for b in order))
        model.train()
        return acc

    print("trước fine-tune:")
    evaluate()
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
            loss = model(**enc, labels=labels).loss
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()
            optimizer.zero_grad()
            step += 1
            if step % 100 == 0:
                rate = step / (time.monotonic() - started)
                print(f"  epoch {epoch + 1} bước {step}/{steps} loss {loss.item():.3f} · còn ~{(steps - step) / rate / 60:.0f} phút", flush=True)
        print(f"sau epoch {epoch + 1}:")
        evaluate()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(OUT_DIR)
    tokenizer.save_pretrained(OUT_DIR)
    print(f"đã lưu {OUT_DIR}")


async def main(args: argparse.Namespace) -> None:
    await ensure_source_column()
    if args.refresh_data or not DATA_FILE.exists():
        rows = await download()
    else:
        rows = [json.loads(line) for line in DATA_FILE.read_text(encoding="utf-8").split("\n") if line]
    print(f"{len(rows)} bình luận có nhãn: {dict(Counter(r['label'] for r in rows))}")
    await asyncio.to_thread(train, rows, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--per-class", type=int, default=20000, help="tối đa ngần này mẫu mỗi nhãn (mặc định 20000)")
    parser.add_argument("--eval-size", type=int, default=3000)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--refresh-data", action="store_true")
    asyncio.run(main(parser.parse_args()))
