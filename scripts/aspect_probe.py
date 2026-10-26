"""Thử nhận diện khía cạnh (Review Angles) bằng embedding + lớp phân loại tuyến tính, không gọi AI cho từng comment.

Từ 23/10/2026 comment không còn qua AI (chỉ ViSoBERT gán cảm xúc) nên gần như không comment mới nào có khía cạnh. Dữ
liệu huấn luyện: các comment đã được AI gán khía cạnh trước đó (cột aspects "key:+/-", app/ai/aspects.py) - mẫu âm là
comment AI đã xử lý mà không thấy khía cạnh nào (aspects = '[]').

Các bước (mỗi bước cache ra ~/cinemark-ml/aspects/, chạy lại chỉ làm phần còn thiếu):
  1. download  - comment có nhãn từ D1 (mọi mẫu dương + mẫu âm lấy ngẫu nhiên)
  2. embed     - vector của từng comment theo từng model (mean pooling, chuẩn hoá L2)
  3. train     - một lớp tuyến tính nhiều nhãn trên vector; chia train/val/test THEO BÀI (comment cùng bài rất giống nhau
                 - chia theo comment cho điểm ảo); ngưỡng từng nhãn chọn trên val, báo P/R/F1 trên test
  4. speed     - số comment/giây trên CPU 2 luồng (giống VPS: OMP_NUM_THREADS=2)

Cách dùng:
    python -m scripts.aspect_probe                          # cả 4 bước, mọi model trong MODELS
    python -m scripts.aspect_probe --models e5-small --neg 60000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from collections import Counter
from pathlib import Path

import numpy as np
import torch

from app.ai.aspects import parse_aspects
from app.clients.d1 import d1_query

DATA_DIR = Path.home() / "cinemark-ml" / "aspects"
MODELS = {
    # name: (huggingface id, tiền tố cho mỗi câu, độ dài tối đa)
    "e5-small": ("intfloat/multilingual-e5-small", "query: ", 128),
    "visobert": ("5CD-AI/Vietnamese-Sentiment-visobert", "", 128),
}
MIN_SUPPORT = 200  # nhãn ít mẫu hơn chừng này trong toàn bộ dữ liệu thì bỏ (vd. san_phim:+ chỉ 16)
SEED = 13


# --- 1. download ----------------------------------------------------------------------------------------------------


async def _fetch(where: str, limit: int | None) -> list[dict]:
    rows: list[dict] = []
    last = ""
    while True:
        page = await d1_query(
            f"""SELECT id, post_id, message, aspects, audience_stage FROM comments
            WHERE insights_classified_at IS NOT NULL AND {where} AND id > ?1 AND length(message) >= 2
            ORDER BY id LIMIT 2000""",
            [last],
            timeout=120.0,
        )
        if not page:
            break
        rows.extend(page)
        last = page[-1]["id"]
        print(f"  {len(rows)} dòng", end="\r", flush=True)
        if limit and len(rows) >= limit * 3:  # mẫu âm: lấy dư rồi chọn ngẫu nhiên
            break
    print()
    return rows


async def _fetch_all(neg: int) -> tuple[list[dict], list[dict]]:
    print("tải mẫu dương (có khía cạnh)...")
    pos = await _fetch("aspects IS NOT NULL AND aspects NOT IN ('[]', '')", None)
    print("tải mẫu âm (AI xử lý, không khía cạnh)...")
    return pos, await _fetch("aspects = '[]'", neg)


def download(neg: int) -> list[dict]:
    path = DATA_DIR / "comments.jsonl"
    if path.exists():
        return [json.loads(line) for line in path.open()]
    pos, negatives = asyncio.run(_fetch_all(neg))  # một vòng lặp: client D1 dùng chung gắn với vòng lặp đầu tiên
    random.Random(SEED).shuffle(negatives)
    rows = []
    for r in pos + negatives[:neg]:
        labels = parse_aspects(json.loads(r["aspects"])) if r["aspects"] not in ("", "[]") else []
        rows.append(
            {
                "id": r["id"],
                "post_id": r["post_id"],
                "text": r["message"],
                "labels": labels,
                "stage": r["audience_stage"],
            }
        )
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return rows


# --- 2. embed -------------------------------------------------------------------------------------------------------


def _load(name: str, device: str):
    from transformers import AutoModel, AutoTokenizer, PreTrainedTokenizerFast

    model_id = MODELS[name][0]
    # ViSoBERT: AutoTokenizer của transformers 5 nạp hỏng vocab dạng dict (xem scripts/visobert_finetune.py).
    tokenizer = (PreTrainedTokenizerFast if name == "visobert" else AutoTokenizer).from_pretrained(model_id)
    return tokenizer, AutoModel.from_pretrained(model_id).to(device).eval()


def _encode(tokenizer, model, texts: list[str], prefix: str, max_len: int, device: str, batch: int = 128) -> np.ndarray:
    out = []
    with torch.inference_mode():
        for start in range(0, len(texts), batch):
            enc = tokenizer(
                [prefix + t for t in texts[start : start + batch]],
                padding=True,
                truncation=True,
                max_length=max_len,
                return_tensors="pt",
            ).to(device)
            hidden = model(**enc).last_hidden_state
            mask = enc["attention_mask"].unsqueeze(-1).to(hidden.dtype)
            pooled = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1)
            out.append(torch.nn.functional.normalize(pooled, dim=-1).float().cpu().numpy())
    return np.concatenate(out)


def embed(rows: list[dict], name: str) -> np.ndarray:
    path = DATA_DIR / f"emb-{name}.npy"
    if path.exists():
        return np.load(path)
    device = "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer, model = _load(name, device)
    _, prefix, max_len = MODELS[name]
    started = time.monotonic()
    vectors = _encode(tokenizer, model, [r["text"] for r in rows], prefix, max_len, device)
    print(f"  {name}: {len(rows)} comment trong {time.monotonic() - started:.0f}s ({device})")
    np.save(path, vectors.astype(np.float16))
    return vectors


# --- 3. train -------------------------------------------------------------------------------------------------------


def _split(rows: list[dict]) -> np.ndarray:
    """0 train / 1 val / 2 test theo post_id (70/15/15)."""
    posts = sorted({r["post_id"] for r in rows})
    random.Random(SEED).shuffle(posts)
    cut1, cut2 = int(len(posts) * 0.7), int(len(posts) * 0.85)
    part = {p: (0 if i < cut1 else 1 if i < cut2 else 2) for i, p in enumerate(posts)}
    return np.array([part[r["post_id"]] for r in rows])


def train(rows: list[dict], vectors: np.ndarray, name: str) -> dict:
    support = Counter(label for r in rows for label in r["labels"])
    labels = sorted(label for label, n in support.items() if n >= MIN_SUPPORT)
    index = {label: i for i, label in enumerate(labels)}
    y = np.zeros((len(rows), len(labels)), dtype=np.float32)
    for i, r in enumerate(rows):
        for label in r["labels"]:
            if label in index:
                y[i, index[label]] = 1
    split = _split(rows)
    x = torch.tensor(vectors, dtype=torch.float32)
    yt = torch.tensor(y)
    tr, va, te = (torch.tensor(split == k) for k in (0, 1, 2))

    torch.manual_seed(SEED)
    head = torch.nn.Linear(x.shape[1], len(labels))
    pos = yt[tr].sum(0)
    pos_weight = ((tr.sum() - pos) / pos.clamp(min=1)).clamp(max=20)
    loss_fn = torch.nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    opt = torch.optim.AdamW(head.parameters(), lr=3e-3, weight_decay=1e-4)
    best, best_state, patience = 1e9, None, 0
    for _epoch in range(400):
        head.train()
        opt.zero_grad()
        loss = loss_fn(head(x[tr]), yt[tr])
        loss.backward()
        opt.step()
        head.eval()
        with torch.no_grad():
            val = loss_fn(head(x[va]), yt[va]).item()
        if val < best - 1e-4:
            best, best_state, patience = val, {k: v.clone() for k, v in head.state_dict().items()}, 0
        else:
            patience += 1
            if patience >= 20:
                break
    assert best_state is not None
    head.load_state_dict(best_state)

    with torch.no_grad():
        p_val = torch.sigmoid(head(x[va])).numpy()
        p_test = torch.sigmoid(head(x[te])).numpy()
    y_val, y_test = y[split == 1], y[split == 2]

    def prf(pred: np.ndarray, gold: np.ndarray) -> tuple[float, float, float]:
        tp = float((pred * gold).sum())
        p = tp / max(pred.sum(), 1)
        r = tp / max(gold.sum(), 1)
        return p, r, (2 * p * r / (p + r) if p + r else 0.0)

    thresholds = []
    for j in range(len(labels)):
        grid = np.linspace(0.1, 0.9, 33)
        thresholds.append(max(grid, key=lambda t: prf((p_val[:, j] >= t).astype(float), y_val[:, j])[2]))
    thresholds = np.array(thresholds)
    pred = (p_test >= thresholds).astype(float)

    print(
        f"\n=== {name}: test {int(te.sum())} comment ({len({r['post_id'] for r, s in zip(rows, split, strict=True) if s == 2})} bài) ==="
    )
    print(f"{'nhãn':16s} {'mẫu test':>8s} {'P':>6s} {'R':>6s} {'F1':>6s}")
    per_label = {}
    for j, label in enumerate(labels):
        p, r, f = prf(pred[:, j], y_test[:, j])
        per_label[label] = round(f, 3)
        print(f"{label:16s} {int(y_test[:, j].sum()):8d} {p:6.2f} {r:6.2f} {f:6.2f}")
    micro = prf(pred, y_test)
    has_any = prf(pred.max(1), y_test.max(1))
    print(f"{'micro':16s} {int(y_test.sum()):8d} {micro[0]:6.2f} {micro[1]:6.2f} {micro[2]:6.2f}")
    print(f"{'có khía cạnh':16s} {int(y_test.max(1).sum()):8d} {has_any[0]:6.2f} {has_any[1]:6.2f} {has_any[2]:6.2f}")
    print(f"bỏ (ít hơn {MIN_SUPPORT} mẫu): {[label for label in support if label not in index]}")
    return {
        "model": name,
        "micro_f1": round(micro[2], 3),
        "has_aspect_f1": round(has_any[2], 3),
        "per_label": per_label,
    }


# --- 4. speed -------------------------------------------------------------------------------------------------------


def speed(rows: list[dict], name: str, n: int = 1000) -> float:
    torch.set_num_threads(2)
    tokenizer, model = _load(name, "cpu")
    _, prefix, max_len = MODELS[name]
    texts = [r["text"] for r in rows[:n]]
    _encode(tokenizer, model, texts[:64], prefix, max_len, "cpu", batch=32)  # khởi động
    started = time.monotonic()
    _encode(tokenizer, model, texts, prefix, max_len, "cpu", batch=32)
    rate = len(texts) / (time.monotonic() - started)
    print(f"  {name}: {rate:.0f} comment/giây trên CPU 2 luồng -> 200.000 comment ~ {200_000 / rate / 60:.0f} phút")
    return rate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--models", default=",".join(MODELS))
    parser.add_argument("--neg", type=int, default=60000, help="số mẫu âm lấy ngẫu nhiên")
    args = parser.parse_args()
    rows = download(args.neg)
    print(
        f"{len(rows)} comment, {sum(bool(r['labels']) for r in rows)} có khía cạnh, {len({r['post_id'] for r in rows})} bài"
    )
    results = []
    for name in args.models.split(","):
        vectors = embed(rows, name)
        result = train(rows, vectors, name)
        result["comments_per_second_cpu2"] = round(speed(rows, name))
        results.append(result)
    (DATA_DIR / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=1))
    print("\nkết quả:", DATA_DIR / "results.json")


if __name__ == "__main__":
    main()
