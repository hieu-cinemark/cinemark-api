"""Tinh chỉnh model độ liên quan (ViSoBERT, related / not_related) bằng bộ nhãn rà soát tay các bài đang ẩn (2026-11-19).

Vì sao: model đang chạy học từ nhãn AI cũ; các bài khó nhất (bài `uncertain` của TikTok/Threads: hashtag trần, phim loạt,
sự kiện) lại không có nhãn đúng. Bộ này là 628 bài đã được rà soát (related / not_related / uncertain) - bài `uncertain`
không dùng để học (chỉ để đo model tự quyết bao nhiêu trong số đó, đó là rủi ro).

Cách làm: tiếp tục huấn luyện từ model đang chạy, trộn thêm một phần dữ liệu cũ (replay) để không quên. Đo trên các PHIM GIỮ
RIÊNG của bộ rà soát, SO với chính model đang chạy (baseline) - chỉ khi hơn rõ mới nên thay.

    python -m scripts.ml.relevance_hand_finetune --build /path/bai_an_da_gan_tay.csv   # dựng data/visobert/relevance_hand.jsonl
    python -m scripts.ml.relevance_hand_finetune --eval-only                            # chỉ đo model đang chạy
    python -m scripts.ml.relevance_hand_finetune                                        # train với phim giữ riêng, đo
    python -m scripts.ml.relevance_hand_finetune --final                                # train trên toàn bộ, lưu model

Model mới lưu ở models/visobert-relevance-hand/ - KHÔNG tự thay cấu hình chạy thật (settings.visobert_relevance_model_dir).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import json
import random
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "visobert"
HAND_FILE = DATA_DIR / "relevance_hand.jsonl"
BASE_FILE = DATA_DIR / "relevance.jsonl"
RELABEL_FILE = DATA_DIR / "relevance_relabel.jsonl"
PROD_MODEL = ROOT / "models" / "visobert-relevance-relabel"
OUT_DIR = ROOT / "models" / "visobert-relevance-hand"
LABELS = ("related", "not_related")
MAX_TOKENS = 128
CONTENT_CHARS = 600
BACKUP_RUNS = ("backups/purge/2026-11-19/7a165953", "backups/purge/2026-11-19/bef45ca4")


async def build(csv_path: Path) -> None:
    """Dựng bộ dữ liệu từ CSV rà soát: nội dung đầy đủ + "tên phim | từ khoá" như lúc huấn luyện/chạy thật. Bài đã xoá khỏi D1
    (not_related) lấy từ bản sao lưu R2 của lượt dọn."""
    from app.clients import lake
    from app.clients.d1 import d1_query

    rows = list(csv.DictReader(csv_path.open(encoding="utf-8-sig")))
    ids = [r["post_id"] for r in rows]
    posts: dict[str, dict] = {}
    for i in range(0, len(ids), 90):
        part = ids[i : i + 90]
        found = await d1_query(
            f"SELECT id, movie_id, keyword_id, content, posted_at FROM posts WHERE id IN ({','.join('?' * len(part))})",
            part,
            timeout=60,
        )
        posts.update({r["id"]: r for r in found or []})
    missing = {i for i in ids if i not in posts}
    for run in BACKUP_RUNS:
        for batch in range(12):
            if not missing:
                break
            try:
                body = await lake.get_object(f"{run}/{batch:03d}-posts.jsonl.gz")
            except Exception:  # noqa: BLE001 - hết lô của lượt sao lưu này
                break
            for line in gzip.decompress(body).decode().splitlines():
                if line.strip():
                    row = json.loads(line)
                    if row["id"] in missing:
                        posts[row["id"]] = row
                        missing.discard(row["id"])
    movies = {r["id"]: r["title"] for r in await d1_query("SELECT id, title FROM movies", timeout=60) or []}
    keywords = {r["id"]: r["keyword"] for r in await d1_query("SELECT id, keyword FROM keywords", timeout=60) or []}
    out = []
    for r in rows:
        p = posts.get(r["post_id"])
        if p is None:
            continue
        title = movies.get(p["movie_id"]) or r["phim"]
        film = f"{title} | {keywords.get(p.get('keyword_id')) or ''}".strip(" |")
        out.append(
            {
                "id": p["id"],
                "movie_id": p["movie_id"],
                "film": film,
                "text": " ".join((p.get("content") or r["noi_dung_200_ky_tu_dau"])[:CONTENT_CHARS].split()),
                "label": r["nhan_gan_tay"],
                "posted_at": p.get("posted_at"),
            }
        )
    HAND_FILE.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in out), encoding="utf-8")
    print(
        f"đã dựng {len(out)}/{len(rows)} bài ({len(missing)} không tìm thấy): {dict(Counter(x['label'] for x in out))}"
    )


def load_base(per_class: int, rng: random.Random) -> list[dict]:
    """Dữ liệu cũ (2 nhãn) để trộn lại, áp cùng cách gán lại bài uncertain như lúc train model đang chạy."""
    rows = [json.loads(line) for line in BASE_FILE.read_text(encoding="utf-8").split("\n") if line]
    relabel = {}
    for line in RELABEL_FILE.read_text(encoding="utf-8").split("\n"):
        if line:
            item = json.loads(line)
            relabel[item["id"]] = float(item.get("confidence") or 0)
    by: dict[str, list[dict]] = {label: [] for label in LABELS}
    for row in rows:
        label = row["label"]
        if label == "uncertain" and row["id"] in relabel:
            score = relabel[row["id"]]
            label = "related" if score >= 0.7 else "not_related" if score <= 0.3 else "uncertain"
        if label in by:
            by[label].append({**row, "label": label})
    for items in by.values():
        rng.shuffle(items)
    return [r for label in LABELS for r in by[label][:per_class]]


def run(args: argparse.Namespace) -> None:
    """Đo model đang chạy (baseline) trên phim giữ riêng của bộ rà soát, rồi tiếp tục huấn luyện từ nó với nhãn rà soát cộng dữ
    liệu cũ và đo lại sau từng epoch. Chỉ lưu model khi --final; không bao giờ đổi cấu hình chạy thật."""
    import torch
    from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

    rng = random.Random(7)
    hand = [json.loads(line) for line in HAND_FILE.read_text(encoding="utf-8").split("\n") if line]
    unsure = [r for r in hand if r["label"] == "uncertain"]
    labeled = [r for r in hand if r["label"] in LABELS]
    films = sorted({r["movie_id"] for r in labeled})
    rng.shuffle(films)
    held = set() if args.final else set(films[: max(3, len(films) // 4)])
    eval_rows = [r for r in labeled if r["movie_id"] in held]
    eval_unsure = [r for r in unsure if r["movie_id"] in held]
    train_hand = [r for r in labeled if r["movie_id"] not in held]
    print(
        f"nhãn rà soát: học {len(train_hand)} {dict(Counter(r['label'] for r in train_hand))} · "
        f"kiểm tra {len(eval_rows)} {dict(Counter(r['label'] for r in eval_rows))} ({len(held)} phim giữ riêng) · "
        f"uncertain để đo rủi ro {len(eval_unsure)}"
    )

    device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer = PreTrainedTokenizerFast.from_pretrained(PROD_MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(PROD_MODEL).to(device)
    id2label = {int(k): v for k, v in model.config.id2label.items()}
    label_id = {v: k for k, v in id2label.items()}

    def batches(data: list[dict], size: int):
        for i in range(0, len(data), size):
            chunk = data[i : i + size]
            enc = tokenizer(
                [r["film"] for r in chunk],
                [r["text"] for r in chunk],
                padding=True,
                truncation="only_second",
                max_length=MAX_TOKENS,
                return_tensors="pt",
            ).to(device)
            yield enc, torch.tensor([label_id.get(r["label"], 0) for r in chunk], device=device)

    def predict(data: list[dict]) -> list[list[float]]:
        out: list[list[float]] = []
        model.eval()
        with torch.inference_mode():
            for enc, _ in batches(data, 64):
                out.extend(torch.softmax(model(**enc).logits.float(), dim=-1).cpu().tolist())
        return out

    def report(title: str) -> None:
        if not eval_rows:
            return
        probs = predict(eval_rows)
        gold = [label_id[r["label"]] for r in eval_rows]
        pred = [max(range(len(p)), key=p.__getitem__) for p in probs]
        acc = sum(a == b for a, b in zip(gold, pred, strict=True)) / len(gold)
        print(f"\n[{title}] đúng {acc:.1%} trên {len(gold)} bài của phim giữ riêng")
        un = predict(eval_unsure) if eval_unsure else []
        for th_rel, th_not in ((0.9, 0.95), (0.8, 0.9), (0.7, 0.8)):
            decided = [
                (g, p)
                for g, p, pr in zip(gold, pred, probs, strict=True)
                if pr[p] >= (th_rel if id2label[p] == "related" else th_not)
            ]
            ok = sum(g == p for g, p in decided) / len(decided) if decided else float("nan")
            risky = sum(
                1
                for pr in un
                if pr[max(range(len(pr)), key=pr.__getitem__)]
                >= (th_rel if id2label[max(range(len(pr)), key=pr.__getitem__)] == "related" else th_not)
            )
            print(
                f"  ngưỡng related>={th_rel} not_related>={th_not}: tự quyết {len(decided) / len(gold):.0%}, đúng {ok:.1%}"
                f" · bài uncertain bị tự quyết {risky}/{len(un)}"
            )

    report("model đang chạy (baseline)")
    if args.eval_only:
        return
    replay = load_base(args.replay, rng)
    train_rows = replay + train_hand * args.hand_weight
    rng.shuffle(train_rows)
    counts = Counter(r["label"] for r in train_rows)
    print(
        f"\nhuấn luyện {len(train_rows)} {dict(counts)} (nhãn rà soát nhân {args.hand_weight}, dữ liệu cũ {len(replay)})"
    )
    weights = torch.tensor(
        [len(train_rows) / (len(LABELS) * max(1, counts[id2label[i]])) for i in range(len(id2label))], device=device
    )
    loss_fn = torch.nn.CrossEntropyLoss(weight=weights)
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.01)
    steps = args.epochs * ((len(train_rows) + args.batch_size - 1) // args.batch_size)
    scheduler = torch.optim.lr_scheduler.LambdaLR(
        optimizer, lambda s: min(1.0, s / max(1, steps * 0.06)) * max(0.0, (steps - s) / steps)
    )
    step, started = 0, time.monotonic()
    for epoch in range(args.epochs):
        model.train()
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
                print(
                    f"  epoch {epoch + 1} bước {step}/{steps} loss {loss.item():.3f} · còn ~{(steps - step) / (step / (time.monotonic() - started)) / 60:.0f} phút",
                    flush=True,
                )
        report(f"model mới sau epoch {epoch + 1}")
    if args.final:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        model.save_pretrained(OUT_DIR)
        tokenizer.save_pretrained(OUT_DIR)
        print(f"\nđã lưu {OUT_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build", type=Path, help="CSV rà soát -> dựng data/visobert/relevance_hand.jsonl")
    parser.add_argument("--eval-only", action="store_true")
    parser.add_argument(
        "--final", action="store_true", help="train trên toàn bộ bộ rà soát (không giữ phim riêng) và lưu model"
    )
    parser.add_argument("--replay", type=int, default=3000, help="số bài dữ liệu cũ mỗi nhãn trộn lại")
    parser.add_argument("--hand-weight", type=int, default=4, help="nhân bản nhãn rà soát bấy nhiêu lần")
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--lr", type=float, default=1.5e-5)
    cli = parser.parse_args()
    if cli.build:
        asyncio.run(build(cli.build))
    else:
        run(cli)
