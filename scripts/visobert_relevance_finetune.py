"""Fine-tune ViSoBERT thành model ĐỘ LIÊN QUAN của bài viết (bài có nói về phim đích không) - model nhà trồng chạy sau
luật và trước AI (2026-10-22): bài model đủ chắc thì không tốn lời gọi AI, chỉ ca khó mới gọi Bee / Kira.

Dữ liệu: bài đã được AI gán nhãn (relevance_confidence trong (0, 1) - nhãn luật ghi 1.0 / NULL nên không học lại luật),
3 nhãn related / not_related / uncertain. Đầu vào là CẶP (tên phim + từ khoá, nội dung bài) vì cùng một bài có thể liên
quan phim này mà không liên quan phim khác. Bộ kiểm tra là các PHIM GIỮ RIÊNG (model không thấy lúc huấn luyện) - tuần
nào cũng có phim mới nên phải đo trên phim lạ. Khởi tạo từ model cảm xúc đã fine-tune trên bình luận phim
(models/visobert-cinemark, đã quen ngôn ngữ mạng xã hội về phim), thay đầu phân loại 3 nhãn mới.

Dữ liệu tải về data/visobert/relevance.jsonl (chỉ ở máy local, không commit); model lưu ở models/visobert-relevance/.

Cách dùng:
    python -m scripts.visobert_relevance_finetune                    # tải dữ liệu (lần đầu), huấn luyện, đo
    python -m scripts.visobert_relevance_finetune --per-class 10000 --epochs 1
    python -m scripts.visobert_relevance_finetune --refresh-data
    python -m scripts.visobert_relevance_finetune --two-class      # chỉ related / not_related (bỏ bài AI "uncertain")
    python -m scripts.visobert_relevance_finetune --two-class --relabel   # bài "uncertain" lấy nhãn AI gán lại
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from collections import Counter, defaultdict
from pathlib import Path

from app.repositories.d1.posts import MIN_CONTENT_LENGTH
from app.services.d1 import d1_query

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "data" / "visobert" / "relevance.jsonl"
RELABEL_FILE = ROOT / "data" / "visobert" / "relevance_relabel.jsonl"  # scripts/relevance_relabel_haiku.py
INIT_MODEL = ROOT / "models" / "visobert-cinemark"
OUT_DIR = ROOT / "models" / "visobert-relevance"
LABELS: tuple[str, ...] = ("related", "not_related", "uncertain")
MAX_TOKENS = 128  # đo 22/10/2026 trên Mac: 128 token ~1 giây/bước (lô 32), 256 token ~8,5 giây
CONTENT_CHARS = 600
FETCH_PAGE = 5000


async def download() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    last_id = ""
    while True:
        page = await d1_query(
            f"SELECT p.id, p.movie_id, m.title, substr(p.content, 1, {CONTENT_CHARS}) AS content, p.relevance_label, "
            "k.keyword FROM posts p JOIN movies m ON m.id = p.movie_id LEFT JOIN keywords k ON k.id = p.keyword_id "
            "WHERE p.relevance_confidence > 0 AND p.relevance_confidence < 1 "
            "AND p.relevance_label IN ('related', 'not_related', 'uncertain') "
            f"AND length(trim(p.content)) >= {MIN_CONTENT_LENGTH} AND p.id > ? ORDER BY p.id LIMIT ?",
            [last_id, FETCH_PAGE],
            timeout=120.0,
        )
        if page is None:
            raise RuntimeError("download_failed")
        if not page:
            break
        rows.extend(
            {
                "id": r["id"],
                "movie_id": r["movie_id"],
                "film": f"{r['title']} | {r['keyword'] or ''}".strip(" |"),
                "text": " ".join((r["content"] or "").split()),
                "label": r["relevance_label"],
            }
            for r in page
        )
        last_id = page[-1]["id"]
        print(f"  tải {len(rows)} bài…", flush=True)
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    return rows


def split_by_film(rows: list[dict[str, str]], args: argparse.Namespace, rng: random.Random):
    """Bộ kiểm tra = bài của một nhóm phim ngẫu nhiên (không lấy phim quá lớn - chúng là phần lớn dữ liệu huấn luyện) đủ
    ít nhất --eval-min-negative bài not_related; bài "related" của bộ kiểm tra lấy mẫu còn --eval-related."""
    by_film: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_film[row["movie_id"]].append(row)
    films = [f for f in by_film if len(by_film[f]) <= 0.15 * len(rows)]
    for _ in range(200):
        rng.shuffle(films)
        held: list[str] = []
        negatives = 0
        for film in films:
            if negatives >= args.eval_min_negative and len(held) >= args.eval_films:
                break
            held.append(film)
            negatives += sum(1 for r in by_film[film] if r["label"] == "not_related")
        eval_rows = [r for f in held for r in by_film[f]]
        if (
            negatives >= args.eval_min_negative
            and len(held) >= args.eval_films
            and set(LABELS) <= {r["label"] for r in eval_rows}
        ):
            held_set = set(held)
            related = [r for r in eval_rows if r["label"] == "related"]
            rng.shuffle(related)
            eval_rows = [r for r in eval_rows if r["label"] != "related"] + related[: args.eval_related]
            return eval_rows, [r for r in rows if r["movie_id"] not in held_set], held_set
    raise RuntimeError("không tách được bộ kiểm tra đủ bài not_related")


def train(rows: list[dict[str, str]], args: argparse.Namespace) -> None:
    import torch
    from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

    rng = random.Random(42)
    eval_rows, pool, held = split_by_film(rows, args, rng)
    # --two-class: bài AI "uncertain" của phim giữ riêng không chấm đúng/sai, chỉ đo model tự quyết bao nhiêu bài trong
    # số đó (khi chạy thật model vẫn gặp loại bài này - phần tự quyết ấy là rủi ro).
    unsure_rows = [r for r in eval_rows if r["label"] not in LABELS]
    eval_rows = [r for r in eval_rows if r["label"] in LABELS]
    pool = [r for r in pool if r["label"] in LABELS]
    by_label: dict[str, list[dict[str, str]]] = {label: [] for label in LABELS}
    for row in pool:
        by_label[row["label"]].append(row)
    for items in by_label.values():
        rng.shuffle(items)
    train_rows = [r for label in LABELS for r in by_label[label][: args.per_class]]
    rng.shuffle(train_rows)
    counts = Counter(r["label"] for r in train_rows)
    print(
        f"huấn luyện {len(train_rows)} {dict(counts)} · kiểm tra {len(eval_rows)} bài của {len(held)} phim giữ riêng "
        f"{dict(Counter(r['label'] for r in eval_rows))}"
    )

    device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer = PreTrainedTokenizerFast.from_pretrained(INIT_MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(
        INIT_MODEL,
        num_labels=len(LABELS),
        id2label=dict(enumerate(LABELS)),
        label2id={label: i for i, label in enumerate(LABELS)},
        ignore_mismatched_sizes=True,  # đầu phân loại cảm xúc 3 nhãn -> đầu độ liên quan mới
    ).to(device)
    label_id = {label: i for i, label in enumerate(LABELS)}
    # Nhãn ít mẫu (not_related) nặng hơn trong hàm mất mát để model không lờ nó đi.
    weights = torch.tensor([len(train_rows) / (len(LABELS) * max(1, counts[label])) for label in LABELS], device=device)
    loss_fn = torch.nn.CrossEntropyLoss(weight=weights)

    def batches(data: list[dict[str, str]], size: int):
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

    def predict(data: list[dict[str, str]]) -> list[list[float]]:
        probs: list[list[float]] = []
        with torch.inference_mode():
            for enc, _ in batches(data, 128):
                probs.extend(torch.softmax(model(**enc).logits.float(), dim=-1).cpu().tolist())
        return probs

    def evaluate() -> None:
        model.eval()
        probs = predict(eval_rows)
        unsure_top = [max(p) for p in predict(unsure_rows)] if unsure_rows else []
        gold = [label_id[r["label"]] for r in eval_rows]
        preds = [max(range(len(LABELS)), key=p.__getitem__) for p in probs]
        acc = sum(p == g for p, g in zip(preds, gold, strict=True)) / len(gold)
        matrix = Counter((LABELS[g], LABELS[p]) for p, g in zip(preds, gold, strict=True))
        print(f"  khớp nhãn AI {acc:.1%}  (dòng = nhãn AI, cột = model)")
        print(" " * 14 + "".join(f"{s:>13}" for s in LABELS))
        for a in LABELS:
            print(f"  {a:<12}" + "".join(f"{matrix[(a, b)]:>13}" for b in LABELS))
        # Ngưỡng tự tin: model chỉ tự quyết related / not_related khi đủ chắc, còn lại (kể cả "uncertain") để AI.
        print(
            "  ngưỡng | model tự quyết | khớp AI trên phần đó | not_related đúng (precision) | bài AI uncertain bị tự quyết"
        )
        for threshold in (0.6, 0.7, 0.8, 0.9, 0.95):
            decided = [
                (g, p)
                for g, p, pr in zip(gold, preds, probs, strict=True)
                if LABELS[p] != "uncertain" and pr[p] >= threshold
            ]
            if not decided:
                continue
            ok = sum(g == p for g, p in decided) / len(decided)
            neg = [(g, p) for g, p in decided if LABELS[p] == "not_related"]
            neg_prec = sum(g == p for g, p in neg) / len(neg) if neg else float("nan")
            unsure = (
                f"{sum(top >= threshold for top in unsure_top) / len(unsure_top):.1%} / {len(unsure_top)}"
                if unsure_top
                else "-"
            )
            print(
                f"  {threshold:>6} | {len(decided) / len(gold):>13.1%} | {ok:>20.1%} | {neg_prec:.1%} ({len(neg)} bài)"
                f" | {unsure}"
            )
        model.train()

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
            if step % 100 == 0:
                rate = step / (time.monotonic() - started)
                print(
                    f"  epoch {epoch + 1} bước {step}/{steps} loss {loss.item():.3f} · còn ~{(steps - step) / rate / 60:.0f} phút",
                    flush=True,
                )
        print(f"sau epoch {epoch + 1} ({(time.monotonic() - started) / 60:.0f} phút):")
        evaluate()
    args.out.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(args.out)
    tokenizer.save_pretrained(args.out)
    (args.out / "held_out_films.json").write_text(json.dumps(sorted(held)), encoding="utf-8")
    print(f"đã lưu {args.out}")


async def main(args: argparse.Namespace) -> None:
    global LABELS
    if args.two_class:
        LABELS = ("related", "not_related")
    if args.refresh_data or not DATA_FILE.exists():
        rows = await download()
    else:
        rows = [json.loads(line) for line in DATA_FILE.read_text(encoding="utf-8").split("\n") if line]
    if args.relabel:
        # Bài "uncertain" nhận nhãn AI gán lại khi điểm đủ rõ; điểm lưng chừng vẫn là "uncertain" (không học, chỉ đo).
        relabel = {}
        for line in RELABEL_FILE.read_text(encoding="utf-8").split("\n"):
            if line:
                item = json.loads(line)
                relabel[item["id"]] = item
        moved = Counter()
        for row in rows:
            item = relabel.get(row["id"])
            if row["label"] != "uncertain" or item is None:
                continue
            # score = xác suất bài nói về phim đích (prompt post_relevance), bất kể nhãn.
            score = float(item.get("confidence") or 0)
            if score >= args.relabel_min:
                row["label"] = "related"
            elif score <= 1 - args.relabel_min:
                row["label"] = "not_related"
            moved[row["label"]] += 1
        print(f"gán lại {sum(moved.values())} bài uncertain: {dict(moved)}")
    print(
        f"{len(rows)} bài có nhãn AI: {dict(Counter(r['label'] for r in rows))} · {len({r['movie_id'] for r in rows})} phim"
    )
    await asyncio.to_thread(train, rows, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--per-class", type=int, default=15000, help="tối đa ngần này mẫu mỗi nhãn (mặc định 15000)")
    parser.add_argument(
        "--eval-min-negative", type=int, default=300, help="bộ kiểm tra có ít nhất ngần này bài not_related"
    )
    parser.add_argument("--eval-films", type=int, default=8, help="bộ kiểm tra có ít nhất ngần này phim")
    parser.add_argument("--eval-related", type=int, default=3000, help="lấy mẫu ngần này bài related cho bộ kiểm tra")
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--refresh-data", action="store_true")
    parser.add_argument("--two-class", action="store_true", help='chỉ học related / not_related, bỏ bài AI "uncertain"')
    parser.add_argument(
        "--relabel", action="store_true", help="dùng nhãn gán lại của bài uncertain (relevance_relabel.jsonl)"
    )
    parser.add_argument("--relabel-min", type=float, default=0.7, help="độ chắc tối thiểu của nhãn gán lại")
    parser.add_argument("--out", type=Path, default=OUT_DIR)
    asyncio.run(main(parser.parse_args()))
