"""Gán nhãn cảm xúc hàng loạt bằng ViSoBERT (5CD-AI/Vietnamese-Sentiment-visobert) chạy ở máy local - không tốn
quota Jev/Kira, vài trăm câu mỗi giây trên GPU (MPS của Mac M1 / CUDA) hoặc CPU.

Chỉ ghi cột sentiment (+ sentiment_source='visobert', sentiment_confidence) cho dòng sentiment IS NULL và chỉ khi
model đủ chắc (--min-confidence). Câu model không chắc giữ NULL để Jev/Kira gán như cũ. Khía cạnh/giai đoạn
(aspects, audience_stage) không đụng tới: insights_classified_at vẫn NULL nên lượt bổ sung insights của Kira
(classify_pending(insights_only=True)) vẫn lấy được các dòng này sau. D1 là remote nên nhãn có hiệu lực ngay.

Cần torch + transformers (không có trong requirements.txt - chỉ cài ở máy chạy script):
    pip install torch transformers

Cách dùng:
    python -m scripts.visobert_label --eval 3000                 # so với nhãn Jev/Kira đã có, không ghi gì
    python -m scripts.visobert_label --dry-run --limit 50        # in thử nhãn, không ghi
    python -m scripts.visobert_label                              # mọi comment chưa có nhãn, mới nhất trước
    python -m scripts.visobert_label --table posts --days 30      # nội dung bài liên quan tới phim
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

from app.core.logging import get_logger
from app.repositories.d1.posts import MIN_CONTENT_LENGTH, RELEVANT_POST_SQL, ensure_post_voice_columns
from app.services.d1 import d1_query

logger = get_logger(__name__)

BASE_MODEL = "5CD-AI/Vietnamese-Sentiment-visobert"
# Bản fine-tune bằng nhãn Jev/Kira (scripts/visobert_finetune.py) - bản gốc chỉ khớp ~40% trên bình luận phim.
FINETUNED_DIR = Path(__file__).resolve().parent.parent / "models" / "visobert-cinemark"
LABELS = {"NEG": "negative", "POS": "positive", "NEU": "neutral"}
SOURCE = "visobert"
MAX_TOKENS = 256
POST_TEXT_LIMIT = 1200
PAGE_SIZE = 1000
SAVE_CHUNK = 400  # dòng mỗi câu UPDATE: dữ liệu đi qua 1 tham số JSON nên không vướng giới hạn 100 tham số của D1
SAVE_CONCURRENCY = 4
EXTRA_COLUMNS = (("sentiment_source", "TEXT"), ("sentiment_confidence", "REAL"))


class Classifier:
    def __init__(self, batch_size: int, model: str) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

        self._torch = torch
        if torch.cuda.is_available():
            self.device = "cuda"
        elif torch.backends.mps.is_available():
            self.device = "mps"
        else:
            self.device = "cpu"
        # AutoTokenizer của transformers 5 lỗi với tokenizer XLM-R của repo này - nạp thẳng tokenizer.json.
        self.tokenizer = PreTrainedTokenizerFast.from_pretrained(model)
        self.model = AutoModelForSequenceClassification.from_pretrained(model).to(self.device).eval()
        self.name = model
        self.id2label = {int(k): LABELS[v] for k, v in self.model.config.id2label.items()}
        self.batch_size = batch_size

    def predict(self, texts: list[str]) -> list[tuple[str, float]]:
        torch = self._torch
        # Sắp theo độ dài để mỗi lô đệm ít token thừa, rồi trả về đúng thứ tự ban đầu.
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        results: dict[int, tuple[str, float]] = {}
        with torch.inference_mode():
            for start in range(0, len(order), self.batch_size):
                idx = order[start : start + self.batch_size]
                enc = self.tokenizer(
                    [texts[i] for i in idx], padding=True, truncation=True, max_length=MAX_TOKENS, return_tensors="pt"
                ).to(self.device)
                probs = torch.softmax(self.model(**enc).logits.float(), dim=-1).cpu()
                conf, label = probs.max(dim=-1)
                for i, c, lab in zip(idx, conf.tolist(), label.tolist(), strict=True):
                    results[i] = (self.id2label[lab], round(c, 4))
        return [results[i] for i in range(len(texts))]


async def _ensure_columns(table: str) -> None:
    if table == "posts":
        await ensure_post_voice_columns()
    cols = await d1_query(f"PRAGMA table_info({table})")
    existing = {row.get("name") for row in cols or []}
    for column, kind in EXTRA_COLUMNS:
        if column not in existing:
            await d1_query(f"ALTER TABLE {table} ADD COLUMN {column} {kind}", quiet=True)
            logger.info("visobert_column_added", table=table, column=column)


def _select_sql(table: str, *, labeled: bool) -> str:
    cond = "IS NOT NULL" if labeled else "IS NULL"
    if table == "posts":
        return (
            f"SELECT p.id, substr(p.content, 1, {POST_TEXT_LIMIT}) AS message, p.sentiment FROM posts p "
            f"WHERE p.sentiment {cond} AND {RELEVANT_POST_SQL} AND length(trim(p.content)) >= {MIN_CONTENT_LENGTH}"
        )
    return (
        "SELECT c.id, c.message, c.sentiment FROM comments c "
        f"WHERE c.sentiment {cond} AND length(trim(c.message)) >= {MIN_CONTENT_LENGTH}"
    )


async def _select(
    table: str, limit: int, cursor: tuple[str, str] | None, since: str | None, platform: str | None
) -> list[dict[str, Any]]:
    """Một trang dòng chưa có nhãn, mới nhất trước, sau con trỏ (scraped_at, id) của trang trước - câu model không chắc
    vẫn NULL nên không thể chỉ chọn lại "sentiment IS NULL" từ đầu (mỗi trang sẽ đọc lại mọi câu đã bỏ qua)."""
    alias = "p" if table == "posts" else "c"
    sql = _select_sql(table, labeled=False).replace("SELECT ", f"SELECT {alias}.scraped_at, ", 1)
    params: list[Any] = []
    if cursor:
        sql += f" AND ({alias}.scraped_at < ? OR ({alias}.scraped_at = ? AND {alias}.id < ?))"
        params.extend([cursor[0], cursor[0], cursor[1]])
    if since:
        sql += f" AND {alias}.scraped_at >= ?"
        params.append(since)
    if platform:
        sql += f" AND {alias}.platform = ?"
        params.append(platform)
    sql += f" ORDER BY {alias}.scraped_at DESC, {alias}.id DESC LIMIT ?"
    params.append(limit)
    rows = await d1_query(sql, params, timeout=90.0)
    if rows is None:
        raise RuntimeError("visobert_select_failed")
    return rows


async def _save(table: str, labels: dict[str, tuple[str, float]]) -> int:
    """Một câu UPDATE cho cả khối qua json_each - chỉ dòng vẫn còn sentiment IS NULL (Jev/Kira ghi trước thì giữ)."""
    if not labels:
        return 0
    payload = json.dumps([{"id": cid, "s": s, "c": c} for cid, (s, c) in labels.items()])
    pick = f"(SELECT j.value ->> '{{key}}' FROM json_each(?1) j WHERE j.value ->> 'id' = {table}.id)"
    result = await d1_query(
        f"UPDATE {table} SET sentiment = {pick.format(key='s')}, sentiment_confidence = {pick.format(key='c')}, "
        f"sentiment_source = '{SOURCE}', sentiment_classified_at = CURRENT_TIMESTAMP "
        f"WHERE id IN (SELECT j.value ->> 'id' FROM json_each(?1) j) AND sentiment IS NULL",
        [payload],
        timeout=60.0,
    )
    if result is None:
        logger.warning("visobert_save_failed", table=table, count=len(labels))
        return 0
    return len(labels)


async def evaluate(clf: Classifier, table: str, n: int) -> None:
    """So với nhãn đã có (Jev/Kira, sentiment_source khác visobert): độ khớp theo từng ngưỡng tin cậy + ma trận nhầm."""
    cols = {row.get("name") for row in await d1_query(f"PRAGMA table_info({table})") or []}
    alias = "p" if table == "posts" else "c"
    not_ours = f" AND COALESCE({alias}.sentiment_source, '') != '{SOURCE}'" if "sentiment_source" in cols else ""
    rows = await d1_query(
        _select_sql(table, labeled=True) + not_ours + " ORDER BY random() LIMIT ?",
        [n],
        timeout=120.0,
    )
    rows = [r for r in rows or [] if r.get("sentiment") in LABELS.values()]
    started = time.monotonic()
    preds = clf.predict([r["message"] for r in rows])
    elapsed = time.monotonic() - started
    print(f"\n{len(rows)} dòng có nhãn · {elapsed:.1f}s · {len(rows) / max(elapsed, 1e-6):.0f} câu/giây · {clf.device}")
    print("phân bố nhãn cũ:", dict(Counter(r["sentiment"] for r in rows)))
    print("phân bố ViSoBERT:", dict(Counter(p for p, _ in preds)))
    print("\nngưỡng  phủ    khớp")
    for threshold in (0.0, 0.6, 0.7, 0.8, 0.9, 0.95):
        kept = [(r["sentiment"], p) for r, (p, c) in zip(rows, preds, strict=True) if c >= threshold]
        if kept:
            agree = sum(a == b for a, b in kept) / len(kept)
            print(f"{threshold:<6.2f}  {len(kept) / len(rows):5.1%}  {agree:5.1%}")
    print("\nma trận (hàng = nhãn cũ, cột = ViSoBERT):")
    sentiments = ("positive", "neutral", "negative")
    matrix = Counter((r["sentiment"], p) for r, (p, _) in zip(rows, preds, strict=True))
    print(" " * 10 + "".join(f"{s:>10}" for s in sentiments))
    for a in sentiments:
        print(f"{a:<10}" + "".join(f"{matrix[(a, b)]:>10}" for b in sentiments))


async def run(args: argparse.Namespace) -> None:
    from datetime import UTC, datetime, timedelta

    if args.model is None and not FINETUNED_DIR.exists() and not args.eval:
        raise SystemExit("Chưa có model fine-tune - chạy python -m scripts.visobert_finetune trước (hoặc --model).")
    clf = Classifier(args.batch_size, args.model or (str(FINETUNED_DIR) if FINETUNED_DIR.exists() else BASE_MODEL))
    logger.info("visobert_model", model=clf.name, device=clf.device)
    if args.eval:
        await evaluate(clf, args.table, args.eval)
        return
    if not args.dry_run:
        await _ensure_columns(args.table)
    since = (datetime.now(tz=UTC) - timedelta(days=args.days)).isoformat() if args.days else None
    totals: Counter[str] = Counter()
    cursor: tuple[str, str] | None = None
    sem = asyncio.Semaphore(SAVE_CONCURRENCY)
    pending: list[asyncio.Task[int]] = []
    started = time.monotonic()

    async def save_chunk(chunk: dict[str, tuple[str, float]]) -> int:
        async with sem:
            return await _save(args.table, chunk)

    while args.limit is None or totals["selected"] < args.limit:
        page = PAGE_SIZE if args.limit is None else min(PAGE_SIZE, args.limit - totals["selected"])
        rows = await _select(args.table, page, cursor, since, args.platform)
        if not rows:
            break
        cursor = (str(rows[-1]["scraped_at"] or ""), str(rows[-1]["id"]))
        totals["selected"] += len(rows)
        preds = await asyncio.to_thread(clf.predict, [row["message"] for row in rows])
        confident: dict[str, tuple[str, float]] = {}
        for row, (label, conf) in zip(rows, preds, strict=True):
            if conf >= args.min_confidence:
                confident[row["id"]] = (label, conf)
                totals[label] += 1
            else:
                totals["low_confidence"] += 1  # để Jev/Kira
        if args.dry_run:
            for row, (label, conf) in list(zip(rows, preds, strict=True))[:50]:
                print(f"{label:>8} {conf:.2f}  {' '.join(str(row['message']).split())[:110]}")
            break
        items = list(confident.items())
        for i in range(0, len(items), SAVE_CHUNK):
            pending.append(asyncio.create_task(save_chunk(dict(items[i : i + SAVE_CHUNK]))))
        # Không để quá nhiều câu UPDATE chờ: lô kế tiếp chọn lại sentiment IS NULL nên phải ghi xong trước.
        totals["saved"] += sum(await asyncio.gather(*pending))
        pending.clear()
        elapsed = max(time.monotonic() - started, 1)
        logger.info(
            "visobert_label_page",
            table=args.table,
            page=len(rows),
            totals=dict(totals),
            per_minute=round(totals["saved"] / elapsed * 60),
        )
        if totals["saved"] == 0 and totals["selected"] >= PAGE_SIZE * 2:
            logger.error("visobert_label_nothing_saved", hint="D1 lỗi hoặc mọi câu dưới ngưỡng tin cậy")
            break
    logger.info(
        "visobert_label_finished",
        table=args.table,
        **totals,
        dry_run=args.dry_run,
        minutes=round((time.monotonic() - started) / 60, 1),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", choices=("comments", "posts"), default="comments")
    parser.add_argument("--model", help="đường dẫn/tên model (mặc định models/visobert-cinemark)")
    parser.add_argument("--eval", type=int, metavar="N", help="so N dòng đã có nhãn với ViSoBERT, không ghi gì")
    parser.add_argument("--min-confidence", type=float, default=0.85, help="chỉ ghi khi model chắc từ mức này (mặc định 0.85: khớp ~86%% với nhãn Jev/Kira, phủ ~56%%)")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--days", type=float, help="chỉ dòng thu thập trong N ngày gần nhất")
    parser.add_argument("--platform", help="chỉ một nền tảng (facebook/threads/tiktok)")
    parser.add_argument("--limit", type=int, help="tối đa ngần này dòng lượt này")
    parser.add_argument("--dry-run", action="store_true", help="gán nhãn một trang và in ra, không ghi")
    asyncio.run(run(parser.parse_args()))
