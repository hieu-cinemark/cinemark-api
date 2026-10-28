"""Gán khía cạnh (cột aspects, dạng ["key:+", "key:-"] - app/ai/aspects.py) cho comment bằng model ViSoBERT nhiều nhãn
đã fine-tune (scripts/ml/aspect_finetune.py) - dữ liệu cho Review Angles: xếp hạng các góc được nhắc tới của phim.

Từ 23/10/2026 comment không còn qua AI nên gần như không comment mới nào có khía cạnh. Model này thay phần đó, chạy ở máy
(VPS: systemd timer deploy/systemd/cinemark-aspects.*). Ngưỡng từng nhãn = count_thresholds (chọn để SỐ ĐẾM sát thực tế:
thứ hạng khía cạnh khớp nhãn thật 0,95 trên tập test - xem aspect_finetune --calibrate).

Chỉ ghi dòng aspects IS NULL (không đè nhãn AI cũ), kèm aspects_source='model'. Comment không mang nội dung (sticker,
emoji, tag tên - rule_sentiment) hoặc không có khía cạnh nào được ghi [] để lượt sau không chọn lại.

Cần torch + transformers (requirements-ml.txt) và thư mục model ở models/visobert-aspects (không có trong git).

Cách dùng:
    python -m scripts.jobs.aspect_label --dry-run --limit 50     # in thử, không ghi
    python -m scripts.jobs.aspect_label --days 0.25              # comment thu trong 6 giờ qua (timer)
    python -m scripts.jobs.aspect_label --table posts --days 30  # nội dung bài liên quan tới phim
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

from app.ai.tasks.sentiment_rules import rule_sentiment, strip_placeholders
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.repositories.d1.store import d1_query

logger = get_logger(__name__)

MODEL_DIR = Path(__file__).resolve().parents[2] / "models" / "visobert-aspects"
SOURCE = "model"
MAX_TOKENS = 128  # bằng lúc train
POST_TEXT_LIMIT = 1200
PAGE_SIZE = 1000
SAVE_CHUNK = 400  # dòng mỗi câu UPDATE: dữ liệu đi qua 1 tham số JSON (giới hạn 100 tham số của D1)
SAVE_CONCURRENCY = 4


class AspectModel:
    def __init__(self, model_dir: Path, batch_size: int) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        # AutoTokenizer của transformers 5 nạp hỏng tokenizer XLM-R của ViSoBERT (xem scripts/jobs/visobert_label.py).
        self.tokenizer = PreTrainedTokenizerFast.from_pretrained(model_dir)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_dir).to(self.device).eval()
        meta = json.loads((model_dir / "aspect_labels.json").read_text())
        self.labels: list[str] = meta["labels"]
        thresholds = meta.get("count_thresholds") or meta["results"][-1]["thresholds"]
        self.thresholds = torch.tensor([thresholds[label] for label in self.labels])
        self.batch_size = batch_size

    def predict(self, texts: list[str]) -> list[list[str]]:
        torch = self._torch
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))  # lô cùng độ dài -> ít token đệm
        results: dict[int, list[str]] = {}
        with torch.inference_mode():
            for start in range(0, len(order), self.batch_size):
                idx = order[start : start + self.batch_size]
                enc = self.tokenizer(
                    [texts[i] for i in idx], padding=True, truncation=True, max_length=MAX_TOKENS, return_tensors="pt"
                ).to(self.device)
                hits = torch.sigmoid(self.model(**enc).logits.float()).cpu() >= self.thresholds
                for i, row in zip(idx, hits.tolist(), strict=True):
                    results[i] = [label for label, on in zip(self.labels, row, strict=True) if on]
        return [results[i] for i in range(len(texts))]


async def _ensure_columns(table: str) -> None:
    cols = {row.get("name") for row in await d1_query(f"PRAGMA table_info({table})") or []}
    if "aspects_source" not in cols:
        await d1_query(f"ALTER TABLE {table} ADD COLUMN aspects_source TEXT", quiet=True)
        logger.info("aspect_column_added", table=table, column="aspects_source")


async def _select(table: str, limit: int, cursor: tuple[str, str] | None, since: str | None) -> list[dict[str, Any]]:
    """Một trang dòng chưa có khía cạnh, mới nhất trước, sau con trỏ (scraped_at, id) của trang trước."""
    if table == "posts":
        sql = (
            f"SELECT p.id, p.scraped_at, substr(p.content, 1, {POST_TEXT_LIMIT}) AS message FROM posts p "
            f"WHERE p.aspects IS NULL AND {RELEVANT_POST_SQL}"
        )
        alias = "p"
    else:
        sql = "SELECT c.id, c.scraped_at, c.message FROM comments c WHERE c.aspects IS NULL"
        alias = "c"
    params: list[Any] = []
    if cursor:
        sql += f" AND ({alias}.scraped_at < ? OR ({alias}.scraped_at = ? AND {alias}.id < ?))"
        params.extend([cursor[0], cursor[0], cursor[1]])
    if since:
        sql += f" AND {alias}.scraped_at >= ?"
        params.append(since)
    sql += f" ORDER BY {alias}.scraped_at DESC, {alias}.id DESC LIMIT ?"
    params.append(limit)
    rows = await d1_query(sql, params, timeout=90.0)
    if rows is None:
        raise RuntimeError("aspect_select_failed")
    return rows


async def _save(table: str, aspects: dict[str, list[str]]) -> int:
    """Một câu UPDATE cho cả khối qua json_each, tra theo khoá chính. Dấu "+" chặn planner dùng index trên aspects (cùng
    bẫy với sentiment 19/10/2026: điều kiện IS NULL có index làm câu UPDATE quét mọi dòng chưa có nhãn)."""
    if not aspects:
        return 0
    payload = json.dumps([{"id": cid, "a": json.dumps(labels)} for cid, labels in aspects.items()])
    result = await d1_query(
        f"UPDATE {table} SET aspects = j.a, aspects_source = '{SOURCE}' "
        f"FROM (SELECT value ->> 'id' AS id, value ->> 'a' AS a FROM json_each(?1)) AS j "
        f"WHERE {table}.id = j.id AND +{table}.aspects IS NULL",
        [payload],
        timeout=60.0,
    )
    if result is None:
        logger.warning("aspect_save_failed", table=table, count=len(aspects))
        return 0
    return len(aspects)


async def run(args: argparse.Namespace) -> None:
    from datetime import UTC, datetime, timedelta

    if not args.model.exists():
        raise SystemExit(f"Chưa có model ở {args.model} - copy từ máy đã train (scripts/ml/aspect_finetune.py).")
    model = AspectModel(args.model, args.batch_size)
    logger.info("aspect_model", model=str(args.model), device=model.device, labels=len(model.labels))
    if not args.dry_run:
        await _ensure_columns(args.table)
    since = (datetime.now(tz=UTC) - timedelta(days=args.days)).isoformat() if args.days else None
    totals: Counter[str] = Counter()
    cursor: tuple[str, str] | None = None
    sem = asyncio.Semaphore(SAVE_CONCURRENCY)
    started = time.monotonic()

    async def save_chunk(chunk: dict[str, list[str]]) -> int:
        async with sem:
            return await _save(args.table, chunk)

    while args.limit is None or totals["selected"] < args.limit:
        page = PAGE_SIZE if args.limit is None else min(PAGE_SIZE, args.limit - totals["selected"])
        rows = await _select(args.table, page, cursor, since)
        if not rows:
            break
        cursor = (str(rows[-1]["scraped_at"] or ""), str(rows[-1]["id"]))
        totals["selected"] += len(rows)
        result: dict[str, list[str]] = {}
        model_rows = []
        for row in rows:
            if args.table == "comments" and rule_sentiment(row.get("message")) is not None:
                result[row["id"]] = []  # không mang nội dung
                totals["no_content"] += 1
            else:
                model_rows.append(row)
        preds = await asyncio.to_thread(model.predict, [strip_placeholders(row["message"] or "") for row in model_rows])
        for row, labels in zip(model_rows, preds, strict=True):
            result[row["id"]] = labels
            totals["with_aspect" if labels else "no_aspect"] += 1
            for label in labels:
                totals[label] += 1
        if args.dry_run:
            for row in rows[:50]:
                print(f"{', '.join(result[row['id']]) or '-':28s} {' '.join(str(row['message']).split())[:100]}")
            break
        items = list(result.items())
        # Ghi xong trang này rồi mới chọn trang sau (trang sau chọn lại aspects IS NULL theo con trỏ).
        saved = await asyncio.gather(
            *(save_chunk(dict(items[i : i + SAVE_CHUNK])) for i in range(0, len(items), SAVE_CHUNK))
        )
        totals["saved"] += sum(saved)
        elapsed = max(time.monotonic() - started, 1)
        logger.info(
            "aspect_label_page",
            table=args.table,
            page=len(rows),
            saved=totals["saved"],
            with_aspect=totals["with_aspect"],
            per_minute=round(totals["saved"] / elapsed * 60),
        )
        if totals["saved"] == 0 and totals["selected"] >= PAGE_SIZE * 2:
            logger.error("aspect_label_nothing_saved", hint="D1 lỗi - dừng lượt này")
            break
    logger.info(
        "aspect_label_finished",
        table=args.table,
        dry_run=args.dry_run,
        minutes=round((time.monotonic() - started) / 60, 1),
        **{k: v for k, v in totals.items() if ":" not in k},
        top_aspects=dict(Counter({k: v for k, v in totals.items() if ":" in k}).most_common(6)),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", choices=("comments", "posts"), default="comments")
    parser.add_argument("--model", type=Path, default=MODEL_DIR)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--days", type=float, help="chỉ dòng thu thập trong N ngày gần nhất")
    parser.add_argument("--limit", type=int, help="tối đa ngần này dòng lượt này")
    parser.add_argument("--dry-run", action="store_true", help="gán một trang và in ra, không ghi")
    asyncio.run(run(parser.parse_args()))
