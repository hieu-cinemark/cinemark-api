"""Gán giai đoạn khán giả (audience_stage: hong / da_xem / khac) cho bình luận của một phim bằng model local
models/visobert-stage (scripts/ml/stage_finetune.py), 2026-11-09. Thay nhãn của AI cũ đã ngừng từ 21/10 - khối "Từ hóng thành
đã xem" của Social Topic đọc cột này qua movie_comment_daily.

- Bình luận chỉ có tag / icon (rule_sentiment) bỏ qua: backfill_tag_icon_stage đã gán hong.
- Bình luận đăng trước ngày chiếu sớm (không có thì ngày khởi chiếu) không thể là đã xem phim này: da_xem -> khac.
- Đọc theo từng nhóm bài (index post_id). Ghi đè nhãn cũ của AI.

    python -m scripts.jobs.stage_label --movie-id <id> --dry-run
    python -m scripts.jobs.stage_label --movie-id <id>
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from app.ai.tasks.sentiment_rules import rule_sentiment, strip_placeholders, with_parent_context
from app.repositories.d1.store import d1_query

MODEL_DIR = Path(__file__).resolve().parents[2] / "models" / "visobert-stage"
LABELS = ("hong", "da_xem", "khac")
POST_CHUNK = 40
MAX_TOKENS = 128


class StageModel:
    def __init__(self, batch_size: int = 64) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        self.tokenizer = PreTrainedTokenizerFast.from_pretrained(MODEL_DIR)
        self.model = AutoModelForSequenceClassification.from_pretrained(MODEL_DIR).to(self.device).eval()
        self.batch_size = batch_size

    def predict(self, texts: list[str]) -> list[str]:
        torch = self._torch
        out: list[str] = []
        with torch.inference_mode():
            for start in range(0, len(texts), self.batch_size):
                enc = self.tokenizer(
                    texts[start : start + self.batch_size],
                    padding=True,
                    truncation=True,
                    max_length=MAX_TOKENS,
                    return_tensors="pt",
                ).to(self.device)
                out.extend(LABELS[i] for i in self.model(**enc).logits.argmax(dim=-1).tolist())
        return out


async def premiere_start(movie_id: str) -> str | None:
    rows = await d1_query(
        "SELECT COALESCE(s.premiere_date, m.released_at) AS d FROM movies m LEFT JOIN movie_measure_settings s ON s.movie_id = m.id WHERE m.id = ?",
        [movie_id],
        quiet=True,
    )
    day = (rows or [{}])[0].get("d")
    if not day:
        return None
    # 00:00 giờ VN của ngày chiếu sớm, cùng định dạng posted_at.
    return datetime.fromisoformat(f"{str(day)[:10]}T00:00:00+07:00").astimezone(UTC).isoformat()


async def run(args: argparse.Namespace) -> None:
    model = StageModel()
    start = await premiere_start(args.movie_id)
    posts = await d1_query("SELECT id FROM posts WHERE movie_id = ?", [args.movie_id], timeout=90.0, quiet=True)
    post_ids = [row["id"] for row in posts or []]
    totals: Counter[str] = Counter()
    for i in range(0, len(post_ids), POST_CHUNK):
        chunk = post_ids[i : i + POST_CHUNK]
        rows = await d1_query(
            "SELECT c.id, c.message, c.posted_at, par.message AS parent_message FROM comments c "
            "LEFT JOIN comments par ON par.platform = c.platform AND par.external_id = c.parent_external_id "
            f"WHERE c.post_id IN ({','.join('?' * len(chunk))})",
            chunk,
            timeout=90.0,
            quiet=True,
        )
        rows = [row for row in rows or [] if rule_sentiment(row.get("message")) is None]
        if not rows:
            continue
        preds = await asyncio.to_thread(
            model.predict,
            [strip_placeholders(with_parent_context(r["message"], r.get("parent_message"))) for r in rows],
        )
        updates = []
        for row, stage in zip(rows, preds, strict=True):
            if stage == "da_xem" and start and row.get("posted_at") and str(row["posted_at"]) < start:
                stage = "khac"
                totals["truoc_chieu_som"] += 1
            totals[stage] += 1
            updates.append({"id": row["id"], "s": stage})
        if args.dry_run:
            for row, item in list(zip(rows, updates, strict=True))[:20]:
                print(f"{item['s']:<7} {' '.join(str(row['message']).split())[:100]}")
            break
        result = await d1_query(
            "UPDATE comments SET audience_stage = j.s FROM (SELECT value ->> 'id' AS id, value ->> 's' AS s FROM json_each(?1)) AS j "
            "WHERE comments.id = j.id",
            [json.dumps(updates)],
            timeout=60.0,
        )
        if result is None:
            raise SystemExit(f"ghi lỗi sau {sum(totals[label] for label in LABELS)} dòng")
    print(f"{args.movie_id}: {dict(totals)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    asyncio.run(run(parser.parse_args()))
