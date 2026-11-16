"""Gán giai đoạn khán giả (audience_stage: hong / da_xem / khac) cho bình luận của một phim bằng model local
models/visobert-stage (scripts/ml/stage_finetune.py), 2026-11-09. Thay nhãn của AI cũ đã ngừng từ 21/10 - khối "Từ hóng thành
đã xem" của Social Topic đọc cột này qua movie_comment_daily.

- Bình luận chỉ có tag / icon (rule_sentiment) bỏ qua: backfill_tag_icon_stage đã gán hong.
- Bình luận đăng trước ngày chiếu sớm (không có thì ngày khởi chiếu) không thể là đã xem phim này: da_xem -> khac.
- Đọc theo từng nhóm bài (index post_id). Ghi đè nhãn cũ của AI.
- --days (16/11/2026, chạy cùng timer cinemark-aspects trên VPS): bình luận thu trong N ngày gần nhất chưa qua model
  (stage_source IS NULL) của bài liên quan. Trước đó job chỉ chạy tay theo phim nên từ 09/11 bình luận mới đều mang "khac"
  (nhãn mặc định của ViSoBERT cảm xúc) và khối hóng -> đã xem gần như trống. Mỗi dòng đã xét ghi stage_source ('model',
  hoặc 'rule' với bình luận chỉ tag / icon - giữ nhãn hong) để lượt sau không chọn lại.

    python -m scripts.jobs.stage_label --movie-id <id> --dry-run
    python -m scripts.jobs.stage_label --movie-id <id>
    python -m scripts.jobs.stage_label --days 0.25              # timer: bình luận 6 giờ qua
    python -m scripts.jobs.stage_label --days 10                # bù khoảng trống
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import and_, func, or_, select, text

from app.ai.tasks.sentiment_rules import rule_sentiment, strip_placeholders, with_parent_context
from app.core.logging import get_logger
from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.store import d1_query
from app.repositories.d1.tables import comments as comments_table
from app.repositories.d1.tables import movie_measure_settings, movies
from app.repositories.d1.tables import posts as posts_table

logger = get_logger(__name__)

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
    m, s = movies.alias("m"), movie_measure_settings.alias("s")
    rows = await d1_exec(
        select(func.coalesce(s.c.premiere_date, m.c.released_at).label("d"))
        .select_from(m.outerjoin(s, s.c.movie_id == m.c.id))
        .where(m.c.id == movie_id),
        quiet=True,
    )
    day = (rows or [{}])[0].get("d")
    if not day:
        return None
    # 00:00 giờ VN của ngày chiếu sớm, cùng định dạng posted_at.
    return datetime.fromisoformat(f"{str(day)[:10]}T00:00:00+07:00").astimezone(UTC).isoformat()


async def _ensure_stage_source() -> None:
    cols = {row.get("name") for row in await d1_query("PRAGMA table_info(comments)") or []}
    if "stage_source" not in cols:
        await d1_query("ALTER TABLE comments ADD COLUMN stage_source TEXT", quiet=True)
        logger.info("stage_column_added", column="stage_source")


async def _premiere_starts() -> dict[str, str]:
    """movie_id -> 00:00 giờ VN (UTC ISO) của ngày chiếu sớm, không có thì ngày khởi chiếu."""
    m, s = movies.alias("m"), movie_measure_settings.alias("s")
    rows = await d1_exec(
        select(m.c.id, func.coalesce(s.c.premiere_date, m.c.released_at).label("d")).select_from(
            m.outerjoin(s, s.c.movie_id == m.c.id)
        ),
        quiet=True,
    )
    return {
        row["id"]: datetime.fromisoformat(f"{str(row['d'])[:10]}T00:00:00+07:00").astimezone(UTC).isoformat()
        for row in rows or []
        if row.get("d")
    }


async def _save(updates: list[dict[str, str | None]]) -> None:
    """Ghi nhãn + stage_source. s = None: giữ nhãn hiện có (bình luận tag / icon), chỉ đánh dấu đã xét."""
    result = await d1_query(
        "UPDATE comments SET audience_stage = COALESCE(j.s, comments.audience_stage), stage_source = j.src "
        "FROM (SELECT value ->> 'id' AS id, value ->> 's' AS s, value ->> 'src' AS src FROM json_each(?1)) AS j "
        "WHERE comments.id = j.id",
        [json.dumps(updates)],
        timeout=60.0,
    )
    if result is None:
        raise SystemExit(f"ghi lỗi {len(updates)} dòng")


async def run_recent(args: argparse.Namespace) -> None:
    """Bình luận thu trong args.days ngày chưa qua model, mới nhất trước, từng trang (scraped_at, id)."""
    await _ensure_stage_source()
    model = StageModel(batch_size=args.batch_size)
    starts = await _premiere_starts()
    since = (datetime.now(tz=UTC) - timedelta(days=args.days)).isoformat()
    totals: Counter[str] = Counter()
    cursor: tuple[str, str] | None = None
    while True:
        c, par, p = comments_table.alias("c"), comments_table.alias("par"), posts_table.alias("p")
        # Dấu "+" chặn planner dùng index trên cột nhãn (bẫy full scan của D1, xem aspect_label.pending_sql).
        stmt = (
            select(
                c.c.id, c.c.message, c.c.posted_at, c.c.scraped_at, p.c.movie_id, par.c.message.label("parent_message")
            )
            .select_from(
                c.join(p, p.c.id == c.c.post_id).outerjoin(
                    par, and_(par.c.platform == c.c.platform, par.c.external_id == c.c.parent_external_id)
                )
            )
            .where(text("+c.stage_source IS NULL"), c.c.scraped_at >= since, relevant_post(p))
        )
        if cursor:
            stmt = stmt.where(or_(c.c.scraped_at < cursor[0], and_(c.c.scraped_at == cursor[0], c.c.id < cursor[1])))
        rows = await d1_exec(stmt.order_by(c.c.scraped_at.desc(), c.c.id.desc()).limit(args.page_size), timeout=90.0)
        if rows is None:
            raise SystemExit("đọc bình luận lỗi")
        if not rows:
            break
        cursor = (str(rows[-1]["scraped_at"] or ""), str(rows[-1]["id"]))
        ruled = [r for r in rows if rule_sentiment(r.get("message")) is not None]
        todo = [r for r in rows if rule_sentiment(r.get("message")) is None]
        preds = (
            await asyncio.to_thread(
                model.predict,
                [strip_placeholders(with_parent_context(r["message"], r.get("parent_message"))) for r in todo],
            )
            if todo
            else []
        )
        updates: list[dict[str, str | None]] = [{"id": r["id"], "s": None, "src": "rule"} for r in ruled]
        totals["rule"] += len(ruled)
        for row, stage in zip(todo, preds, strict=True):
            start = starts.get(str(row.get("movie_id") or ""))
            if stage == "da_xem" and start and row.get("posted_at") and str(row["posted_at"]) < start:
                stage = "khac"
                totals["truoc_chieu_som"] += 1
            totals[stage] += 1
            updates.append({"id": row["id"], "s": stage, "src": "model"})
        if args.dry_run:
            for row, item in list(zip(todo, updates[len(ruled) :], strict=True))[:20]:
                print(f"{item['s']:<7} {' '.join(str(row['message']).split())[:100]}")
            break
        await _save(updates)
    logger.info("stage_label_recent_done", days=args.days, **totals)
    print(f"{args.days} ngày: {dict(totals)}")


async def run(args: argparse.Namespace) -> None:
    if args.days:
        await run_recent(args)
        return
    await _ensure_stage_source()
    model = StageModel()
    start = await premiere_start(args.movie_id)
    post_rows = await d1_exec(
        select(posts_table.c.id).where(posts_table.c.movie_id == args.movie_id), timeout=90.0, quiet=True
    )
    post_ids = [row["id"] for row in post_rows or []]
    totals: Counter[str] = Counter()
    for i in range(0, len(post_ids), POST_CHUNK):
        chunk = post_ids[i : i + POST_CHUNK]
        c = comments_table.alias("c")
        par = comments_table.alias("par")
        rows = await d1_exec(
            select(
                c.c.id,
                c.c.message,
                c.c.posted_at,
                par.c.message.label("parent_message"),
            )
            .select_from(
                c.outerjoin(par, and_(par.c.platform == c.c.platform, par.c.external_id == c.c.parent_external_id))
            )
            .where(c.c.post_id.in_(chunk)),
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
        # Giữ SQL thuần: UPDATE ... FROM json_each - SQLAlchemy dựng câu này dài dòng và khó đọc.
        await _save([{**item, "src": "model"} for item in updates])
    print(f"{args.movie_id}: {dict(totals)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--movie-id")
    target.add_argument("--days", type=float, help="bình luận thu trong N ngày gần nhất chưa qua model (timer)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--page-size", type=int, default=500)
    asyncio.run(run(parser.parse_args()))
