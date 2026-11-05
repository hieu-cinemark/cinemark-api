"""Thử cho AI tổng kết từng chủ đề bình luận của một phim (2026-11-05). Lấy bình luận gốc nhiều like nhất, bỏ bài trend và bình luận
không có nội dung, gán chủ đề bằng model visobert-aspects, rồi mỗi chủ đề một lời gọi AI. Chỉ đọc D1 và ghi file data/summaries/.

    python -m scripts.tools.summarize_aspects --movie-id <id> --title "Chị Chị Em Em 3"
    python -m scripts.tools.summarize_aspects --movie-id <id> --title "Chị Chị Em Em 3" --phase official --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.ai.aspects import ASPECTS
from app.ai.runtime import call_llm
from app.ai.tasks.sentiment_rules import strip_placeholders
from app.repositories.d1.store import d1_query
from scripts.jobs.aspect_label import MODEL_DIR, AspectModel

OUT_DIR = Path(__file__).resolve().parents[2] / "data" / "summaries"
TASK = "comment_aspect_summary"
SAMPLE = 4000
PER_POLARITY = 40
PHASE_LABELS = {"hong": "Hóng", "early": "Chiếu sớm", "official": "Chiếu chính thức"}
MIN_COMMENTS = 5

SYSTEM_PROMPT = """Bạn tổng kết ý kiến khán giả về một phim điện ảnh Việt Nam từ bình luận mạng xã hội, cho bộ phận truyền thông đọc.

Quy tắc:
- Chỉ dựa vào các bình luận được cung cấp, không thêm kiến thức ngoài, không suy đoán về phim, không khuyên nhủ.
- Một số bình luận có thể bị xếp nhầm chủ đề hoặc nhầm khen/chê: bỏ qua các câu không liên quan, không cố gượng đưa vào.
- Nêu cụ thể tên diễn viên, nhân vật, cảnh, chi tiết khi bình luận có nhắc. Không nêu tên người bình luận.
- Nếu ý kiến phân tán, ít, hoặc nhiều câu chỉ là khen chung chung thì nói thẳng như vậy.
- Trích đúng nguyên văn, không sửa chữ.

Viết tiếng Việt, đúng định dạng sau, không thêm gì khác:
**Tóm tắt:** 2-3 câu.
**Khen:** các ý chính, mỗi ý một gạch đầu dòng; không có thì ghi "Không đáng kể".
**Chê:** tương tự.
**Trích dẫn:** 2-3 câu nguyên văn tiêu biểu."""


def vn_day_start(day: str) -> str:
    return datetime.fromisoformat(f"{day[:10]}T00:00:00+07:00").astimezone(UTC).isoformat()


def phase_filter(phase: str | None, premiere: str | None, released: str | None) -> tuple[str, list[str]]:
    """Cùng định nghĩa với cinemark api/lib/commentPhase.ts."""
    first = premiere or released
    if not phase or not first:
        return "", []
    start = vn_day_start(first)
    release = vn_day_start(released) if released else start
    at = "COALESCE(c.posted_at, c.scraped_at)"
    if phase == "hong":
        return f"AND {at} < ? ", [start]
    if phase == "early":
        return f"AND {at} >= ? AND {at} < ? AND p.posted_at >= ? ", [start, max(start, release), start]
    return f"AND {at} >= ? AND p.posted_at >= ? ", [release, start]


def group_comments(rows: list[dict[str, Any]], labels: list[list[str]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """aspect -> {"+": [...], "-": [...]}, nhiều like trước. Một bình luận có thể ở nhiều chủ đề."""
    groups: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"+": [], "-": []})
    for row, row_labels in zip(rows, labels, strict=True):
        for label in row_labels:
            aspect, polarity = label.rsplit(":", 1)
            groups[aspect][polarity].append(row)
    for sides in groups.values():
        for items in sides.values():
            items.sort(key=lambda row: -(row.get("likes") or 0))
    return groups


def build_prompt(title: str, aspect: str, sides: dict[str, list[dict[str, Any]]]) -> str:
    name, description = ASPECTS[aspect]
    lines = [f"Phim: {title}", f"Chủ đề: {name} ({description})"]
    for polarity, heading in (("+", "KHEN"), ("-", "CHÊ")):
        items = sides[polarity]
        lines.append(f"\n{heading}: {len(items)} bình luận, dưới đây là tối đa {PER_POLARITY} câu nhiều like nhất")
        lines.extend(f"- ({row.get('likes') or 0} like) {' '.join(row['text'].split())[:300]}" for row in items[:PER_POLARITY])
    return "\n".join(lines)


async def main(args: argparse.Namespace) -> None:
    dates = await d1_query(
        "SELECT m.released_at, s.premiere_date FROM movies m LEFT JOIN movie_measure_settings s ON s.movie_id = m.id WHERE m.id = ?",
        [args.movie_id],
        quiet=True,
    )
    extra_sql, extra_params = phase_filter(args.phase, *(dates[0][key] for key in ("premiere_date", "released_at")))
    rows = await d1_query(
        "SELECT c.message AS text, c.reactions_count AS likes FROM comments c JOIN posts p ON p.id = c.post_id "
        "WHERE p.movie_id = ? AND COALESCE(p.trend_post, 0) = 0 "
        "AND (p.relevance_label = 'related' OR (p.relevance_label IS NULL AND p.keyword_match > 0)) "
        "AND c.parent_external_id IS NULL AND COALESCE(c.sentiment_source, '') != 'rule' AND length(trim(c.message)) >= 10 "
        f"{extra_sql}ORDER BY c.reactions_count DESC LIMIT ?",
        [args.movie_id, *extra_params, SAMPLE],
        timeout=90.0,
    )
    if not rows:
        raise SystemExit("Không đọc được bình luận")
    labels = AspectModel(MODEL_DIR, 64).predict([strip_placeholders(row["text"]) for row in rows])
    groups = group_comments(rows, labels)
    print(f"{len(rows)} bình luận, {sum(bool(item) for item in labels)} có chủ đề", flush=True)
    if args.dry_run:
        counts = {aspect: (len(sides["+"]), len(sides["-"])) for aspect, sides in groups.items()}
        print(sorted(counts.items(), key=lambda item: -sum(item[1])))
        return

    async def summarize(aspect: str, sides: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        # force: công tắc AI đang tắt là để chặn bộ phân loại lúc ingest, còn đây là bản thử do người vận hành yêu cầu.
        text = await call_llm(
            task=TASK,
            system_prompt=SYSTEM_PROMPT,
            user_prompt=build_prompt(args.title, aspect, sides),
            max_tokens=8000,  # Haiku suy luận ẩn trước khi trả lời: 2.500 làm 3/10 chủ đề bị cắt giữa câu
            force=True,
        )
        return {"aspect": aspect, "name": ASPECTS[aspect][0], "positive": len(sides["+"]), "negative": len(sides["-"]), "summary": text}

    picked = {a: s for a, s in groups.items() if a in ASPECTS and len(s["+"]) + len(s["-"]) >= MIN_COMMENTS}
    results = await asyncio.gather(*(summarize(a, s) for a, s in picked.items()))
    results.sort(key=lambda item: -(item["positive"] + item["negative"]))
    source = " ".join(" ".join(row["text"].split()) for row in rows)
    for item in results:
        item["complete"] = "**Trích dẫn:**" in item["summary"]
        quotes = re.findall(r'"([^"\n]{8,})"', item["summary"].split("**Trích dẫn:**")[-1])
        item["quotes_found"] = sum(quote.strip() in source for quote in quotes)
        item["quotes"] = len(quotes)
        print(item["name"], "đủ" if item["complete"] else "BỊ CẮT", f"trích dẫn đúng {item['quotes_found']}/{item['quotes']}", flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    heading = f"{args.title}, {PHASE_LABELS[args.phase]}" if args.phase else args.title
    lines = [f"# {heading}: tổng kết theo chủ đề (thử)\n", f"Từ {len(rows)} bình luận gốc nhiều like nhất, gán chủ đề bằng model.\n"]
    for item in results:
        lines.append(f"## {item['name']} ({item['positive']} khen, {item['negative']} chê)\n\n{item['summary'].strip()}\n")
    path = OUT_DIR / f"{args.movie_id}{'-' + args.phase if args.phase else ''}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    path.with_suffix(".json").write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"đã ghi {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--phase", choices=tuple(PHASE_LABELS))
    parser.add_argument("--dry-run", action="store_true", help="gán chủ đề và in số lượng, không gọi AI")
    asyncio.run(main(parser.parse_args()))
