"""Đo độ chính xác của máy (nhãn cảm xúc, khía cạnh khen/chê, giai đoạn khán giả) so với nhãn người làm.

1. export - rút ngẫu nhiên N comment đã được máy gán đủ nhãn (có nội dung, dưới bài liên quan tới phim) ra CSV
   để người chấm điền. File KHÔNG chứa nhãn của máy - để người chấm không bị nhãn máy dẫn dắt; lúc chấm điểm
   script tự lấy nhãn máy từ D1 theo id.
       python -m scripts.accuracy_sample export --n 150 --out accuracy_2026-10-07.csv
       python -m scripts.accuracy_sample export --n 150 --movie-id movie_... --out hhcc.csv

   Người chấm điền các cột:
     nguoi_ve_phim    co / khong   - comment có nói về phim không
     nguoi_cam_xuc    positive / negative / neutral   (hoặc tich_cuc / tieu_cuc / trung_lap)
     nguoi_khia_canh  vd "dien_xuat:+, kich_ban:-" - để trống nếu không nhắc khía cạnh nào. Key hợp lệ:
                      dien_xuat, dien_vien, kich_ban, cam_xuc, hinh_anh, am_thanh, thong_diep, san_phim,
                      quang_ba, rap_ve (xem app/ai/aspects.py)
     nguoi_giai_doan  hong (chưa xem) / da_xem / khac
   Dòng để trống nguoi_cam_xuc bị bỏ qua khi chấm.

2. score - so nhãn người với nhãn máy, in kết quả; --save lưu vào Supabase (ai_accuracy_runs) để report Social
   Topic hiện "máy đúng bao nhiêu" cạnh các con số.
       python -m scripts.accuracy_sample score --file accuracy_2026-10-07.csv
       python -m scripts.accuracy_sample score --file accuracy_2026-10-07.csv --save --note "Lan chấm"
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from app.ai.accuracy import compute_metrics, parse_human_aspects, parse_human_sentiment, parse_human_yes_no
from app.ai.tasks.sentiment_rules import rule_sentiment
from app.repositories.d1.comments import ensure_comment_insight_columns
from app.services.d1 import RELEVANT_POST_SQL, d1_query

COLUMNS = [
    "id",
    "nen_tang",
    "bai_viet",
    "binh_luan",
    "nguoi_ve_phim",
    "nguoi_cam_xuc",
    "nguoi_khia_canh",
    "nguoi_giai_doan",
    "ghi_chu",
]
_POST_SNIPPET_CHARS = 200
_ID_CHUNK = 90  # D1 cho tối đa 100 tham số mỗi câu lệnh


async def export(n: int, out: Path, movie_id: str | None) -> None:
    await ensure_comment_insight_columns()
    sql = f"""
        SELECT c.id, c.platform, c.message, p.content AS post_content
        FROM comments c JOIN posts p ON p.id = c.post_id
        WHERE {RELEVANT_POST_SQL} AND c.insights_classified_at IS NOT NULL AND length(trim(c.message)) >= 15
    """
    params: list[str | int] = []
    if movie_id:
        sql += " AND p.movie_id = ?"
        params.append(movie_id)
    sql += " ORDER BY random() LIMIT ?"
    params.append(n * 2)  # dư để bù các comment không mang nội dung bị bỏ dưới đây
    rows = await d1_query(sql, params)
    if rows is None:
        sys.exit("Không đọc được D1")
    picked = [r for r in rows if rule_sentiment(r["message"]) is None][:n]
    with out.open("w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig: Excel mở đúng tiếng Việt
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for r in picked:
            writer.writerow(
                {
                    "id": r["id"],
                    "nen_tang": r["platform"],
                    "bai_viet": " ".join((r.get("post_content") or "").split())[:_POST_SNIPPET_CHARS],
                    "binh_luan": r["message"],
                }
            )
    print(f"Đã ghi {len(picked)} comment vào {out}")


async def _machine_labels(ids: list[str]) -> dict[str, dict]:
    labels: dict[str, dict] = {}
    for start in range(0, len(ids), _ID_CHUNK):
        chunk = ids[start : start + _ID_CHUNK]
        rows = await d1_query(
            f"SELECT id, sentiment, aspects, audience_stage, insights_classified_at FROM comments "
            f"WHERE id IN ({','.join('?' * len(chunk))})",
            chunk,
        )
        for r in rows or []:
            aspects = json.loads(r["aspects"]) if r.get("insights_classified_at") and r.get("aspects") else None
            labels[r["id"]] = {"sentiment": r["sentiment"], "aspects": aspects, "stage": r.get("audience_stage")}
    return labels


async def score(path: Path, save: bool, note: str | None, labeled_at: str) -> None:
    with path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    human = {}
    for r in rows:
        sentiment = parse_human_sentiment(r.get("nguoi_cam_xuc"))
        if sentiment is None:
            continue
        human[r["id"]] = {
            "sentiment": sentiment,
            "aspects": parse_human_aspects(r.get("nguoi_khia_canh")),
            "stage": (r.get("nguoi_giai_doan") or "").strip().lower(),
            "about_film": parse_human_yes_no(r.get("nguoi_ve_phim")),
        }
    if not human:
        sys.exit("Chưa có dòng nào điền nguoi_cam_xuc")
    machine = await _machine_labels(list(human))
    pairs = [{"human": h, "machine": machine[cid]} for cid, h in human.items() if machine.get(cid, {}).get("sentiment")]
    metrics = compute_metrics(pairs)
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    if save:
        from app.services.platform_config_db import insert_accuracy_run

        await insert_accuracy_run(labeled_at=labeled_at, sample_size=metrics["n"], metrics=metrics, note=note)
        print("Đã lưu vào ai_accuracy_runs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    ex = sub.add_parser("export")
    ex.add_argument("--n", type=int, default=150)
    ex.add_argument("--out", type=Path, required=True)
    ex.add_argument("--movie-id")
    sc = sub.add_parser("score")
    sc.add_argument("--file", type=Path, required=True)
    sc.add_argument("--save", action="store_true")
    sc.add_argument("--note")
    sc.add_argument("--date", default=datetime.now(tz=UTC).date().isoformat(), help="Ngày người chấm (YYYY-MM-DD)")
    args = parser.parse_args()
    if args.command == "export":
        asyncio.run(export(args.n, args.out, args.movie_id))
    else:
        asyncio.run(score(args.file, args.save, args.note, args.date))
