"""Thử cho AI đặt tên các cụm bình luận của một phim (2026-11-09), đọc data/clusters/<movie_id>.json của
scripts/tools/cluster_film_comments.py. Một lời gọi cho cả phim để AI thấy mọi cụm, đặt tên phân biệt được và chỉ ra cụm
trùng ý. Ghi data/clusters/<movie_id>.names.json và .names.md.

    python -m scripts.tools.name_clusters --movie-id <id> --title "Chị Chị Em Em 3"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re

from app.ai.runtime import call_llm
from scripts.tools.cluster_film_comments import OUT_DIR

TASK = "comment_cluster_naming"
SAMPLES_PER_CLUSTER = 20

SYSTEM_PROMPT = """Bạn giúp bộ phận truyền thông của một công ty phát hành phim Việt Nam đọc bình luận mạng xã hội về một phim.
Máy đã gom các bình luận giống nhau về ý nghĩa thành từng cụm. Với mỗi cụm, bạn được xem các câu mẫu.

Nhiệm vụ: đặt tên từng cụm để người đọc biết ngay khán giả đang nói gì.
- Tên ngắn (2-6 chữ), cụ thể: nêu tên diễn viên, nhân vật, cảnh, câu thoại, bài hát nếu câu mẫu nhắc tới. Tránh tên chung chung
  như "Bình luận tích cực".
- kind: "goc_phim" (nói về chính bộ phim: diễn viên, nhân vật, kịch bản, cảnh, nhạc, hình ảnh, so sánh...),
  "di_xem" (rủ đi xem, hỏi lịch, vé, rạp), "ngoai_le" (chuyện đời tư, showbiz, chuyện không liên quan phim),
  "tuong_tac" (tag bạn, cười, khen chung chung không nội dung), "lan_lon" (các câu mẫu không cùng một ý).
- Chỉ dựa vào câu mẫu, không thêm kiến thức ngoài. Không nêu tên người bình luận.
- merge_with: số cụm khác nói cùng một ý với cụm này (nếu có), để gộp khi hiển thị.

Trả về đúng một mảng JSON, mỗi phần tử: {"cluster": số, "name": "...", "description": "1 câu", "kind": "...", "merge_with": [số...]}.
Trong tên và mô tả, trích câu thoại bằng nháy đơn hoặc « », không dùng dấu nháy kép. Không thêm gì ngoài JSON."""


def build_prompt(title: str, clusters: list[dict]) -> str:
    lines = [f"Phim: {title}", f"Có {len(clusters)} cụm."]
    for cluster in clusters:
        lines.append(f"\n## Cụm {cluster['cluster']} - {cluster['comments']} bình luận ({cluster['share_percent']}%)")
        for sample in cluster["samples"][:SAMPLES_PER_CLUSTER]:
            text = " ".join(str(sample["text"]).split())[:200]
            parent = f" (trả lời: {' '.join(str(sample['parent']).split())[:80]})" if sample.get("parent") else ""
            lines.append(f"- {text}{parent}")
    return "\n".join(lines)


async def main(args: argparse.Namespace) -> None:
    clusters = json.loads((OUT_DIR / f"{args.movie_id}.json").read_text(encoding="utf-8"))
    # force: công tắc AI đang tắt cho ingest; đây là bản thử người vận hành yêu cầu.
    text = await call_llm(
        task=TASK,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=build_prompt(args.title, clusters),
        max_tokens=16000,  # 30 cụm có lúc vượt 8.000 token, câu trả lời bị cắt ở cụm thứ 23 (09/11/2026)
        force=True,
    )
    # Không dùng with_suffix: nó thay đuôi ".names" và ghi đè file cụm <movie_id>.json (09/11/2026).
    out = lambda ext: OUT_DIR / f"{args.movie_id}.names.{ext}"
    out("raw.txt").write_text(text or "", encoding="utf-8")
    # Đọc từng object một: một cụm hỏng (câu trả lời bị cắt, nháy kép trong câu thoại) không làm mất các cụm khác.
    names = {}
    for chunk in re.findall(r"\{[^{}]*\}", text or ""):
        try:
            item = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        if isinstance(item.get("cluster"), int):
            names[item["cluster"]] = item
    if not names:
        raise SystemExit(f"AI không trả JSON: {(text or '')[:300]}")
    rows = []
    for cluster in sorted(clusters, key=lambda c: -c["comments"]):
        named = names.get(cluster["cluster"], {})
        rows.append({**{k: cluster[k] for k in ("cluster", "comments", "share_percent", "cohesion")}, **named})
    out("json").write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
    md = [
        f"# {args.title}: tên cụm bình luận (thử, Haiku)\n",
        "| Cụm | Bình luận | % | Loại | Tên | Mô tả | Gộp với |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        md.append(
            f"| {row['cluster']} | {row['comments']} | {row['share_percent']} | {row.get('kind', '')} | {row.get('name', '')} | "
            f"{row.get('description', '')} | {', '.join(map(str, row.get('merge_with') or []))} |"
        )
    out("md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"đã ghi {out('md')} ({len(names)}/{len(clusters)} cụm có tên)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--title", required=True)
    asyncio.run(main(parser.parse_args()))
