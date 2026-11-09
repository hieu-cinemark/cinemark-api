"""Gộp, lọc và tổng kết cụm bình luận đã đặt tên của một phim (2026-11-09), trước khi đưa lên D1 (publish_clusters.py).

- Gộp các cụm AI đánh dấu cùng ý (merge_with của name_clusters.py); cụm gộp lấy tên của cụm lớn nhất.
- Ẩn cụm "lẫn lộn" và "tương tác chung" (tag bạn, khen chung không nội dung).
- Khen / chê / trung lập của mỗi cụm đếm từ nhãn cảm xúc sẵn có của từng bình luận.
- AI tổng kết tối đa MAX_SUMMARIES cụm lớn nhất: đọc SAMPLE câu nhiều like nhất, viết tóm tắt, ý khen, ý chê, trích dẫn.

Ghi data/clusters/<movie_id>.final.json.

    python -m scripts.tools.summarize_clusters --movie-id <id> --title "Chị Chị Em Em 3"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from typing import Any

from app.ai.runtime import call_llm
from app.ai.tasks.sentiment_rules import slang_positive
from scripts.tools.cluster_film_comments import OUT_DIR

TASK = "comment_cluster_summary"
HIDDEN_KINDS = {"lan_lon", "tuong_tac"}
MAX_SUMMARIES = 15
MIN_SUMMARY_COMMENTS = 80
SAMPLE = 60
POST_SAMPLE = (
    20  # bài đăng Facebook/Threads trong nhóm, đọc thêm ngoài SAMPLE bình luận (09/11/2026, chấp nhận tốn token hơn)
)

SYSTEM_PROMPT = """Bạn tổng kết ý kiến khán giả về một phim điện ảnh Việt Nam cho bộ phận truyền thông, từ một nhóm bài đăng và bình
luận mạng xã hội cùng nói về một ý. Bài đăng Facebook/Threads thường là review, cảm nhận dài - đọc kỹ để lấy lý lẽ cụ thể.

Quy tắc:
- Chỉ dựa vào các bình luận được cung cấp, không thêm kiến thức ngoài, không suy đoán, không khuyên nhủ.
- Bỏ qua câu lạc ý. Nêu cụ thể tên diễn viên, nhân vật, cảnh, câu thoại khi bình luận có nhắc. Không nêu tên người bình luận.
- Nếu ý kiến ít hoặc chỉ khen chung chung thì nói thẳng như vậy.
- Trích dẫn đúng nguyên văn, không sửa chữ.
- Trong chuỗi, không dùng dấu nháy kép; trích câu thoại bằng nháy đơn.

Trả về đúng một object JSON, không thêm gì khác:
{"tom_tat": "2-3 câu", "khen": ["ý chính", ...], "che": ["ý chính", ...], "trich_dan": ["câu nguyên văn", ...]}
khen / che tối đa 4 ý mỗi bên, để trống nếu không đáng kể; trich_dan 2-3 câu."""


def merge_groups(names: list[dict[str, Any]], members: dict[str, list[str]]) -> list[dict[str, Any]]:
    """Gộp cụm hiện được theo merge_with (union-find), bỏ cụm ẩn. Mỗi nhóm giữ tên/mô tả/loại của cụm lớn nhất."""
    visible = {
        row["cluster"]: row for row in names if row.get("kind") not in HIDDEN_KINDS and str(row["cluster"]) in members
    }
    parent = {cid: cid for cid in visible}

    def find(cid: int) -> int:
        while parent[cid] != cid:
            parent[cid] = parent[parent[cid]]
            cid = parent[cid]
        return cid

    for cid, row in visible.items():
        for other in row.get("merge_with") or []:
            if other in visible:
                parent[find(other)] = find(cid)
    groups: dict[int, list[int]] = {}
    for cid in visible:
        groups.setdefault(find(cid), []).append(cid)
    merged = []
    for cids in groups.values():
        lead = max(cids, key=lambda cid: len(members[str(cid)]))
        ids = [comment for cid in cids for comment in members[str(cid)]]
        merged.append(
            {
                **{k: visible[lead].get(k) for k in ("name", "description", "kind")},
                "cluster": lead,
                "merged": sorted(cids),
                "members": ids,
            }
        )
    return sorted(merged, key=lambda group: -len(group["members"]))


def sentiment_counts(ids: list[str], details: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts = {"positive": 0, "negative": 0, "neutral": 0}
    for cid in ids:
        detail = details.get(cid, {})
        label = detail.get("sentiment")
        if label == "negative" and slang_positive(detail.get("text")):
            label = "positive"  # nhãn cũ trước luật tiếng lóng (09/11/2026)
        if label in counts:
            counts[label] += 1
    return counts


def build_prompt(title: str, group: dict[str, Any], details: dict[str, dict[str, Any]]) -> str:
    by_likes = sorted(group["members"], key=lambda cid: -(details[cid]["likes"] or 0))
    posts = [cid for cid in by_likes if details[cid].get("post")][:POST_SAMPLE]
    top = [cid for cid in by_likes if not details[cid].get("post")][:SAMPLE]
    header = [f"Phim: {title}", f"Nhóm: {group['name']} ({group['description']})"]
    if posts:
        header.append(
            f"\nBÀI ĐĂNG Facebook/Threads ({group.get('posts', len(posts))} bài, {len(posts)} bài nhiều like nhất):"
        )
        header.extend(
            f"- [{details[cid]['post']}] ({details[cid]['likes']} like) {' '.join(details[cid]['text'].split())[:500]}"
            for cid in posts
        )
    header.append(f"\nBÌNH LUẬN ({group.get('comments_only', len(top))} bình luận, {len(top)} câu nhiều like nhất):")
    header.extend(f"- ({details[cid]['likes']} like) {' '.join(details[cid]['text'].split())[:300]}" for cid in top)
    return "\n".join(header)


def parse_summary(text: str | None) -> dict[str, Any] | None:
    match = re.search(r"\{.*\}", text or "", re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return {key: data.get(key) for key in ("tom_tat", "khen", "che", "trich_dan")}


async def main(args: argparse.Namespace) -> None:
    names = json.loads((OUT_DIR / f"{args.movie_id}.names.json").read_text(encoding="utf-8"))
    members = json.loads((OUT_DIR / f"{args.movie_id}.members.json").read_text(encoding="utf-8"))
    details = json.loads((OUT_DIR / f"{args.movie_id}.details.json").read_text(encoding="utf-8"))
    total = sum(len(ids) for ids in members.values())
    groups = merge_groups(names, members)
    for group in groups:
        group.update(sentiment_counts(group["members"], details))
        group["comments"] = len(group["members"])
        group["posts"] = sum(1 for cid in group["members"] if details.get(cid, {}).get("post"))
        group["comments_only"] = group["comments"] - group["posts"]
        group["share_percent"] = round(group["comments"] * 100 / max(total, 1), 1)
    targets = [group for group in groups if group["comments"] >= MIN_SUMMARY_COMMENTS][:MAX_SUMMARIES]

    async def summarize(group: dict[str, Any]) -> None:
        # force: công tắc AI đang tắt cho ingest; đây là bản thử người vận hành yêu cầu.
        text = await call_llm(
            task=TASK,
            system_prompt=SYSTEM_PROMPT,
            user_prompt=build_prompt(args.title, group, details),
            max_tokens=4000,
            force=True,
        )
        group["summary"] = parse_summary(text)

    await asyncio.gather(*(summarize(group) for group in targets))
    (OUT_DIR / f"{args.movie_id}.final.json").write_text(json.dumps(groups, ensure_ascii=False), encoding="utf-8")
    hidden = sum(
        len(members[str(row["cluster"])])
        for row in names
        if row.get("kind") in HIDDEN_KINDS and str(row["cluster"]) in members
    )
    print(
        f"{len(names)} cụm -> {len(groups)} nhóm hiện (ẩn {hidden} bình luận lẫn lộn/tương tác), "
        f"tổng kết {sum(1 for g in targets if g.get('summary'))}/{len(targets)}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--title", required=True)
    asyncio.run(main(parser.parse_args()))
