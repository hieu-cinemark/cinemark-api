"""Gán nhãn giai đoạn khán giả (đang hóng / đã xem / khác) cho bình luận mẫu bằng Haiku (2026-11-09), làm dữ liệu train
model giai đoạn chạy local (scripts/ml/stage_finetune.py). Nhãn cũ của AI đã ngừng từ khi tắt AI (21/10): gần như mọi bình
luận mới mang "khác", khối "Từ hóng thành đã xem" của Social Topic không còn ý nghĩa.

Lấy bình luận gốc đủ nội dung từ data/clusters/<movie_id>.details.json (scripts/tools/cluster_film_comments.py), chia đều các
phim. Ghi nối data/stage/haiku_labels.jsonl - chạy lại bỏ qua bình luận đã có nhãn. Không ghi DB.

    python -m scripts.ml.stage_label_haiku --limit 4000 --concurrency 4
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
from collections import Counter
from pathlib import Path

from app.ai.runtime import call_llm
from scripts.tools.cluster_film_comments import OUT_DIR as CLUSTER_DIR

ROOT = Path(__file__).resolve().parents[2]
# v2 (09/11/2026): khen nhan sắc chung chung là khac - bản đầu xếp vào hong, model học theo và thổi phồng phần "đang hóng".
OUT_FILE = ROOT / "data" / "stage" / "haiku_labels_v2.jsonl"
TASK = "comment_stage_label"
LABELS = ("hong", "da_xem", "khac")
BATCH = 25
SEED = 11

SYSTEM_PROMPT = """Bạn phân loại bình luận mạng xã hội về một phim điện ảnh Việt Nam theo giai đoạn của người viết:
- hong: CÓ Ý ĐỊNH hoặc MONG CHỜ xem phim rõ ràng - sẽ đi xem, chờ ngày chiếu, đếm ngày, hỏi lịch chiếu / suất / giá vé / rạp,
  rủ bạn đi xem, đặt vé, "hóng quá", "bao giờ chiếu".
- da_xem: CÓ DẤU HIỆU ĐÃ XEM phim - "xem rồi", "vừa ra rạp", "coi xong", kể cảm nhận sau khi xem, nhắc cảnh / tình tiết / cái
  kết / diễn biến như người đã xem, review hay khen chê cụ thể nội dung phim.
- khac: mọi trường hợp còn lại - khen nhan sắc / diễn viên / trang phục chung chung, chuyện đời tư, tin tức, doanh thu, tag bạn
  không kèm lời rủ, câu đùa, câu không rõ đã xem hay chưa.

Khen đẹp, khen diễn viên mà không có ý định xem hay dấu hiệu đã xem thì là khac, không phải hong.
Chỉ dựa vào chữ trong bình luận. Không chắc thì chọn khac.
Trả về đúng một mảng JSON theo thứ tự đầu vào: [{"i": số, "stage": "hong|da_xem|khac"}, ...]. Không thêm gì khác."""


def load_pool(limit: int) -> list[dict]:
    rng = random.Random(SEED)
    pools = []
    for path in sorted(CLUSTER_DIR.glob("*.details.json")):
        details = json.loads(path.read_text(encoding="utf-8"))
        items = [
            {"id": cid, "text": " ".join(d["text"].split())[:300]} for cid, d in details.items() if not d.get("post")
        ]
        rng.shuffle(items)
        pools.append(items)
    picked, i = [], 0
    while len(picked) < limit and any(i < len(p) for p in pools):
        picked.extend(p[i] for p in pools if i < len(p))
        i += 1
    return picked[:limit]


async def label_batch(batch: list[dict]) -> list[dict]:
    prompt = "\n".join(f"{i}. {item['text']}" for i, item in enumerate(batch))
    # force: công tắc AI đang tắt cho ingest; đây là gán nhãn train do người vận hành yêu cầu.
    text = await call_llm(task=TASK, system_prompt=SYSTEM_PROMPT, user_prompt=prompt, max_tokens=2000, force=True)
    out = []
    for chunk in re.findall(r"\{[^{}]*\}", text or ""):
        try:
            item = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        idx, stage = item.get("i"), item.get("stage")
        if isinstance(idx, int) and 0 <= idx < len(batch) and stage in LABELS:
            out.append({**batch[idx], "label": stage})
    return out


async def main(args: argparse.Namespace) -> None:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if OUT_FILE.exists():
        done = {json.loads(line)["id"] for line in OUT_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}
    todo = [item for item in load_pool(args.limit) if item["id"] not in done]
    batches = [todo[i : i + BATCH] for i in range(0, len(todo), BATCH)]
    sem = asyncio.Semaphore(args.concurrency)
    counts: Counter[str] = Counter()

    async def run(batch: list[dict]) -> None:
        async with sem:
            rows = await label_batch(batch)
        with OUT_FILE.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                counts[row["label"]] += 1

    await asyncio.gather(*(run(batch) for batch in batches))
    print(f"gán {sum(counts.values())}/{len(todo)} câu: {dict(counts)} -> {OUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=4000)
    parser.add_argument("--concurrency", type=int, default=4)
    asyncio.run(main(parser.parse_args()))
