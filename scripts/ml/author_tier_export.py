"""Xuất dữ liệu train mô hình phân loại tầng tác giả (owned / pro / media / outsider) - 18/11/2026.

Nhãn: author_classifications (database D1 "app"), `fan` gộp vào `outsider`, BỎ nhãn do chính mô hình áp (model =
local:author_tier) để lần train sau không học lại cái sai của mình. Nhãn không ghi nền tảng áp cho mọi nền tảng có handle đó.
Đặc trưng: app/services/ai_jobs/author_model.group_authors - đúng hàm job gắn nhãn hằng ngày dùng.

Chỉ đọc D1 (theo trang, có nghỉ). Ghi data/author_tiers/dataset.jsonl (tác giả có nhãn) và unlabeled.jsonl.

    python -m scripts.ml.author_tier_export
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path

from app.services.ai_jobs.author_model import group_authors, load_labels, load_posts, load_profiles

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "author_tiers"


async def export(out_dir: Path = OUT_DIR) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    labels = await load_labels(exclude_model_labels=True)
    print(f"nhãn (không tính nhãn do mô hình gắn): {len(labels):,}", flush=True)
    posts = await load_posts()
    print(f"bài: {len(posts):,}", flush=True)
    authors = group_authors(posts, await load_profiles())
    labelled = unlabelled = 0
    tiers: Counter = Counter()
    with (
        (out_dir / "dataset.jsonl").open("w", encoding="utf-8") as out,
        (out_dir / "unlabeled.jsonl").open("w", encoding="utf-8") as rest,
    ):
        for (platform, handle), author in authors.items():
            tier = labels.get((platform, handle)) or labels.get(("", handle))
            if tier:
                out.write(json.dumps({**author, "tier": tier}, ensure_ascii=False) + "\n")
                labelled += 1
                tiers[(platform, tier)] += 1
            else:
                rest.write(json.dumps(author, ensure_ascii=False) + "\n")
                unlabelled += 1
    print(f"đã ghi {labelled:,} tác giả có nhãn, {unlabelled:,} chưa nhãn -> {out_dir}")
    for (platform, tier), n in sorted(tiers.items()):
        print(f"  {platform:9} {tier:9} {n:,}")
    return {"labelled": labelled, "unlabelled": unlabelled}


if __name__ == "__main__":
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    asyncio.run(export())
