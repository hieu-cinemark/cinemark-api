"""Dọn Review Angles đã gom (16/11/2026): gộp góc vụn + gắn khen/chê cho dẫn chứng cũ.

1. Gộp: trước 15/11/2026 không có trần góc mỗi khía cạnh nên một phim có tới 222 góc (Bò Sữa Bay, khía cạnh kịch bản 38
   góc gần nghĩa). Khía cạnh nào quá MERGE_OVER góc thì nhờ AI gộp còn tối đa 8 nhóm (tasks.merge_groups).
2. Khen/chê: dẫn chứng đọc trước 16/11/2026 chưa có `stance`. AI gắn theo đoạn trích + tên góc, STANCE_BATCH đoạn/lần.

Đọc/ghi state.json trên R2 của từng phim (analysis/review-angles/v2/<movieId>/), giữ khoá Redis của job
(app/services/ai_jobs/review_angles.py) trong lúc ghi để không đè lượt đọc đang chạy.

Mặc định chỉ in kế hoạch (không gọi AI, không ghi). --apply mới chạy.

    python -m scripts.tools.review_angles_cleanup
    python -m scripts.tools.review_angles_cleanup --apply
    python -m scripts.tools.review_angles_cleanup --apply --movie <movieId> --skip-stance
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from typing import Any

from app.ai.prompts.review_angles import MERGE_ANGLES_SYSTEM, REVIEW_ASPECTS, STANCE_SYSTEM
from app.ai.runtime import call_llm_json
from app.ai.tasks.review_angles import merge_groups, parse_stances
from app.clients import lake
from app.clients.redis import get_redis_client
from app.services.ai_jobs import review_angles as ra

MERGE_OVER = 8
STANCE_BATCH = 60
LOCK_WAIT_SECONDS = 600


async def _lock(movie_id: str) -> str:
    key = f"{ra._PREFIX}lock:{movie_id}"
    for _ in range(LOCK_WAIT_SECONDS // 5):
        if await get_redis_client().set(key, "cleanup", nx=True, ex=900):
            return key
        await asyncio.sleep(5)
    raise SystemExit(f"{movie_id}: job đang giữ khoá quá lâu, thử lại sau")


async def _merge(state: dict[str, Any], title: str) -> list[str]:
    notes = []
    counts = Counter(a["aspect"] for a in state["angles"])
    for aspect, count in counts.items():
        if count <= MERGE_OVER:
            continue
        items = [
            {
                "id": a["id"],
                "name": a["name"],
                "description": (a.get("description") or "")[:160],
                "posts": len(a["refs"]),
            }
            for a in state["angles"]
            if a["aspect"] == aspect
        ]
        user = json.dumps(
            {"movieTitle": title, "aspect": REVIEW_ASPECTS.get(aspect, aspect), "angles": items}, ensure_ascii=False
        )
        raw, _ = await call_llm_json(
            system_prompt=MERGE_ANGLES_SYSTEM, user_prompt=user, max_tokens=6000, task="review_angles", temperature=0.1
        )
        merged = merge_groups(state["angles"], aspect, raw)
        if merged is None:
            notes.append(f"{aspect}: AI trả không dùng được, giữ nguyên {count}")
            continue
        state["angles"] = merged
        notes.append(f"{aspect}: {count} -> {sum(1 for a in merged if a['aspect'] == aspect)} góc")
    return notes


async def _stances(state: dict[str, Any], title: str) -> int:
    todo = [(angle, ref) for angle in state["angles"] for ref in angle["refs"] if not ref.get("stance")]
    labelled = 0
    for start in range(0, len(todo), STANCE_BATCH):
        chunk = todo[start : start + STANCE_BATCH]
        user = json.dumps(
            {
                "movieTitle": title,
                "items": [
                    {"i": i, "angle": angle["name"], "quote": ref["quote"]} for i, (angle, ref) in enumerate(chunk)
                ],
            },
            ensure_ascii=False,
        )
        raw, _ = await call_llm_json(
            system_prompt=STANCE_SYSTEM, user_prompt=user, max_tokens=4000, task="review_angles", temperature=0.0
        )
        for i, stance in parse_stances(raw, len(chunk)).items():
            chunk[i][1]["stance"] = stance
            labelled += 1
    return labelled


async def run(args: argparse.Namespace) -> None:
    keys = [k for k in await lake.list_keys("analysis/review-angles/v2/") if k.endswith("/state.json")]
    for key in keys:
        movie_id = key.split("/")[3]
        if args.movie and movie_id != args.movie:
            continue
        state = await ra.read_json(key)
        if not state or not state.get("angles"):
            continue
        movie = await ra.load_movie(movie_id) or {"title": movie_id}
        over = {asp: n for asp, n in Counter(a["aspect"] for a in state["angles"]).items() if n > MERGE_OVER}
        missing = sum(1 for a in state["angles"] for r in a["refs"] if not r.get("stance"))
        print(
            f"{movie['title']}: {len(state['angles'])} góc, khía cạnh cần gộp {over or '-'}, {missing} dẫn chứng chưa có khen/chê"
        )
        if not args.apply:
            continue
        lock = await _lock(movie_id)
        try:
            state = await ra.read_json(key)  # đọc lại trong khoá - job có thể vừa ghi
            for note in await _merge(state, movie["title"]) if not args.skip_merge else []:
                print("   ", note)
            if not args.skip_stance:
                print(f"    khen/chê: gắn {await _stances(state, movie['title'])}")
            await ra.write_json(key, state)
        finally:
            await get_redis_client().delete(lock)
    if not args.apply:
        print("Chạy thử - chưa gọi AI, chưa ghi. Thêm --apply để chạy.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--movie")
    parser.add_argument("--skip-merge", action="store_true")
    parser.add_argument("--skip-stance", action="store_true")
    asyncio.run(run(parser.parse_args()))
