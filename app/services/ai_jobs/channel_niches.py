"""Gắn tệp khán giả (niche) cho kênh bằng AI.

Mỗi giờ: lấy mẫu bài 14 ngày, chọn kênh có từ 2 bài chưa có niche (hoặc niche cũ hơn 30 ngày, trừ niche gắn tay), hỏi
AI theo lô 15 kênh và ghi `channel_profiles`. Danh sách niche lấy từ setting `audience_niches`.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, literal, select, true
from sqlalchemy.dialects.sqlite import insert

from app.ai.prompts.channel_niches import CHANNEL_NICHES_SYSTEM, DEFAULT_NICHES
from app.ai.runtime import call_llm_json
from app.core.jsonutil import loads_or_none
from app.core.logging import get_logger
from app.repositories.d1.post_filters import relevant_post
from app.repositories.d1.sql import d1_exec, json_rows
from app.repositories.d1.tables import channel_profiles, config_settings, posts
from app.services.ai_jobs.channels import channel_of_post

logger = get_logger(__name__)

LOOKBACK_DAYS = 14
SAMPLE_POSTS = 3000
CHANNELS_PER_RUN = 30
BATCH = 15
MIN_POSTS = 2
RECHECK_DAYS = 30
MIN_CONFIDENCE = 50
CAPTIONS_PER_CHANNEL = 4


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


async def _niches() -> list[dict[str, Any]]:
    niches: list[dict[str, Any]] = DEFAULT_NICHES
    rows = await d1_exec(
        select(config_settings.c.value).where(config_settings.c.key == "audience_niches").limit(1), database="config"
    )
    if rows:
        parsed = loads_or_none(rows[0]["value"], source="audience_niches")
        if isinstance(parsed, list):
            niches = parsed
    return [n for n in niches if isinstance(n, dict) and n.get("key") and n.get("active") is not False]


async def save_niches(rows: list[dict[str, Any]], stamp: str) -> None:
    """Ghi niche AI cho một lô kênh ({platform, handle, name, niche, confidence, reason}) - một câu INSERT ... SELECT qua
    json_each. Niche nhập tay (niche_source = 'manual') không bị ghi đè; tên đã có thì giữ."""
    t = channel_profiles
    src = json_rows(rows)
    field = src.c.value.op("->>")
    stmt = insert(t).from_select(
        ["platform", "handle", "name", "niche", "niche_confidence", "niche_reason", "niche_source", "niche_at"],
        select(
            field("platform"),
            field("handle"),
            field("name"),
            field("niche"),
            field("confidence"),
            field("reason"),
            literal("ai"),
            literal(stamp),
        ).where(true()),
    )
    new = stmt.excluded
    # Niche nhập tay (niche_source = 'manual') không bị AI ghi đè; tên đã có thì giữ.
    await d1_exec(
        stmt.on_conflict_do_update(
            index_elements=[t.c.platform, t.c.handle],
            set_={
                "name": func.coalesce(t.c.name, new.name),
                "niche": new.niche,
                "niche_confidence": new.niche_confidence,
                "niche_reason": new.niche_reason,
                "niche_source": "ai",
                "niche_at": new.niche_at,
            },
            where=func.coalesce(t.c.niche_source, "") != "manual",
        )
    )


async def process_channel_niches() -> dict[str, Any]:
    """Một lượt gắn tệp khán giả cho kênh: lấy mẫu bài 14 ngày, chọn kênh chưa có niche (hoặc niche quá
    `RECHECK_DAYS`, trừ nhãn gắn tay), hỏi AI theo lô và ghi `channel_profiles`. Trả số kênh đã kiểm tra/gắn nhãn,
    hoặc lý do bỏ qua (`no_niches`, `d1_unavailable`)."""
    niches = await _niches()
    if not niches:
        return {"checked": 0, "labelled": 0, "skipped": "no_niches"}
    now = datetime.now(tz=UTC)
    since = _iso(now - timedelta(days=LOOKBACK_DAYS))
    recheck_before = _iso(now - timedelta(days=RECHECK_DAYS))
    p, cp = posts.alias("p"), channel_profiles.c
    sample = await d1_exec(
        select(p.c.platform, p.c.author, p.c.url, func.substr(p.c.content, 1, 240).label("content"))
        .where(p.c.posted_at >= since, relevant_post(p))
        .order_by((func.coalesce(p.c.like_count, 0) + func.coalesce(p.c.reply_count, 0)).desc())
        .limit(SAMPLE_POSTS),
        timeout=60.0,
    )
    profiles = await d1_exec(
        select(cp.platform, cp.handle, cp.niche_at, cp.niche_source).where(cp.niche_at.is_not(None)), timeout=60.0
    )
    if sample is None or profiles is None:
        return {"checked": 0, "labelled": 0, "skipped": "d1_unavailable"}
    done = {
        f"{p['platform']}|{p['handle']}"
        for p in profiles
        if p.get("niche_source") == "manual" or (p.get("niche_at") or "") >= recheck_before
    }
    by_channel: dict[str, dict[str, Any]] = {}
    for post in sample:
        channel = channel_of_post(post)
        if not channel:
            continue
        key = f"{post['platform']}|{channel['handle']}"
        if key in done:
            continue
        entry = by_channel.setdefault(
            key, {"platform": post["platform"], "handle": channel["handle"], "name": "", "captions": [], "posts": 0}
        )
        entry["posts"] += 1
        if not entry["name"] and channel["kind"] != "group" and post.get("author"):
            entry["name"] = post["author"]
        if len(entry["captions"]) < CAPTIONS_PER_CHANNEL and (post.get("content") or "").strip():
            entry["captions"].append(" ".join(str(post["content"]).split()))
    queue = sorted((c for c in by_channel.values() if c["posts"] >= MIN_POSTS), key=lambda c: -c["posts"])[
        :CHANNELS_PER_RUN
    ]
    if not queue:
        return {"checked": 0, "labelled": 0}
    keys = {n["key"] for n in niches}
    stamp = _iso(datetime.now(tz=UTC))
    labelled = 0
    for start in range(0, len(queue), BATCH):
        batch = queue[start : start + BATCH]
        user = json.dumps(
            {
                "niches": [{"key": n["key"], "label": n.get("label", n["key"])} for n in niches],
                "channels": [
                    {
                        "i": i + 1,
                        "platform": c["platform"],
                        "name": c["name"] or c["handle"],
                        "handle": c["handle"],
                        "posts": c["captions"],
                    }
                    for i, c in enumerate(batch)
                ],
            },
            ensure_ascii=False,
        )
        parsed, _ = await call_llm_json(
            system_prompt=CHANNEL_NICHES_SYSTEM, user_prompt=user, max_tokens=3000, task="channel_niches"
        )
        results = parsed.get("results") if isinstance(parsed, dict) else None
        rows = []
        for i, channel in enumerate(batch):
            hit = next((r for r in results or [] if isinstance(r, dict) and str(r.get("i")) == str(i + 1)), {})
            try:
                confidence = max(0, min(100, round(float(hit.get("confidence") or 0))))
            except TypeError, ValueError:
                confidence = 0
            niche = hit.get("niche") if hit.get("niche") in keys and confidence >= MIN_CONFIDENCE else None
            if niche:
                labelled += 1
            rows.append(
                {
                    "platform": channel["platform"],
                    "handle": channel["handle"],
                    "name": channel["name"] or None,
                    "niche": niche,
                    "confidence": confidence,
                    "reason": str(hit.get("reason") or "")[:200],
                }
            )
        await save_niches(rows, stamp)
    return {"checked": len(queue), "labelled": labelled}
