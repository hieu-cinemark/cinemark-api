"""Dấu hiệu D: một luận điểm chê lan qua nhiều kênh / nền tảng. Chỉ thêm ngữ cảnh, KHÔNG BAO GIỜ strong."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.ai_jobs.channels import channel_of_post
from app.services.attack.signals import Signal

MIN_CHANNELS, MIN_PLATFORMS = 3, 2
COMMON_SOURCE_SHARE = 0.6
TOP_CLUSTERS = 5


def _channel(row: dict[str, Any]) -> str:
    found = channel_of_post(
        {"url": row.get("post_url"), "platform": row.get("platform"), "author": row.get("post_author")}
    )
    return f"{row.get('platform')}:{found['handle']}" if found else f"post:{row['post_id']}"


def spread_of(cluster: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    items = [by_id[i] for i in cluster.get("member_ids", []) if i in by_id]
    first_by_post: dict[str, datetime] = {}
    per_post: dict[str, int] = {}
    for r in sorted(items, key=lambda r: r["posted_at"]):
        first_by_post.setdefault(r["post_id"], datetime.fromisoformat(r["posted_at"]))
        per_post[r["post_id"]] = per_post.get(r["post_id"], 0) + 1
    order = sorted(first_by_post.values())
    minutes = round((order[9] - order[0]).total_seconds() / 60) if len(order) >= 10 else None
    top_share = max(per_post.values()) / len(items) if items else 0.0
    return {
        "sample": cluster.get("sample", ""),
        "posts": len(first_by_post),
        "channels": len({_channel(r) for r in items}),
        "platforms": len({r.get("platform") for r in items if r.get("platform")}),
        "minutes_to_10_posts": minutes,
        "top_post_share": round(top_share, 2),
        "common_source": top_share >= COMMON_SOURCE_SHARE,
    }


def spread_signal(rows: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> Signal:
    by_id = {r["id"]: r for r in rows}
    spreads = [spread_of(c, by_id) for c in clusters[:TOP_CLUSTERS]]
    wide = [
        (c, s)
        for c, s in zip(clusters, spreads, strict=False)
        if s["channels"] >= MIN_CHANNELS or s["platforms"] >= MIN_PLATFORMS
    ]
    if not wide:
        return Signal(key="spread", fired=False, strong=False, score=0.0, detail={"spread": spreads})
    cluster, top = max(wide, key=lambda pair: pair[1]["channels"])
    return Signal(
        key="spread",
        fired=True,
        strong=False,  # chỉ là ngữ cảnh - không được tự nâng trạng thái
        score=float(top["channels"]),
        window=tuple(cluster["window"]) if cluster.get("window") else None,
        evidence=cluster.get("evidence", []),
        detail={"spread": spreads},
    )
