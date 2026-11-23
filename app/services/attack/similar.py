"""Dấu hiệu B: nhiều tài khoản đăng bình luận chê gần như giống nhau trong thời gian ngắn.

Gom chặt (docs mục 2.4 B): trùng chữ sau chuẩn hoá trước (rẻ), rồi nối các cặp có cosine >= SIM (embedding e5, đã chuẩn hoá)
bằng union-find. Mỗi cụm đo số bình luận, số tài khoản khác nhau, số bài, nền tảng, độ dồn (% bình luận nằm trong 2 giờ dày
nhất) và độ giống trung bình. Cụm "mạnh": >= STRONG_ACCOUNTS tài khoản, >= STRONG_BURST trong 2 giờ, giống trung bình >= SIM.
300 người chê "kịch bản dở" rải cả tuần là phản hồi tự nhiên; 40 tài khoản đăng gần như y hệt một câu trong 2 giờ mới là tín hiệu.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np

from app.services.attack.signals import Signal, mask_name

SIM = 0.92
MIN_SIZE = 5
BURST_WINDOW = timedelta(hours=2)
STRONG_ACCOUNTS = 15
STRONG_BURST = 0.7
# Quá chừng này câu thì chỉ gom trùng chữ (ma trận cosine n x n quá lớn).
MAX_VECTOR_ROWS = 5000


def normalize(text: str | None) -> str:
    """Chữ thường, bỏ dấu, bỏ @tag/link/emoji/dấu câu - để bắt câu chép lại y hệt."""
    value = re.sub(r"@\S+|https?://\S+", " ", text or "")
    value = unicodedata.normalize("NFD", value).replace("đ", "d").replace("Đ", "d")
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn").lower()
    return " ".join(re.sub(r"[^\w\s]", " ", value).split())


def vn_hour(posted_at: str) -> str:
    at = datetime.fromisoformat(posted_at).astimezone(UTC) + timedelta(hours=7)
    return at.strftime("%Y-%m-%dT%H")


def _burst(times: list[datetime]) -> tuple[float, datetime, datetime]:
    """(tỷ lệ trong cửa sổ BURST_WINDOW dày nhất, đầu cửa sổ, cuối cửa sổ)."""
    times = sorted(times)
    best, best_i, j = 0, 0, 0
    for i in range(len(times)):
        while times[i] - times[j] > BURST_WINDOW:
            j += 1
        if i - j + 1 > best:
            best, best_i = i - j + 1, j
    start = times[best_i]
    return best / len(times), start, start + BURST_WINDOW


def similar_clusters(
    rows: list[dict[str, Any]], vectors: np.ndarray | None, *, sim: float = SIM
) -> list[dict[str, Any]]:
    """rows: [{"id","author_id","author_name","post_id","post_url","platform","posted_at","message","likes"}].
    vectors: (n, d) cùng thứ tự rows (dòng thiếu embedding là vector 0), hoặc None = chỉ gom trùng chữ."""
    n = len(rows)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    first: dict[str, int] = {}
    for i, row in enumerate(rows):
        key = normalize(row.get("message"))
        if len(key) >= 8:  # câu quá ngắn ("dở", "chán") trùng nhau là chuyện thường
            parent[find(i)] = find(first.setdefault(key, i))
    sims = None
    if vectors is not None and 0 < n <= MAX_VECTOR_ROWS:
        sims = vectors @ vectors.T
        for i, j in zip(*np.where(np.triu(sims, 1) >= sim), strict=True):
            parent[find(int(i))] = find(int(j))

    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    clusters = []
    for members in groups.values():
        if len(members) < MIN_SIZE:
            continue
        items = [rows[i] for i in members]
        accounts = {r.get("author_id") or r.get("author_name") or r["id"] for r in items}
        times = [datetime.fromisoformat(r["posted_at"]) for r in items if r.get("posted_at")]
        burst, start, end = _burst(times) if times else (0.0, None, None)
        if sims is not None:
            block = sims[np.ix_(members, members)]
            mean_sim = float((block.sum() - len(members)) / max(1, len(members) * (len(members) - 1)))
        else:
            mean_sim = 1.0  # chỉ có trùng chữ
        strong = len(accounts) >= STRONG_ACCOUNTS and burst >= STRONG_BURST and mean_sim >= sim
        clusters.append(
            {
                "size": len(items),
                "member_ids": [rows[i]["id"] for i in members],  # để dấu hiệu C/D tra ngược lại dòng bình luận
                "accounts": len(accounts),
                "posts": len({r["post_id"] for r in items}),
                "platforms": sorted({r.get("platform") or "" for r in items} - {""}),
                "burst": round(burst, 2),
                "mean_sim": round(mean_sim, 3),
                "strong": strong,
                "window": (start.astimezone(UTC).isoformat(), end.astimezone(UTC).isoformat()) if start else None,
                "sample": items[0].get("message") or "",
                "evidence": [
                    {
                        "comment_id": r["id"],
                        "author": mask_name(r.get("author_name")),
                        "platform": r.get("platform"),
                        "post_url": r.get("post_url"),
                        "posted_at": r.get("posted_at"),
                        "text": (r.get("message") or "")[:300],
                        "likes": r.get("likes") or 0,
                    }
                    for r in sorted(items, key=lambda r: r.get("posted_at") or "")[:10]
                ],
            }
        )
    return sorted(clusters, key=lambda c: (-c["accounts"], -c["size"]))


def similar_signal(
    rows: list[dict[str, Any]], vectors: np.ndarray | None, clusters: list[dict[str, Any]] | None = None
) -> Signal:
    """`clusters`: cụm đã tính sẵn (C/D dùng chung); None thì tự tính."""
    clusters = clusters if clusters is not None else similar_clusters(rows, vectors)
    if not clusters:
        return Signal(key="similar", fired=False, strong=False, score=0.0, detail={"comments": len(rows)})
    top = clusters[0]
    strong = [c for c in clusters if c["strong"]]
    lead = strong[0] if strong else top
    window = None
    if lead["window"]:
        window = (vn_hour(lead["window"][0]), vn_hour(lead["window"][1]))
    return Signal(
        key="similar",
        fired=True,
        strong=bool(strong),
        score=float(lead["accounts"]),
        window=window,
        evidence=lead["evidence"],
        detail={
            "comments": len(rows),
            "clusters": [{k: v for k, v in c.items() if k not in ("evidence", "member_ids")} for c in clusters[:5]],
        },
    )
