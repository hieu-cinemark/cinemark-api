"""Dấu hiệu E (bản không AI): bình luận chê nêu khẳng định về sự thật cần đối chiếu. KHÔNG BAO GIỜ strong."""

from __future__ import annotations

import re
from typing import Any

from app.services.attack.signals import Signal, mask_name
from app.services.attack.similar import normalize

CLAIM_PATTERNS: dict[str, tuple[str, re.Pattern[str]]] = {
    key: (label, re.compile(pattern))
    for key, (label, pattern) in {
        "dao_nhai": ("Đạo nhái / sao chép", r"\b(dao nhai|dao phim|sao chep|nhai lai)\b"),
        "seeding": (
            "Seeding / review thuê / tài khoản ảo",
            r"\b(seeding|review thue|thue review|acc clone|tai khoan ao)\b",
        ),
        "doanh_thu": ("Doanh thu / vé ảo", r"\b(doanh thu ao|ve ao|bom doanh thu|mua ve ao)\b"),
        "diem_ao": ("Điểm / rating ảo", r"\b(diem ao|rating ao|buff diem|cay diem)\b"),
        "kiem_duyet": (
            "Bị cấm / cắt cảnh / ngừng chiếu",
            r"\b(bi cam|cam chieu|cat canh|kiem duyet|rut khoi rap|ngung chieu)\b",
        ),
        "ep_suat": ("Ép suất chiếu", r"\b(ep suat|gianh suat|khong cho chieu|doc quyen suat)\b"),
    }.items()
}
MIN_COMMENTS, MIN_ACCOUNTS = 5, 3


def claim_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for key, (label, pattern) in CLAIM_PATTERNS.items():
        hits = [r for r in rows if pattern.search(normalize(r.get("message")))]
        accounts = {r.get("author_id") or r.get("author_name") or r["id"] for r in hits}
        if len(hits) < MIN_COMMENTS or len(accounts) < MIN_ACCOUNTS:
            continue
        top = sorted(hits, key=lambda r: -(r.get("likes") or 0))[:5]
        out.append(
            {
                "key": key,
                "label": label,
                "comments": len(hits),
                "accounts": len(accounts),
                "samples": [
                    {
                        "comment_id": r["id"],
                        "author": mask_name(r.get("author_name")),
                        "platform": r.get("platform"),
                        "post_url": r.get("post_url"),
                        "posted_at": r.get("posted_at"),
                        "text": (r.get("message") or "")[:300],
                        "likes": r.get("likes") or 0,
                    }
                    for r in top
                ],
            }
        )
    return sorted(out, key=lambda g: -g["comments"])


def claims_signal(rows: list[dict[str, Any]]) -> Signal:
    groups = claim_groups(rows)
    if not groups:
        return Signal(key="claims", fired=False, strong=False, score=0.0)
    return Signal(
        key="claims",
        fired=True,
        strong=False,  # chỉ ghi "cần kiểm tra", không tự nâng trạng thái
        score=float(groups[0]["comments"]),
        evidence=groups[0]["samples"],
        detail={"claims": groups[:5]},
    )
