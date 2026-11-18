"""Gắn tầng tác giả bằng mô hình chạy local (app/ml/author_tier.py) thay cho hỏi AI - 18/11/2026.

Mỗi ngày (systemd cinemark-author-tier.timer -> scripts/jobs/author_tier_label.py): đọc mọi bài một lần (theo trang, nghỉ giữa
các trang), gom theo tác giả bằng đúng hàm đặc trưng lúc train, dự đoán cho tác giả CHƯA có nhãn và tự áp nhãn có xác suất >=
proxy_settings.author_model_threshold vào author_classifications (không đè nhãn có sẵn - UNIQUE handle). Mỗi nhãn áp ghi một dòng
author_ai_review với model = "local:author_tier" để job AI theo phim (author_classify.py) không hỏi lại, và để lần train sau
loại nhãn do chính mô hình gắn ra khỏi dữ liệu (không tự học lại cái sai của mình). Tác giả mô hình chưa chắc để nguyên cho job
AI, hoặc chờ lần train sau tốt hơn.

Cũng là nơi đọc dữ liệu cho scripts/ml/author_tier_export.py (train) - train và gắn nhãn dùng chung load_posts / group_authors.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.logging import get_logger
from app.repositories.d1.sql import d1_insert_many, d1_query
from app.services.ai_jobs.author_classify import _ensure_tables, classifications, review
from app.services.ai_jobs.channels import extract_handle

logger = get_logger(__name__)

ROOT = Path(__file__).resolve().parents[3]
MODEL_PATH = ROOT / "models" / "author_tier" / "model.joblib"
PAGE = 5000
PAUSE = 1.5


async def _paged(sql: str, key: str, *, database: str = "scraper", pause: float = PAUSE) -> list[dict[str, Any]]:
    """Đọc hết một bảng theo khoá tăng dần `key` (keyset), PAGE dòng mỗi lần, nghỉ `pause` giây giữa hai trang (D1 từng quá tải
    7429 khi đọc dồn)."""
    out: list[dict[str, Any]] = []
    last = ""
    while True:
        rows = await d1_query(sql, [last, PAGE], database=database, timeout=60.0, quiet=True)
        if rows is None:
            raise RuntimeError(f"D1 {database}: lỗi khi đọc trang sau {last!r}")
        out.extend(rows)
        if len(rows) < PAGE:
            return out
        last = rows[-1][key]
        await asyncio.sleep(pause)


async def load_labels(*, exclude_model_labels: bool = False) -> dict[tuple[str, str], str]:
    """(nền tảng, handle) -> tầng đã chuẩn hoá (fan -> outsider); nền tảng "" là nhãn không ghi nền tảng. `exclude_model_labels`:
    bỏ nhãn do chính mô hình áp (author_ai_review.model = local:author_tier) - dùng khi xuất dữ liệu train."""
    from app.ml.author_tier import MODEL_SOURCE, normalize_tier

    rows = await _paged(
        "SELECT id, platform, handle, tier FROM author_classifications WHERE id > ? ORDER BY id LIMIT ?",
        "id",
        database="app",
        pause=0.5,
    )
    model_made: set[str] = set()
    if exclude_model_labels:
        made = await d1_query(
            "SELECT handle FROM author_ai_review WHERE model = ? AND applied = 1", [MODEL_SOURCE], database="app"
        )
        model_made = {str(r["handle"]) for r in made or []}
    labels: dict[tuple[str, str], str] = {}
    for row in rows:
        tier = normalize_tier(row["tier"])
        handle = str(row["handle"] or "").strip().lower().lstrip("@")
        if tier and handle and handle not in model_made:
            labels[(str(row["platform"] or ""), handle)] = tier
    return labels


async def load_posts() -> list[dict[str, Any]]:
    signals = ", ".join(
        f"json_extract(raw_json, '$.{key}') AS {key}"
        for key in (
            "author_verified",
            "author_is_business",
            "author_is_seller",
            "is_ad",
            "is_branded",
            "author_signature",
        )
    )
    return await _paged(
        "SELECT id, platform, url, author, json_extract(raw_json, '$.author_name') AS author_name, "
        f"json_extract(raw_json, '$.author_followers') AS raw_followers, {signals}, substr(content, 1, 300) AS content, "
        "movie_id, keyword_id, like_count, reply_count, view_count, posted_at, relevance_label "
        "FROM posts WHERE id > ? ORDER BY id LIMIT ?",
        "id",
    )


async def load_profiles() -> dict[tuple[str, str], dict[str, Any]]:
    rows = await d1_query("SELECT platform, handle, followers, niche FROM channel_profiles", timeout=60.0, quiet=True)
    return {(str(r["platform"]), str(r["handle"] or "").lower()): r for r in rows or []}


def group_authors(
    posts: list[dict[str, Any]], profiles: dict[tuple[str, str], dict[str, Any]]
) -> dict[tuple[str, str], dict]:
    """(nền tảng, handle) -> dòng đặc trưng (app.ml.author_tier.author_features)."""
    from app.ml.author_tier import author_features

    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for post in posts:
        platform = str(post["platform"] or "")
        handle = extract_handle(platform, post.get("author"), post.get("url")).strip().lstrip("@")
        if handle:
            groups[(platform, handle)].append(post)
    return {key: author_features(key[0], key[1], items, profiles.get(key)) for key, items in groups.items()}


def pick_unlabeled(authors: dict[tuple[str, str], dict], labels: dict[tuple[str, str], str]) -> list[dict[str, Any]]:
    """Tác giả chưa có nhãn (theo nền tảng hoặc nhãn chung của handle) và có ít nhất một bài liên quan."""
    labeled_handles = {handle for _platform, handle in labels}
    return [
        row for (platform, handle), row in authors.items() if handle not in labeled_handles and row["n_relevant"] > 0
    ]


async def label_with_model(*, threshold: float | None = None, dry_run: bool = False) -> dict[str, Any]:
    """Một lượt gắn nhãn bằng mô hình. Không có file model thì bỏ qua (job AI vẫn chạy như trước)."""
    from app.ml.author_tier import MODEL_SOURCE, confident, load_bundle, predict
    from app.services.accounts.platform_config_db import crawl_setting

    if not MODEL_PATH.exists():
        logger.warning("author_model_missing", path=str(MODEL_PATH))
        return {"skipped": "no_model"}
    bundle = await asyncio.to_thread(load_bundle, MODEL_PATH)
    threshold = float(threshold if threshold is not None else await crawl_setting("author_model_threshold"))
    await _ensure_tables()
    labels = await load_labels()
    posts = await load_posts()
    authors = group_authors(posts, await load_profiles())
    pending = pick_unlabeled(authors, labels)
    predictions = await asyncio.to_thread(predict, bundle, pending)
    chosen = confident(predictions, threshold)
    stats: dict[str, Any] = {
        "authors": len(authors),
        "unlabeled": len(pending),
        "applied": len(chosen),
        "threshold": threshold,
        "by_tier": {},
    }
    for index in chosen:
        tier = predictions[index][0]
        stats["by_tier"][tier] = stats["by_tier"].get(tier, 0) + 1
    if dry_run or not chosen:
        logger.info("author_model_labelled", dry_run=dry_run, **stats)
        return stats
    now = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    rows = [pending[i] for i in chosen]
    for start in range(0, len(rows), 500):
        part = rows[start : start + 500]
        picked = chosen[start : start + 500]
        # Nhãn có sẵn (gắn tay / AI) luôn thắng: UNIQUE handle, không cập nhật khi trùng.
        await d1_insert_many(
            classifications,
            [
                {
                    "id": uuid.uuid4().hex,
                    "platform": row["platform"],
                    "handle": row["handle"],
                    "tier": predictions[i][0],
                }
                for row, i in zip(part, picked, strict=True)
            ],
            conflict=["handle"],
            update=(),
            database="app",
        )
        await d1_insert_many(
            review,
            [
                {
                    "handle": row["handle"],
                    "platform": row["platform"],
                    "movie_id": None,
                    "tier": predictions[i][0],
                    "confidence": round(predictions[i][1] * 100),
                    "reason": f"mô hình local, xác suất {predictions[i][1]:.2f}",
                    "model": MODEL_SOURCE,
                    "applied": 1,
                    "reviewed_at": now,
                }
                for row, i in zip(part, picked, strict=True)
            ],
            conflict=["handle"],
            database="app",
        )
        await asyncio.sleep(0.5)
    logger.info("author_model_labelled", dry_run=False, **stats)
    return stats
