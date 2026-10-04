"""Đẩy comments.relevance_label / relevance_confidence / relevance_labeled_at từ bản sao
D1 local lên D1 remote thật - bản sinh đôi cho bảng comments của
scripts/push_relevance_labels.py (posts). Cùng kiểu migrate schema: 3 cột được thêm
thẳng vào remote (và vào bản sao local) bằng ALTER TABLE, không có file migration
được commit - schema của D1 do repo cinemark-scraper sở hữu, không phải repo này.

Phạm vi: chỉ các comment đã có trên remote (khớp theo id) mới thực sự được cập nhật -
một câu UPDATE ... WHERE id IN (...) thường chỉ đơn giản khớp 0 dòng với id không có
trên remote. Comment có ở local nhưng chưa từng được đẩy lên remote là chuyện khác
(xem scripts/push_local_data_to_remote.py) - script này không tạo dòng, chỉ cập nhật
dòng có sẵn.

Gom nhiều dòng vào một UPDATE bằng CASE/WHEN (cùng trần tham số bind của D1 mà comment
ROWS_PER_BATCH trong push_relevance_labels.py ghi lại).

Chạy lại an toàn - mỗi dòng chỉ là UPDATE ... WHERE id = một trong số này, nên chạy
lại chỉ ghi lại đúng các giá trị đó.

    python -m scripts.push_comment_relevance_labels
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Cùng 7 tham số mỗi dòng / trần tham số bind của D1 như comment ROWS_PER_BATCH trong
# push_relevance_labels.py.
ROWS_PER_BATCH = 10


async def _push_batch(d1_query, batch: list[sqlite3.Row]) -> bool:
    ids = [r["id"] for r in batch]
    label_cases = " ".join("WHEN ? THEN ?" for _ in batch)
    conf_cases = " ".join("WHEN ? THEN ?" for _ in batch)
    time_cases = " ".join("WHEN ? THEN ?" for _ in batch)
    placeholders = ", ".join("?" for _ in batch)

    sql = f"""
        UPDATE comments SET
            relevance_label = CASE id {label_cases} END,
            relevance_confidence = CASE id {conf_cases} END,
            relevance_labeled_at = CASE id {time_cases} END
        WHERE id IN ({placeholders})
    """
    params: list[object] = []
    for r in batch:
        params.extend([r["id"], r["relevance_label"]])
    for r in batch:
        params.extend([r["id"], r["relevance_confidence"]])
    for r in batch:
        params.extend([r["id"], r["relevance_labeled_at"]])
    params.extend(ids)

    result = await d1_query(sql, params)
    return result is not None


async def push() -> None:
    settings.db_mode = "remote"  # ghi vào D1 thật; đọc thẳng từ file local bên dưới
    from app.services.d1 import d1_query  # import sau khi đã ép remote, không phải lúc nạp module

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError(
            "CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set (in .env) to push to "
            "the real D1 - this script has nothing to write to without them."
        )

    local_path = Path(settings.local_db_path)
    if not local_path.exists():
        raise RuntimeError(f"No local mirror at {local_path} - nothing to push.")

    local_conn = sqlite3.connect(local_path)
    local_conn.row_factory = sqlite3.Row

    try:
        rows = local_conn.execute(
            "SELECT id, relevance_label, relevance_confidence, relevance_labeled_at "
            "FROM comments WHERE relevance_label IS NOT NULL"
        ).fetchall()
        logger.info("rows_to_push", count=len(rows))

        pushed = 0
        failed = 0
        for i in range(0, len(rows), ROWS_PER_BATCH):
            batch = rows[i : i + ROWS_PER_BATCH]
            ok = await _push_batch(d1_query, batch)
            if ok:
                pushed += len(batch)
            else:
                failed += len(batch)
                logger.warning("batch_push_failed", batch_start=i, batch_size=len(batch))
            if (i // ROWS_PER_BATCH) % 25 == 0:
                logger.info("push_progress", pushed=pushed, failed=failed, of=len(rows))
    finally:
        local_conn.close()

    logger.info("push_comment_relevance_labels_finished", telegram=True, pushed=pushed, failed=failed, total=len(rows))


if __name__ == "__main__":
    asyncio.run(push())
