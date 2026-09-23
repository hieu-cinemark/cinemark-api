"""Pushes posts.relevance_label / relevance_confidence / relevance_labeled_at
from the local D1 mirror up to the real remote D1 - the data half of
today's "gán nhãn AI" migration (schema half already applied directly via
3 ALTER TABLE statements against remote).

Scope: only posts that already exist on remote (matched by id) actually
get updated - a plain UPDATE ... WHERE id IN (...) simply matches zero
rows for any id not present on remote, no error, so there's no need to
pre-fetch remote's id set to filter locally first (the first version of
this script did that via 44 paginated SELECTs - each one apparently doing
an increasingly expensive OFFSET scan over the posts table, since that
alone hadn't finished after 11 minutes when this was rewritten - not
worth the cost when D1 already does the filtering for free). Posts that
exist locally but were never pushed to remote at all are a separate
concern (see scripts/push_local_data_to_remote.py) - this script does not
create rows, only updates existing ones.

Batches multiple rows into one UPDATE via CASE/WHEN (there's no multi-row
UPDATE syntax the way INSERT has multi-row VALUES) - same D1 bound-
parameter ceiling push_local_data_to_remote.py's own _MAX_PARAMS_PER_BATCH
comment documents, so ROWS_PER_BATCH is sized the same conservative way
(rows * params_per_row comfortably under it).

Safe to re-run - every row is a plain UPDATE ... WHERE id = one of these,
so re-running just re-writes the same values.

    python -m scripts.push_relevance_labels
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# 7 params per row (id+label, id+confidence, id+labeled_at for the 3 CASE
# WHENs, plus one more id for the IN clause) - confirmed live: 20 rows/batch
# (140 params) got "too many SQL variables" from D1, so its real ceiling is
# below that, consistent with push_local_data_to_remote.py's own
# _MAX_PARAMS_PER_BATCH=90 comment. 10 rows (70 params) stays safely under it.
ROWS_PER_BATCH = 10


async def _push_batch(d1_query, batch: list[sqlite3.Row]) -> bool:
    ids = [r["id"] for r in batch]
    label_cases = " ".join("WHEN ? THEN ?" for _ in batch)
    conf_cases = " ".join("WHEN ? THEN ?" for _ in batch)
    time_cases = " ".join("WHEN ? THEN ?" for _ in batch)
    placeholders = ", ".join("?" for _ in batch)

    sql = f"""
        UPDATE posts SET
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
    settings.db_mode = "remote"  # writes target real D1; reads come from the local file directly below
    from app.services.d1 import d1_query  # imported after forcing remote, not at module load

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
            "FROM posts WHERE relevance_label IS NOT NULL"
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

    logger.info("push_relevance_labels_finished", telegram=True, pushed=pushed, failed=failed, total=len(rows))


if __name__ == "__main__":
    asyncio.run(push())
