"""Chạy tay một lượt kiểm tra sức khoẻ mọi tài khoản (scheduler tự chạy mỗi 6 giờ - xem
app/services/ops/health_checks.py):
    python -m scripts.jobs.check_account_health
"""

from __future__ import annotations

import asyncio

from app.services.ops.health_checks import check_account_health

if __name__ == "__main__":
    asyncio.run(check_account_health())
