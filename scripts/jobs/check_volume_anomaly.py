"""Chạy tay một lượt so sản lượng bài hôm nay với 7 ngày trước (scheduler tự chạy mỗi ngày - xem
app/services/ops/health_checks.py):
    python -m scripts.jobs.check_volume_anomaly
"""

from __future__ import annotations

import asyncio

from app.services.ops.health_checks import check_volume_anomaly

if __name__ == "__main__":
    asyncio.run(check_volume_anomaly())
