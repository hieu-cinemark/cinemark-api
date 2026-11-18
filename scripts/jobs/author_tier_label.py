"""Gắn tầng tác giả bằng mô hình local (app/services/ai_jobs/author_model.py) - timer cinemark-author-tier, mỗi ngày.

Đọc mọi bài một lần, dự đoán tầng cho tác giả CHƯA có nhãn, tự áp nhãn có xác suất >= proxy_settings.author_model_threshold
(mặc định 0.9). Không đè nhãn có sẵn. Thiếu file models/author_tier/model.joblib thì bỏ qua.

    python -m scripts.jobs.author_tier_label --dry-run          # chỉ đếm, không ghi
    python -m scripts.jobs.author_tier_label
    python -m scripts.jobs.author_tier_label --threshold 0.95
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.services.ai_jobs.author_model import label_with_model

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="dự đoán và đếm, không ghi D1")
    parser.add_argument("--threshold", type=float, help="ghi đè ngưỡng xác suất (mặc định đọc proxy_settings)")
    args = parser.parse_args()
    result = asyncio.run(label_with_model(threshold=args.threshold, dry_run=args.dry_run))
    print(json.dumps(result, ensure_ascii=False, indent=1))
