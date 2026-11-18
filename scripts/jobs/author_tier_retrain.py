"""Train lại mô hình tầng tác giả - timer cinemark-author-retrain, mỗi tuần.

1. Xuất dữ liệu (scripts/ml/author_tier_export.py): nhãn hiện có TRỪ nhãn do chính mô hình gắn, đặc trưng từ mọi bài.
2. Train và đo (scripts/ml/author_tier_train.py --save): chỉ thay model khi macro-F1 trên tập kiểm tra không thấp hơn model đang
   dùng quá 0.01. Nhãn mới (gắn tay, AI, thêm tín hiệu tác giả lưu từ 18/11/2026) làm model tốt dần mà không cần người duyệt.

    python -m scripts.jobs.author_tier_retrain
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

from app.core.logging import get_logger
from scripts.ml.author_tier_export import export

ROOT = Path(__file__).resolve().parents[2]
logger = get_logger(__name__)


def main() -> int:
    counts = asyncio.run(export())
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "ml" / "author_tier_train.py"), "--save"], cwd=ROOT, check=False
    )
    # 0 = đã thay model, 2 = model mới tệ hơn nên giữ model cũ (không phải lỗi).
    logger.info("author_model_retrained", replaced=result.returncode == 0, exit_code=result.returncode, **counts)
    return 0 if result.returncode in (0, 2) else result.returncode


if __name__ == "__main__":
    sys.exit(main())
