"""Đường dẫn cũ, chỉ giữ tạm để unit systemd trên VPS chưa sửa vẫn chạy - dùng `python -m scripts.jobs.backfill_comment_sentiment`.

Xoá file này khi mọi unit đã chuyển sang đường dẫn mới.
"""

import runpy

if __name__ == "__main__":
    runpy.run_module("scripts.jobs.backfill_comment_sentiment", run_name="__main__", alter_sys=True)
