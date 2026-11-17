"""Tên MỌI hàng đợi Redis mà cinemark-api nạp việc cho spider-hub - khai báo một chỗ (trước 17/11/2026 mỗi file tự viết lại tên khoá).

Phía đọc là `social_crawler/consumer/loop.py` + `consumer/jobs/*.py` của spider-hub, dùng `runtime/job_queue.JobQueue`. Tên khoá phải KHỚP từng
ký tự giữa hai repo; đổi một bên mà quên bên kia thì việc đứng yên, không có lỗi báo. Mỗi hàng có thêm khoá `<tên>:processing` do spider-hub quản
lý (việc đang chạy) - bên này không đụng tới.

| Hằng | Khoá (sau tiền tố) | Việc | Phía spider-hub đọc |
|---|---|---|---|
| PRIORITY_FACEBOOK | crawl_priority:facebook | chen ngang, chạy trước Kafka | `_drain_priority_queue` |
| IDLE_FACEBOOK | crawl_idle:facebook | job không cần tài khoản (post_metrics), chạy khi rảnh | `_run_one_idle_job` |
| PUBLIC_PAGES_FACEBOOK | crawl_public_pages:facebook | đọc page Facebook không đăng nhập | `_run_public_pages_worker` |
| PUBLIC_PAGES_THREADS | crawl_public_pages:threads | đọc kênh Threads không đăng nhập | `_run_threads_channel_worker` |
| YOUTUBE_QUEUE_KEYS | crawl_youtube:search / :comments / :processing | YouTube tìm kiếm / bình luận | `_run_youtube_lane` |
| THREADS_PROFILES | threads:profile_queue | hồ sơ kênh Threads (cinemark-api tự đọc) | (không) |

Hai topic Kafka (`crawl_requests`, `crawl_requests.facebook_public_comments`) khai ở `app/clients/kafka.py`.
"""

from __future__ import annotations

from app.clients.redis import REDIS_KEY_PREFIX

PRIORITY_FACEBOOK = f"{REDIS_KEY_PREFIX}crawl_priority:facebook"
IDLE_FACEBOOK = f"{REDIS_KEY_PREFIX}crawl_idle:facebook"
PUBLIC_PAGES_FACEBOOK = f"{REDIS_KEY_PREFIX}crawl_public_pages:facebook"
PUBLIC_PAGES_THREADS = f"{REDIS_KEY_PREFIX}crawl_public_pages:threads"
# Gồm cả khoá đang chạy: nhịp YouTube liên tục coi việc đang chạy là chưa rảnh (public_continuous).
YOUTUBE_QUEUE_KEYS = tuple(f"{REDIS_KEY_PREFIX}crawl_youtube:{name}" for name in ("search", "comments", "processing"))
THREADS_PROFILES = f"{REDIS_KEY_PREFIX}threads:profile_queue"
