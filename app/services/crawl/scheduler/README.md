# app/services/crawl/scheduler

Chạy trong tiến trình `cinemark-scheduler` (`python -m app.workers.scheduler.main`): NGUỒN DUY NHẤT quyết định khi nào cào và khi nào
chạy job nền. Vòng lặp thức dậy mỗi `POLL_INTERVAL_SECONDS` và chạy lần lượt mọi nhịp trong `TICKS` (`__init__.py`). Mỗi nhịp có timeout;
lỗi hay treo của một nhịp chỉ được log, không chặn nhịp sau. Việc nặng chạy nền bằng `core.spawn`, không await trong nhịp.

## Các file

| File | Việc nó làm |
|---|---|
| `ai_jobs.py` | Job AI nền (app/services/ai_jobs): gắn nhãn tác giả mỗi 2 phút, niche kênh mỗi giờ, Review Angles mỗi ~3 phút (mỗi phút khi đang ưu tiên một phim). |
| `attack.py` | Nhịp của Attack Detection (docs/review-angles-va-attack.md mục 2 và 4). |
| `cleanup.py` | Dọn bài không liên quan mỗi ngày (app/services/relevance/cleanup.py) + nút "chạy ngay" trên dashboard. |
| `comments.py` | Lượt cào bình luận mỗi giờ (bảng `comment_crawl_schedules`, app/services/crawl/comment_planner.py). |
| `core.py` | Phần dùng chung của các nhịp lịch: giờ Việt Nam, kiểm tra tới giờ, chạy nền và khoá chống chạy lặp. |
| `crawl_slots.py` | Lượt cào bài theo lịch (bảng `crawl_schedules`, sửa trên dashboard): mỗi nền tảng tối đa vài giờ chạy mỗi ngày. |
| `discovery.py` | AI tự thêm nguồn cào mỗi ngày một lần ở settings.hashtag_discovery_time: hashtag TikTok, câu tìm Facebook/Threads, trang nguồn gợi ý (app/services/discovery/*, app/services/crawl/source_pages.py). |
| `ops.py` | Việc vận hành: cảnh báo Telegram, sức khoẻ tài khoản, đối soát thống kê, report theo lịch, đồng bộ cấu hình AI sang D1. |
| `refresh.py` | Làm mới định kỳ bài và kênh: bài hưởng ứng trend, page phim đang chiếu, bài booking đang theo dõi, follower kênh. |
| `omar.py` | Omar API: lượt hashtag TikTok chạy liên tục khi key đúng (app/services/omar/omar_tiktok.py) + xếp hàng bình luận hằng ngày |

## Luồng

Một nhịp là hàm `async def <tên>_tick() -> None`. Nó tự quyết định "tới giờ chưa" bằng một trong ba cách:

| Cách | Dùng khi | Ví dụ |
|---|---|---|
| `core.is_due("HH:MM", now)` + cờ Redis theo ngày | Việc chạy một lần mỗi ngày | `discovery.hashtag_tick` |
| `await core.claim(tên, giây)` (khoá Redis có hạn) | Việc chạy mỗi N giây, và chỉ một tiến trình chạy | `attack.comment_hourly_tick`, `ops.*` |
| Mốc thời gian trong bộ nhớ (`_last_run`) | Job AI nhiều chu kỳ trong một file | `ai_jobs.ai_jobs_tick` |

Danh sách nhịp (thứ tự = thứ tự chạy mỗi vòng): `crawl`, `cleanup`, `comments`, `public_pages`, `threads_channels`, `youtube_continuous`,
`hashtag`, `omar`, `trend_posts`, `watch_pages`, `channel_profiles`, `comment_completions`, `ops_alerts`,
`health_checks`, `tracked_posts`, `ai_jobs`, `ai_runtime`, `report_schedule`, `news`, `stats_reconcile`, `deep_crawl`, `comment_hourly`,
`attack_detect`. Ba nhịp `public_pages`, `threads_channels`, `youtube_continuous` có phần việc nằm ngoài thư mục này (`app/services/crawl/`).

## Muốn sửa / thêm thì

1. Viết `async def <tên>_tick() -> None` trong file hợp nhóm (hoặc file mới như `attack.py`).
2. Thêm một dòng vào `TICKS` trong `__init__.py`: `("tên", module.<tên>_tick, báo_telegram_khi_timeout)`. Vị trí trong danh sách = thứ tự chạy.
3. Việc > vài giây: bọc trong hàm `job()` rồi `spawn(job())`, và `claim` trước khi spawn để không chồng lượt.
4. Test: monkeypatch `claim` / Redis (xem `tests/test_discovery_toggle.py`, `tests/test_comment_hourly.py`).

## Lưu ý

Nhịp job AI (`ai_jobs.py`) dùng `AI_JOB_EVERY` cho chu kỳ từng job; muốn đổi tần suất một job AI thì sửa dict đó, không sửa `TICKS`.
