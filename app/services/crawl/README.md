# app/services/crawl

Điều phối việc cào.

- `scheduler.py`: vòng lặp lịch chạy trong `app.workers.scheduler` (lượt cào theo giờ, bình luận, rollup, job AI...).
- `platforms.py`: danh sách nền tảng và cách chuyển dữ liệu spider-hub sang bảng D1.
- `crawl_lanes.py`, `keyword_yield.py`: hạn mức và hiệu quả từ khoá Facebook / Threads.
- `crawl_jobs.py`, `task_queue.py`, `refresh_tracker.py`: trạng thái job và hàng đợi hiển thị trên dashboard.
- `comment_planner.py`, `comment_value.py`: chọn bài để cào bình luận.
- `source_pages.py`, `watch_pages.py`, `priority_channels.py`, `channel_profiles.py`, `tracked_posts.py`: cào theo trang
  nguồn, trang phim, kênh ưu tiên, follower, bài booking.
