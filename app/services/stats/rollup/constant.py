"""Hằng số lịch của các bảng tổng hợp theo ngày (app/services/stats/rollup)."""

# Bảng bình luận: lượt thường tính hôm nay (+ hôm qua mỗi giờ), lượt đêm lúc NIGHTLY_HOUR giờ VN tính NIGHTLY_DAYS ngày.
RECENT_DAYS = 2
NIGHTLY_DAYS = 60
NIGHTLY_HOUR = 3
# Nghỉ giữa hai ngày tính liền nhau để ingest và trang web không phải xếp hàng chờ D1.
PAUSE_SECONDS = 2.0
# Bảng bài: số ngày tối đa mỗi nhịp 10 phút (~864 lượt/ngày, đủ cho nhịp tính lại ~330 lượt/ngày của post_rollup_core).
POST_DAYS_PER_TICK = 6
