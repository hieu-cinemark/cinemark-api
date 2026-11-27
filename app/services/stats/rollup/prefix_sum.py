"""Tổng của một khoảng ngày bất kỳ trong O(1) bằng prefix sum, trên chuỗi theo ngày của một phim.

Cách dùng (27/11/2026): đọc MỘT lần các dòng theo ngày của phim từ bảng tổng hợp (movie_post_daily, movie_comment_daily) - cache
theo phim, không theo khoảng ngày - rồi mọi khoảng ngày người dùng chọn chỉ là P[đến] - P[từ - 1]. Đổi bộ lọc ngày bao nhiêu lần
cũng không đọc lại D1.

Chỉ dùng cho chỉ số CỘNG DỒN ĐƯỢC theo ngày (số bài, bình luận, lượt xem, số theo cảm xúc). Không dùng cho số tác giả khác nhau,
top bài, trung vị - cộng theo ngày sẽ sai.

Prefix sum chỉ nằm trong bộ nhớ, không lưu D1: bài cào trễ / số liệu cập nhật muộn chỉ sửa đúng dòng của ngày đó ở bảng theo ngày.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from app.services.stats.rollup.helper import DAY_MS, day_label, day_start


@dataclass(frozen=True)
class DailySeries:
    """Các chỉ số theo ngày VN liên tục từ `first_day` (ngày thiếu = 0), kèm prefix sum: prefix[m][i] = tổng i ngày đầu."""

    first_day: str
    days: int
    prefix: Mapping[str, tuple[int, ...]]

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, object]], metrics: Iterable[str]) -> DailySeries:
        """rows: mỗi dòng có `day` (YYYY-MM-DD) và các cột trong `metrics`; nhiều dòng cùng ngày được cộng lại."""
        metrics = tuple(metrics)
        by_day: dict[str, dict[str, int]] = {}
        for row in rows:
            day = str(row["day"])
            bucket = by_day.setdefault(day, dict.fromkeys(metrics, 0))
            for metric in metrics:
                bucket[metric] += int(row.get(metric) or 0)
        if not by_day:
            return cls(first_day="", days=0, prefix={metric: (0,) for metric in metrics})
        first, last = min(by_day), max(by_day)
        start = day_start(first)
        days = (day_start(last) - start) // DAY_MS + 1
        empty: dict[str, int] = {}
        buckets = [by_day.get(day_label(start + i * DAY_MS), empty) for i in range(days)]
        prefix: dict[str, tuple[int, ...]] = {}
        for metric in metrics:
            running = [0]
            for bucket in buckets:
                running.append(running[-1] + bucket.get(metric, 0))
            prefix[metric] = tuple(running)
        return cls(first_day=first, days=days, prefix=prefix)

    def _index(self, day: str) -> int:
        """Chỉ số của `day` trong chuỗi (có thể âm hoặc vượt cuối - được kẹp ở range_sum)."""
        return (day_start(day) - day_start(self.first_day)) // DAY_MS

    def range_sum(self, metric: str, from_day: str, to_day: str) -> int:
        """Tổng `metric` từ `from_day` tới `to_day` (tính cả hai đầu). Phần nằm ngoài chuỗi coi là 0."""
        if not self.days or from_day > to_day:
            return 0
        values = self.prefix[metric]
        lo = min(max(self._index(from_day), 0), self.days)
        hi = min(max(self._index(to_day) + 1, 0), self.days)
        return values[hi] - values[lo] if hi > lo else 0

    def totals(self, from_day: str, to_day: str) -> dict[str, int]:
        return {metric: self.range_sum(metric, from_day, to_day) for metric in self.prefix}
