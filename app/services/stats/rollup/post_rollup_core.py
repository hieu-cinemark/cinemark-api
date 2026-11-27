"""Phần tính thuần của bảng tổng hợp bài theo phim x nền tảng x ngày đăng (movie_post_daily), không chạm DB.

Chuyển nguyên từ Worker (cinemark apps/api/src/services/rollup/postRollupCore.ts, 27/11/2026) để gom việc tính bảng tổng hợp về
một chỗ (bộ lập lịch cinemark-api, cạnh bảng tổng hợp bình luận). Phải ra ĐÚNG cùng số với bản TS trong lúc hai bên chạy song song
(scripts/jobs/compare_post_rollup.py) - nên giữ cả các chi tiết như mốc thời gian theo mili giây, `num` (Number(x) || 0).

- seen_age = -1 (CURRENT_AGE): số hiện tại của bài.
- seen_age = N >= 0: số của bài như hệ thống thấy lúc hết ngày đăng + N ngày (bản chụp gần nhất trước mốc; bài không có bản chụp
  thì dùng số hiện tại nếu đã quét trước mốc; chưa quét trước mốc = lúc đó chưa thấy bài). Social Buzz dùng để so kỳ này với kỳ
  trước ở cùng độ tuổi dữ liệu; Media & WOM Tracker dùng để lấy số "tại thời điểm" một mốc đo.
- Phần đọc bảng (seenAgeRuns, blendSeenRows) vẫn ở Worker, không chuyển.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any

from app.services.ai_jobs.channels import extract_handle
from app.services.stats.rollup.helper import DAY_MS, HOUR_MS, MINUTE_MS, day_label, day_start, parse_ms

CURRENT_AGE = -1
SEEN_AGES = (0, 1, 2, 3, 4, 5, 6, 7, 10, 14, 21, 30, 45, 60, 90, 180, 365)
BACKFILL_DAYS = 730

# Hạng tác giả tính là bài "tự nhiên" (organic); tác giả chưa có hạng coi như 'outsider'.
SPONTANEOUS_TIERS = frozenset({"fan", "outsider"})


def seen_cutoff(day: str, age: int) -> int:
    return day_start(day) + (age + 1) * DAY_MS


def num(value: Any) -> int | float:
    """Number(value) || 0 của JS: None, chuỗi rỗng/không phải số và NaN đều thành 0."""
    if value is None or isinstance(value, bool):
        return int(value or 0)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0
    if math.isnan(number) or number == 0:
        return 0
    return int(number) if number.is_integer() else number


_YOUTUBE_HANDLE = re.compile(r"youtube\.com/@([^/?#]+)")


def rollup_handle(platform: str, author: str | None, url: str | None) -> str:
    """Handle tra hạng tác giả - như extractHandle của Worker. channels.extract_handle chưa có nhánh YouTube (handle '@ten' bỏ
    dấu @) nên thêm ở đây cho khớp."""
    if platform == "youtube":
        from_author = re.sub(r"^@+", "", (author or "").lower())
        if from_author:
            return from_author
        match = _YOUTUBE_HANDLE.search(url or "")
        return match.group(1).lower() if match else ""
    return extract_handle(platform, author, url)


def is_organic(handle: str, tiers: dict[str, str]) -> bool:
    return not handle or tiers.get(handle, "outsider") in SPONTANEOUS_TIERS


@dataclass
class RollupRow:
    movie_id: str
    platform: str
    organic: int
    seen_age: int
    posts: int = 0
    discussions: int = 0
    shares: int = 0
    views: int = 0


@dataclass
class _Acc:
    rows: dict[tuple[str, str, int, int], RollupRow] = field(default_factory=dict)

    def add(self, post: dict[str, Any], age: int, replies: Any, shares: Any, views: Any) -> None:
        organic = 1 if post["organic"] else 0
        key = (post["movie_id"], post["platform"], organic, age)
        row = self.rows.get(key)
        if row is None:
            row = RollupRow(post["movie_id"], post["platform"], organic, age)
            self.rows[key] = row
        row.posts += 1
        row.discussions += 1 + num(replies)
        row.shares += num(shares)
        row.views += num(views)


def build_rollup_rows(day: str, posts: list[dict[str, Any]], snapshots: list[dict[str, Any]], now: int) -> list[RollupRow]:
    """Các dòng tổng hợp của một ngày đăng (buildRollupRows của Worker).

    posts: {id, movie_id, platform, scraped_at, replies, shares, views, organic}; snapshots: {post_id, recorded_at, replies, shares,
    views}; now: mili giây."""
    by_post: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for snapshot in snapshots:
        at = parse_ms(snapshot.get("recorded_at"))
        if at is None:
            continue
        by_post.setdefault(snapshot["post_id"], []).append((at, snapshot))
    for items in by_post.values():
        items.sort(key=lambda item: item[0])

    acc = _Acc()
    ages = [age for age in SEEN_AGES if seen_cutoff(day, age) <= now]
    for post in posts:
        acc.add(post, CURRENT_AGE, post.get("replies"), post.get("shares"), post.get("views"))
        items = by_post.get(post["id"])
        scraped_at = parse_ms(post.get("scraped_at"))
        for age in ages:
            cutoff = seen_cutoff(day, age)
            if not items:
                if scraped_at is not None and scraped_at < cutoff:
                    acc.add(post, age, post.get("replies"), post.get("shares"), post.get("views"))
                continue
            latest = None
            for at, snapshot in items:
                if at >= cutoff:
                    break
                latest = snapshot
            if latest is not None:
                acc.add(post, age, latest.get("replies"), latest.get("shares"), latest.get("views"))
    return list(acc.rows.values())


def refresh_every(age: int) -> int:
    """Nhịp tính lại một ngày đăng theo tuổi (ngày) của nó."""
    if age <= 1:
        return 30 * MINUTE_MS
    if age <= 3:
        return HOUR_MS
    if age <= 14:
        return 6 * HOUR_MS
    if age <= 60:
        return 24 * HOUR_MS
    return 7 * 24 * HOUR_MS


def next_rollup_day(computed: dict[str, int], now: int) -> tuple[str, bool] | None:
    """(ngày cần tính tiếp, còn ngày chưa dựng?): ngày chưa dựng trước (mới nhất trước), sau đó ngày quá hạn lâu nhất so với nhịp
    của nó. None khi mọi ngày đều còn mới."""
    today = day_start(day_label(now))
    due: tuple[str, int] | None = None
    for age in range(BACKFILL_DAYS):
        day = day_label(today - age * DAY_MS)
        at = computed.get(day)
        if at is None:
            return day, True
        late = now - at - refresh_every(age)
        if late >= 0 and (due is None or late > due[1]):
            due = (day, late)
    return (due[0], False) if due else None
