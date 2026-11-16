import asyncio
from datetime import UTC, datetime

from app.ai.tasks.review_angles import parse_batch, verbatim_quote
from app.services.ai_jobs import review_angles as ra

NOW = datetime(2026, 11, 16, 8, 0, tzinfo=UTC)
START = "2026-10-22T17:00:00+00:00"  # Premiere 23/10 giờ VN


def _post(i, at):
    return {"id": f"p{i}", "platform": "tiktok", "posted_at": at}


# --- thứ tự đọc ---


def test_old_single_cursor_state_gets_gap_and_new_window():
    state = {**ra.empty_state(), "cursor": "2026-10-25T02:33:00+00:00", "recent": {"old": "2026-10-25T02:33:00+00:00"}}
    ra.init_marks(state, START, NOW)
    assert state["cursor"] == "2026-10-25T02:33:00+00:00"
    assert state["head"] == state["tail"] == "2026-11-15T08:00:00+00:00"  # 24 giờ gần nhất là "bài mới"


def test_fresh_state_reads_from_premiere():
    state = ra.empty_state()
    ra.init_marks(state, "2026-11-16T05:00:00+00:00", NOW)  # Premiere hôm nay, sau mốc 24 giờ
    assert state["head"] == "2026-11-16T05:00:00+00:00"
    assert state["cursor"] == "2026-11-16T04:59:59+00:00"


class _Scan:
    """Giả d1_exec: trả bài theo câu SQL (mới / về muộn / đọc lùi) từ danh sách bài có sẵn."""

    def __init__(self, posts):
        self.posts = posts

    async def __call__(self, stmt, **kwargs):
        params = stmt.compile().params
        values = [v for k, v in params.items() if k.startswith("posted_at")]
        sql = str(stmt)
        rows = list(self.posts)
        if "posted_at DESC" in sql:  # đọc lùi: cursor < posted_at <= tail
            low, high = sorted(values)
            return sorted((p for p in rows if low < p["posted_at"] <= high), key=lambda p: p["posted_at"], reverse=True)
        if len(values) == 1:  # bài mới: posted_at >= head
            return sorted((p for p in rows if p["posted_at"] >= values[0]), key=lambda p: p["posted_at"])
        low, high = sorted(values)  # về muộn: late_from <= posted_at < head
        return sorted((p for p in rows if low <= p["posted_at"] < high), key=lambda p: p["posted_at"])


def test_new_posts_first_then_gap_backwards_until_closed(monkeypatch):
    gap_posts = [_post(i, f"2026-11-0{i}T10:00:00+00:00") for i in range(1, 4)]
    new_posts = [_post(9, "2026-11-16T07:00:00+00:00")]
    monkeypatch.setattr(ra, "d1_exec", _Scan(gap_posts + new_posts))
    monkeypatch.setattr(ra, "BATCH_SIZE", 2)
    state = {**ra.empty_state(), "cursor": "2026-11-01T00:00:00+00:00", "lateScanAt": 10**15}
    ra.init_marks(state, START, NOW)

    metas, kind = asyncio.run(ra._next_batch("m", START, state))
    assert (kind, [m["id"] for m in metas]) == ("new", ["p9"])
    ra.mark_read(state, metas, kind)

    metas, kind = asyncio.run(ra._next_batch("m", START, state))
    assert (kind, [m["id"] for m in metas]) == ("gap", ["p3", "p2"])  # gần đây trước
    ra.mark_read(state, metas, kind)
    assert state["tail"] == "2026-11-02T10:00:00+00:00"

    metas, kind = asyncio.run(ra._next_batch("m", START, state))
    assert [m["id"] for m in metas] == ["p1"]
    ra.mark_read(state, metas, kind)

    metas, kind = asyncio.run(ra._next_batch("m", START, state))
    assert metas == []
    assert state["tail"] == state["cursor"]  # khoảng thiếu đã đọc hết


def test_youtube_caption_adds_transcript():
    text = ra.youtube_caption("Review Con Mắt Thứ Ba\n\n" + "mô tả " * 500, "Đạo diễn Hàm Trần gọi các hồn ma là bạn")
    assert text.startswith("Review Con Mắt Thứ Ba")
    assert text.endswith("Transcript: Đạo diễn Hàm Trần gọi các hồn ma là bạn")
    assert len(text) < ra.YOUTUBE_TEXT_CHARS + 100
    assert ra.youtube_caption("Tiêu đề", None) == "Tiêu đề"


# --- câu trả lời AI ---

POSTS = [
    {"id": "a", "caption": "Phim  hay,\ndiễn xuất của “Thu Trang” rất tự nhiên và cảm động"},
    {"id": "b", "caption": "Kịch bản đoạn cuối hơi vội, nhiều chi tiết chưa được giải thích"},
]


def test_quote_whitespace_and_curly_quotes_are_matched_to_caption():
    assert verbatim_quote('diễn xuất của "Thu Trang" rất tự nhiên', POSTS[0]["caption"]) == (
        "diễn xuất của “Thu Trang” rất tự nhiên"
    )
    assert verbatim_quote("Phim hay, diễn xuất", POSTS[0]["caption"]) == "Phim  hay,\ndiễn xuất"
    assert verbatim_quote("câu không có trong bài", POSTS[0]["caption"]) is None


def test_bad_parts_dropped_instead_of_whole_batch():
    raw = {
        "summary": "Khen diễn xuất, chê kịch bản.",
        "angles": [
            {
                "name": "Diễn xuất tự nhiên",
                "description": "Khen diễn xuất",
                "aspect": "dien_xuat",
                "evidence": [
                    {"postId": "a", "quote": "diễn xuất của “Thu Trang” rất tự nhiên"},
                    {"postId": "zzz", "quote": "bài không có trong lô nhé"},
                ],
            },
            {"name": "Sai khía cạnh", "description": "x", "aspect": "khong_co", "evidence": []},
            {
                "id": "id-bia",
                "name": "Kết vội",
                "description": "",
                "aspect": "kich_ban",
                "evidence": [
                    {"postId": "b", "quote": "đoạn cuối hơi vội"},
                    {"postId": "b", "quote": "bịa ra đoạn này"},
                ],
            },
        ],
    }
    parsed = parse_batch(raw, POSTS, [])
    assert [a["name"] for a in parsed["angles"]] == ["Diễn xuất tự nhiên", "Kết vội"]
    assert [len(a["evidence"]) for a in parsed["angles"]] == [1, 1]
    assert parsed["angles"][1]["id"] is None  # id bịa -> góc mới
    assert parsed["angles"][1]["description"] == "Kết vội"
    assert parse_batch("không phải json", POSTS, []) is None
    assert parse_batch({"angles": []}, POSTS, []) == {"summary": "", "angles": []}


def test_post_keeps_at_most_two_angles():
    def angle(n):
        return {
            "name": f"Góc {n}",
            "description": "d",
            "aspect": "tong_the",
            "evidence": [{"postId": "b", "quote": "Kịch bản đoạn cuối hơi vội"}],
        }

    parsed = parse_batch({"summary": "s", "angles": [angle(1), angle(2), angle(3)]}, POSTS, [])
    assert [a["name"] for a in parsed["angles"]] == ["Góc 1", "Góc 2"]
