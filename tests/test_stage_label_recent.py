import argparse
import asyncio
import json
from typing import Any

from scripts.jobs import stage_label as sl


class _FakeModel:
    def __init__(self, batch_size: int = 32) -> None:
        self.batch_size = batch_size

    def predict(self, texts: list[str]) -> list[str]:
        return ["da_xem" if "xem rồi" in t else "hong" if "hóng" in t else "khac" for t in texts]


def test_recent_mode_labels_new_comments_once(monkeypatch):
    pages = [
        [
            # Bình luận "đã xem" sau ngày chiếu sớm: giữ da_xem.
            {
                "id": "c1",
                "message": "xem rồi, phim hay",
                "posted_at": "2026-11-10T05:00:00+00:00",
                "scraped_at": "s3",
                "movie_id": "m1",
            },
            # "đã xem" trước ngày chiếu sớm: không thể là đã xem phim này -> khac.
            {
                "id": "c2",
                "message": "xem rồi trailer nè",
                "posted_at": "2026-11-01T05:00:00+00:00",
                "scraped_at": "s2",
                "movie_id": "m1",
            },
            {
                "id": "c3",
                "message": "hóng quá",
                "posted_at": "2026-11-10T05:00:00+00:00",
                "scraped_at": "s2",
                "movie_id": "m1",
            },
            # Chỉ tag bạn bè: giữ nhãn hiện có (hong do luật), chỉ đánh dấu đã xét.
            {
                "id": "c4",
                "message": "@Lan Anh",
                "posted_at": "2026-11-10T05:00:00+00:00",
                "scraped_at": "s1",
                "movie_id": "m1",
            },
        ],
        [],
    ]
    saved: list[list[dict[str, Any]]] = []
    queries: list[str] = []

    async def fake_exec(stmt, **_k):
        queries.append(str(stmt))
        return pages.pop(0)

    async def fake_query(sql, params=None, **_k):
        if sql.startswith("PRAGMA"):
            return [{"name": "id"}, {"name": "stage_source"}]
        saved.append(json.loads(params[0]))
        return []

    async def starts():
        return {"m1": "2026-11-05T17:00:00+00:00"}

    monkeypatch.setattr(sl, "StageModel", _FakeModel)
    monkeypatch.setattr(sl, "d1_exec", fake_exec)
    monkeypatch.setattr(sl, "d1_query", fake_query)
    monkeypatch.setattr(sl, "_premiere_starts", starts)

    args = argparse.Namespace(days=0.25, dry_run=False, batch_size=8, page_size=500, movie_id=None)
    asyncio.run(sl.run(args))

    assert len(saved) == 1
    by_id = {item["id"]: item for item in saved[0]}
    assert by_id["c1"] == {"id": "c1", "s": "da_xem", "src": "model"}
    assert by_id["c2"]["s"] == "khac"
    assert by_id["c3"]["s"] == "hong"
    assert by_id["c4"] == {"id": "c4", "s": None, "src": "rule"}
    # Chỉ chọn dòng chưa qua model, của bài liên quan, trong cửa sổ thời gian.
    assert "stage_source IS NULL" in queries[0]
    assert "scraped_at >=" in queries[0]
