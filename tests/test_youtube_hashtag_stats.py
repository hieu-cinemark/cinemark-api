import json

import httpx
import pytest

from app.clients import youtube
from app.services import youtube_hashtag_stats


def _page(text: str | None) -> str:
    header = {}
    if text is not None:
        header = {
            "pageHeaderRenderer": {
                "content": {
                    "pageHeaderViewModel": {
                        "metadata": {
                            "contentMetadataViewModel": {
                                "metadataRows": [{"metadataParts": [{"text": {"content": text}}]}]
                            }
                        }
                    }
                }
            }
        }
    return f"<script>var ytInitialData = {json.dumps({'header': header})};</script>"


@pytest.mark.parametrize(
    ("text", "videos", "channels"),
    [
        ("464 videos • 124 channels", 464, 124),
        ("1,234 videos • 1 channel", 1234, 1),
        ("8.5M videos • 1.3M channels", 8_500_000, 1_300_000),
        ("12K videos", 12_000, 0),
        ("1 video • 1 channel", 1, 1),
    ],
)
def test_parse_counts_reads_the_header_line(text, videos, channels):
    assert youtube.parse_counts(_page(text)) == {"video_count": videos, "channel_count": channels}


def test_parse_counts_returns_none_without_a_count_line():
    assert youtube.parse_counts(_page(None)) is None
    # Hashtag nhỏ: YouTube không cho số thật, không được đọc thành 100.
    assert youtube.parse_counts(_page("< 100 videos and channels")) is None
    assert youtube.parse_counts("<html>no data</html>") is None


async def test_snapshot_saves_counts_and_skips_missing_or_failed(monkeypatch):
    keywords = [
        {"id": "k1", "movie_id": "m1", "keyword": "#BoSuaBay"},
        {"id": "k2", "movie_id": "m1", "keyword": "#khongco"},
        {"id": "k3", "movie_id": "m1", "keyword": "#loi"},
        {"id": "k4", "movie_id": "m1", "keyword": "câu tìm thường"},
    ]
    saved = []

    async def enabled(platform):
        assert platform == "tiktok"
        return keywords

    async def counts(client, name):
        if name == "loi":
            raise httpx.ConnectError("down")
        return {"video_count": 521, "channel_count": 158} if name == "BoSuaBay" else None

    async def upsert(**row):
        saved.append(row)

    monkeypatch.setattr(youtube_hashtag_stats, "get_enabled_keywords", enabled)
    monkeypatch.setattr(youtube_hashtag_stats.youtube, "hashtag_counts", counts)
    monkeypatch.setattr(youtube_hashtag_stats.youtube_hashtag_stats, "upsert_snapshot", upsert)
    stats = await youtube_hashtag_stats.snapshot_all(pause_seconds=0)
    assert stats == {"hashtags": 3, "saved": 1, "missing": 1, "failed": 1}
    assert saved[0]["hashtag"] == "BoSuaBay" and saved[0]["video_count"] == 521 and saved[0]["channel_count"] == 158
