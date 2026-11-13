"""YouTube (13/11/2026): mapper bài/bình luận và đăng ký nền tảng."""

from app.clients.kafka import CONSUMER_GROUPS
from app.services.crawl import platforms
from app.services.crawl.task_queue import PLATFORMS


def test_youtube_post_mapper():
    draft = platforms.get_post_mapper("youtube")(
        {
            "video_id": "XuHpS4skf7o",
            "url": "https://www.youtube.com/shorts/XuHpS4skf7o",
            "title": "Mưa Đỏ - hòa bình đẹp lắm",
            "description": "Khâm phục",
            "channel_name": "Thai SiamPot",
            "channel_handle": "@reviewstar7287",
            "view_count": 1093689,
            "like_count": 8872,
            "duration_seconds": 22,
            "create_time": 1756650119,
            "thumbnail": "https://i.ytimg.com/vi/x/hq.jpg",
            "is_short": True,
        }
    )
    assert draft["external_id"] == "XuHpS4skf7o"
    assert draft["author"] == "@reviewstar7287"
    assert draft["content"] == "Mưa Đỏ - hòa bình đẹp lắm\n\nKhâm phục"
    assert draft["media"]["media_type"] == "short"
    assert (draft["view_count"], draft["like_count"]) == (1093689, 8872)
    assert draft["posted_at"].startswith("2025-08-31")


def test_youtube_comment_mapper_and_registration():
    draft = platforms.get_comment_mapper("youtube")(
        {
            "comment_id": "Ugw1",
            "message": "hay",
            "author_name": "@abc",
            "like_count": 3,
            "parent_comment_id": "Ugw0",
            "timestamp": 1763554612,
        }
    )
    assert draft["author_url"] == "https://www.youtube.com/@abc"
    assert draft["parent_external_id"] == "Ugw0" and draft["reactions_count"] == 3
    assert "youtube" in platforms.registered_platforms() and "youtube" in platforms.COMMENT_CRAWL_PLATFORMS
    assert "youtube" in PLATFORMS
    assert ("youtube", "crawl_requests", "spider-hub.crawl-requests.youtube") in CONSUMER_GROUPS
