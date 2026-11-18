"""Ánh xạ dữ liệu Omar (hashtag_videos, comments, comment_replies, hashtag_info) sang tên trường của spider-hub."""

from app.clients.omar import hashtag_info, map_comment, map_video, next_cursor, videos_of

# Video dạng gốc của TikTok (aweme) như hashtag_videos trả về.
AWEME = {
    "aweme_id": "7301234567890123456",
    "desc": "Review phim mới #traibuonnguoi #phimhay",
    "desc_language": "vi",
    "region": "VN",
    "create_time": 1731715200,
    "share_url": "https://www.tiktok.com/@abc/video/7301234567890123456",
    "author": {
        "uid": "u1",
        "unique_id": "abc",
        "nickname": "ABC",
        "avatar_thumb": {"url_list": ["https://a/abc.jpg"]},
        "custom_verify": "Official",
        "follower_count": 1500,
        "signature": "bio",
    },
    "statistics": {"play_count": 1200, "digg_count": 300, "comment_count": 25, "share_count": 7, "collect_count": 9},
    "video": {
        "duration": 42000,
        "origin_cover": {"url_list": ["https://c/origin.jpg"]},
        "play_addr": {"url_list": ["https://v/play.mp4"]},
    },
    "music": {"title": "nhạc nền"},
    "text_extra": [{"hashtag_name": "traibuonnguoi"}, {"hashtag_name": "phimhay"}, {"type": 0}],
}


def test_map_video_fields():
    item = map_video(AWEME)
    assert item["video_id"] == "7301234567890123456"
    assert item["url"] == "https://www.tiktok.com/@abc/video/7301234567890123456"
    assert item["desc"].startswith("Review phim")
    assert item["hashtags"] == ["traibuonnguoi", "phimhay"]
    assert (item["play_count"], item["like_count"], item["comment_count"]) == (1200, 300, 25)
    assert (item["author_id"], item["author_username"], item["author_name"]) == ("u1", "abc", "ABC")
    assert item["author_avatar_url"] == "https://a/abc.jpg"
    assert item["cover_url"] == "https://c/origin.jpg"
    assert item["duration"] == 42
    assert item["music_title"] == "nhạc nền"
    assert item["source"] == "omar"
    # Response gốc có tín hiệu tác giả.
    assert item["author_verified"] is True and item["author_followers"] == 1500


def test_map_video_photo_post_uses_photo_link():
    item = map_video({**AWEME, "image_post_info": {"images": []}})
    assert item["url"] == "https://www.tiktok.com/@abc/photo/7301234567890123456"


def test_map_video_branded_and_ad_flags():
    item = map_video({**AWEME, "is_ads": True, "commerce_info": {"branded_content_type": 1}})
    assert item["is_ad"] is True and item["is_branded"] is True


def test_map_video_missing_id():
    assert map_video({"desc": "x"}) is None
    assert map_video({"aweme_id": "  ", "author": {"unique_id": "a"}}) is None


def test_map_comment_and_reply():
    raw = {
        "cid": "c1",
        "text": "phim hay",
        "create_time": 1731715300,
        "digg_count": 4,
        "reply_comment_total": 2,
        "user": {"uid": "u9", "unique_id": "xyz", "nickname": "XYZ", "avatar_thumb": {"url_list": ["https://a/x.jpg"]}},
    }
    comment = map_comment(raw)
    assert comment["comment_id"] == "c1" and comment["message"] == "phim hay"
    assert (comment["like_count"], comment["reply_count"]) == (4, 2)
    assert comment["author_username"] == "xyz" and comment["author_avatar_url"] == "https://a/x.jpg"
    assert comment["parent_comment_id"] is None
    # Trả lời: cha lấy từ chỗ gọi, hoặc từ reply_id khi không truyền.
    assert map_comment(raw, parent_comment_id="p1")["parent_comment_id"] == "p1"
    assert map_comment({**raw, "reply_id": "p7"})["parent_comment_id"] == "p7"
    assert map_comment({**raw, "reply_id": "0"})["parent_comment_id"] is None
    assert map_comment({"text": "không có cid"}) is None


def test_next_cursor_needs_has_more_and_cursor():
    assert next_cursor({"has_more": "true", "next_cursor": "30"}) == "30"
    assert next_cursor({"has_more": True, "next_cursor": 30}) == "30"
    assert next_cursor({"has_more": "false", "next_cursor": "30"}) is None
    assert next_cursor({"has_more": "true"}) is None
    assert next_cursor(None) is None


def test_videos_of_ignores_junk():
    assert videos_of({"videos": [{"aweme_id": "1"}, "rác", None]}) == [{"aweme_id": "1"}]
    assert videos_of(None) == []


def test_hashtag_info_reads_clean_and_raw_shapes():
    assert hashtag_info({"hashtag": {"id": "h1", "video_count": 5355, "view_count": 61_311_496}}) == {
        "hashtag_id": "h1",
        "video_count": 5355,
        "view_count": 61_311_496,
    }
    # Dạng gốc của app: số video nằm ở ch_info.user_count.
    nested = hashtag_info({"hashtag": {"ch_info": {"user_count": 900, "view_count": 12}}})
    assert (nested["video_count"], nested["view_count"]) == (900, 12)
    assert hashtag_info(None)["video_count"] == 0
