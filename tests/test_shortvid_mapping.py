from app.clients.shortvid import hashtag_info, map_comment, map_video, next_cursor, videos_of

# Video dạng ShortVid (challenge/posts, video/search, user/posts, video/detail đều trả cùng dạng này).
VIDEO = {
    # Response thật 16/11/2026: aweme_id là mã file video, id video ở video_id.
    "aweme_id": "v0f044gc0000cn0ijh7og65r84nc68u0",
    "video_id": "7301",
    "region": "VN",
    "title": "Review phim mới #traibuonnguoi #phimhay",
    "create_time": 1731715200,
    "duration": 42,
    "cover": "https://c/cover.jpg",
    "origin_cover": "https://c/origin.jpg",
    "play": "https://v/play.mp4",
    "play_count": 1200,
    "digg_count": 300,
    "comment_count": 25,
    "share_count": 7,
    "collect_count": 9,
    "music_info": {"title": "nhạc nền"},
    "is_ad": False,
    "commerce_info": {"branded_content_type": 0},
    "author": {"id": "u1", "unique_id": "abc", "nickname": "ABC", "avatar": "https://a/abc.jpg"},
}


def test_map_video_fields():
    item = map_video(VIDEO)
    assert item["video_id"] == "7301"
    assert item["url"] == "https://www.tiktok.com/@abc/video/7301"
    assert item["desc"].startswith("Review phim")
    assert item["hashtags"] == ["traibuonnguoi", "phimhay"]
    assert (item["play_count"], item["like_count"], item["comment_count"]) == (1200, 300, 25)
    assert (item["author_id"], item["author_username"], item["author_name"]) == ("u1", "abc", "ABC")
    assert item["cover_url"] == "https://c/origin.jpg"
    assert item["music_title"] == "nhạc nền"
    assert item["source"] == "shortvid"
    # ShortVid không trả follower / tick xanh của tác giả.
    assert item["author_followers"] is None and item["author_verified"] is False


def test_map_video_photo_branded_and_hashtag_list():
    photo = {**VIDEO, "content_type": "image", "is_paid_partnership": True, "hashtags": [{"name": "abc"}, "#xyz"]}
    item = map_video(photo, source="shortvid_search")
    assert item["url"] == "https://www.tiktok.com/@abc/photo/7301"
    assert item["is_branded"] is True
    assert item["hashtags"] == ["abc", "xyz"]
    assert item["source"] == "shortvid_search"


def test_map_video_missing_id():
    assert map_video({"title": "x"}) is None
    # aweme_id không phải số thì không dùng làm id.
    assert map_video({"aweme_id": "v0f044gc0000cn0ijh7og65r84nc68u0", "author": {"unique_id": "a"}}) is None
    # video/detail trả id ở "id".
    assert (
        map_video({"id": "7332187682480590112", "aweme_id": "v0f0", "author": {"unique_id": "a"}})["video_id"]
        == "7332187682480590112"
    )
    assert map_video({"video_id": "9", "author": {}})["url"] is None


def test_map_comment_matches_spider_fields():
    raw = {
        "id": "c1",
        "text": "Phim hay",
        "create_time": 1731715300,
        "digg_count": 4,
        "reply_total": 2,
        "user": {"id": "u9", "unique_id": "khach", "nickname": "Khách", "avatar": "https://a/k.jpg"},
    }
    assert map_comment(raw) == {
        "comment_id": "c1",
        "message": "Phim hay",
        "timestamp": 1731715300,
        "like_count": 4,
        "reply_count": 2,
        "author_id": "u9",
        "author_username": "khach",
        "author_name": "Khách",
        "author_avatar_url": "https://a/k.jpg",
        "parent_comment_id": None,
    }
    assert map_comment({**raw}, parent_comment_id="p1")["parent_comment_id"] == "p1"
    assert map_comment({"text": "x"}) is None


def test_pagination_and_lists():
    assert next_cursor({"cursor": 30, "hasMore": True}) == "30"
    assert next_cursor({"cursor": 30, "hasMore": False}) is None
    assert next_cursor({"hasMore": True}) is None
    assert next_cursor(None) is None
    assert videos_of({"videos": [VIDEO, "x"]}) == [VIDEO]
    assert videos_of([VIDEO]) == [VIDEO]  # feed/list trả mảng thẳng trong data
    assert videos_of(None) == []


def test_hashtag_info():
    assert hashtag_info({"id": "42164", "cha_name": "fyp", "user_count": 1000, "view_count": 99}) == {
        "hashtag_id": "42164",
        "video_count": 1000,
        "view_count": 99,
    }
    assert hashtag_info(None) == {"hashtag_id": None, "video_count": 0, "view_count": 0}
