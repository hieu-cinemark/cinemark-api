from app.services.omar_tiktok import map_aweme, map_web_item

WEB = {
    "id": "7692425793506528530",
    "desc": "Phải hên lắm #TraiBuonNguoi",
    "createTime": 1791032456,
    "author": {"id": "1", "uniqueId": "choi.entertainment.com", "nickname": "Trại Buôn Người Movie", "verified": True},
    "statsV2": {"playCount": "2200000", "diggCount": "73300", "commentCount": "669", "shareCount": "14300"},
    "contents": [{"textExtra": [{"hashtagName": "traibuonnguoi"}]}],
    "imagePost": {"images": [{}]},
    "video": {"duration": 0},
}


def test_map_web_item_photo():
    item = map_web_item(WEB)
    assert item["url"] == "https://www.tiktok.com/@choi.entertainment.com/photo/7692425793506528530"
    assert item["play_count"] == 2_200_000 and item["like_count"] == 73_300
    assert item["hashtags"] == ["traibuonnguoi"]
    assert item["author_verified"] is True and item["source"] == "omar_search"
    assert item["duration"] is None


def test_map_web_item_video_and_missing_id():
    video = {**WEB, "imagePost": None}
    assert "/video/" in map_web_item(video)["url"]
    assert map_web_item({"desc": "x"}) is None


def test_map_aweme_photo_url():
    aweme = {"aweme_id": "9", "author": {"unique_id": "abc"}, "image_post_info": {"images": []}}
    assert map_aweme(aweme)["url"] == "https://www.tiktok.com/@abc/photo/9"
    assert map_aweme({**aweme, "image_post_info": None})["url"] == "https://www.tiktok.com/@abc/video/9"


def test_channel_match_movie():
    from app.services.omar_channels import match_movie

    movies = [
        {"keyword_id": "kw_tbn", "title_key": "traibuonnguoi", "tags": {"traibuonnguoi", "phimtraibuonnguoi"}},
        {"keyword_id": "kw_qat", "title_key": "quyantang4hotinh", "tags": {"quyantang4"}},
    ]
    assert match_movie({"hashtags": ["TraiBuonNguoi", "fyp"], "desc": "x"}, movies) == "kw_tbn"
    assert match_movie({"hashtags": [], "desc": "Review nhanh phim Quỷ Ăn Tạng 4: Hổ Tinh nha"}, movies) == "kw_qat"
    assert match_movie({"hashtags": ["fyp"], "desc": "đi ăn lẩu"}, movies) is None
