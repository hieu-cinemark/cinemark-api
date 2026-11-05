import json

from app.repositories.d1.posts import trim_post_raw
from scripts.tools import trim_raw_json


def test_post_raw_keeps_only_the_keys_that_are_read():
    raw = {"reactions": {"like": 2}, "author_followers": 10, "cover_url": "c", "video": {"big": "x"}, "media_url": None}
    assert trim_post_raw(raw) == {"reactions": {"like": 2}, "author_followers": 10, "cover_url": "c"}


def test_post_raw_with_nothing_useful_is_dropped():
    assert trim_post_raw({"video": 1}) is None
    assert trim_post_raw(None) is None


def test_comment_updates_are_chunked_within_the_d1_parameter_limit():
    rows = [{"id": str(i), "raw_json": "{}"} for i in range(200)]
    updates = trim_raw_json.build_updates("comments", rows)
    assert [len(params) for _, params in updates] == [90, 90, 20]
    assert all("raw_json = NULL" in sql for sql, _ in updates)


def test_post_update_is_skipped_when_already_trimmed():
    trimmed = json.dumps({"cover_url": "c"})
    rows = [{"id": "a", "raw_json": trimmed}, {"id": "b", "raw_json": json.dumps({"cover_url": "c", "video": 1})}]
    updates = trim_raw_json.build_updates("posts", rows)
    assert updates == [("UPDATE posts SET raw_json = ? WHERE id = ?", [trimmed, "b"])]
