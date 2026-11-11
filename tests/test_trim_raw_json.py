import json

from app.repositories.d1.posts import trim_post_raw
from app.repositories.d1.sql import compile_sql
from scripts.tools import trim_raw_json


def _as_sql(updates) -> list[tuple[str, list]]:
    return [compile_sql(stmt) for stmt in updates]


def test_post_raw_keeps_only_the_keys_that_are_read():
    raw = {"reactions": {"like": 2}, "author_followers": 10, "cover_url": "c", "video": {"big": "x"}, "media_url": None}
    assert trim_post_raw(raw) == {"reactions": {"like": 2}, "author_followers": 10, "cover_url": "c"}


def test_post_raw_with_nothing_useful_is_dropped():
    assert trim_post_raw({"video": 1}) is None
    assert trim_post_raw(None) is None


def test_comment_updates_are_chunked_within_the_d1_parameter_limit():
    rows = [{"id": str(i), "raw_json": "{}"} for i in range(200)]
    updates = _as_sql(trim_raw_json.build_updates("comments", rows))
    # Mỗi câu có thêm một tham số cho giá trị raw_json NULL, nên vẫn dưới trần 100 tham số của D1.
    assert [len(params) for _, params in updates] == [91, 91, 21]
    assert all(len(params) <= 100 for _, params in updates)
    assert all("raw_json=?" in sql for sql, _ in updates)
    assert all(params[0] is None for _, params in updates)


def test_comment_keeps_only_its_attachment():
    raw = {"message": "x", "sticker_url": "https://s", "is_sticker": True, "gif": None, "author_name": "a"}
    rows = [{"id": "s", "raw_json": json.dumps(raw)}, {"id": "t", "raw_json": json.dumps({"message": "y"})}]
    updates = _as_sql(trim_raw_json.build_updates("comments", rows))
    assert ("UPDATE comments SET raw_json=? WHERE comments.id IN (?)", [None, "t"]) in updates
    assert (
        "UPDATE comments SET raw_json=? WHERE comments.id = ?",
        [json.dumps({"sticker_url": "https://s"}), "s"],
    ) in updates


def test_post_update_is_skipped_when_already_trimmed():
    trimmed = json.dumps({"cover_url": "c"})
    rows = [{"id": "a", "raw_json": trimmed}, {"id": "b", "raw_json": json.dumps({"cover_url": "c", "video": 1})}]
    updates = _as_sql(trim_raw_json.build_updates("posts", rows))
    assert updates == [("UPDATE posts SET raw_json=? WHERE posts.id = ?", [trimmed, "b"])]


def test_aspect_job_replaces_ai_labels_only_when_asked():
    from scripts.jobs.aspect_label import pending_sql

    assert pending_sql("c", False) == "+c.aspects IS NULL"
    assert "aspects_source IS NULL" in pending_sql("c", True)
    assert "aspects_source = 'model'" in pending_sql("c", "model")
