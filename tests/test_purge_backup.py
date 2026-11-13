import asyncio

from app.services.relevance import cleanup


def test_purge_backs_up_before_delete_and_keeps_posts_naming_cast(monkeypatch):
    calls: list[str] = []
    uploads: list[str] = []

    async def fake_run(sql, params):
        calls.append(sql)
        if "FROM posts AS p" in sql:  # câu chọn ứng viên
            return [
                {
                    "id": "a",
                    "content": "bảng cửu chương cộng trừ nhân chia",
                    "title": "Cộng Trừ Nhân Chia",
                    "director": "",
                    "cast": "",
                },
                {
                    "id": "b",
                    "content": "Anh Thư đóng vai này hay quá",
                    "title": "Cộng Trừ Nhân Chia",
                    "director": "",
                    "cast": '["Anh Thư"]',
                },
            ]
        if sql.startswith(("SELECT * FROM posts", "SELECT * \nFROM posts")):
            return [{"id": "a", "platform": "tiktok", "first_seen_at": "2026-10-20T01:00:00", "keyword_id": "k1"}]
        if sql.startswith(("SELECT * FROM comments", "SELECT * \nFROM comments")):
            return [{"id": "c1", "post_id": "a", "platform": "tiktok", "scraped_at": "2026-10-21T01:00:00"}]
        if sql.startswith("SELECT"):
            return [{"n": 0}] if "count" in sql else []
        if sql.startswith("DELETE"):
            return [{"x": 1}]
        return []

    async def fake_put(key, body):
        uploads.append(key)

    subtracted = []

    async def fake_subtract(posts, comments):
        subtracted.append((len(posts), len(comments)))

    monkeypatch.setattr(cleanup, "_run", fake_run)
    monkeypatch.setattr(cleanup, "put_object", fake_put)
    monkeypatch.setattr(cleanup, "lake_configured", lambda: True)
    monkeypatch.setattr(cleanup, "subtract_deleted", fake_subtract)

    totals = asyncio.run(cleanup.purge_irrelevant_posts(grace_hours=168))

    assert totals["kept_named"] == 1  # bài nhắc Anh Thư được giữ (chuyển về uncertain)
    assert any(c.startswith("UPDATE") for c in calls)
    assert len(uploads) == len(cleanup._BACKUP_TABLES)  # một file mỗi bảng cho lô duy nhất
    first_delete = next(i for i, c in enumerate(calls) if c.startswith("DELETE"))
    # Sao lưu: một câu SELECT * cho mỗi bảng trong _BACKUP_TABLES, đều trước câu DELETE đầu tiên.
    backups = [i for i, c in enumerate(calls) if c.startswith(("SELECT * FROM", "SELECT * \nFROM"))]
    assert len(backups) == len(cleanup._BACKUP_TABLES)
    assert max(backups) < first_delete  # sao lưu xong mọi bảng của lô rồi mới xoá
    assert subtracted == [(1, 1)]
    assert totals["backup"].startswith("backups/purge/")
    # Danh sách id dài đi qua json_each (một tham số) chứ không nhúng literal vào SQL.
    deletes = [c for c in calls if c.startswith("DELETE")]
    assert all("json_each" in c for c in deletes)
    assert "'" not in "".join(deletes)


def test_purge_refuses_without_backup_storage(monkeypatch):
    async def fake_run(sql, params):
        if "FROM posts AS p" in sql:
            return [{"id": "a", "content": "rác", "title": "X", "director": "", "cast": ""}]
        return []

    monkeypatch.setattr(cleanup, "_run", fake_run)
    monkeypatch.setattr(cleanup, "lake_configured", lambda: False)
    try:
        asyncio.run(cleanup.purge_irrelevant_posts(grace_hours=168))
    except RuntimeError as exc:
        assert "backup" in str(exc)
    else:
        raise AssertionError("purge must refuse to delete without R2 backups")


def test_dry_run_counts_run_on_sqlite(monkeypatch):
    """Xem trước = 2 câu đếm. select(count).where(text(...)) không tự có FROM ("no such column" trên D1); alias +
    bảng gốc thì nhân chéo (5 comment x 2 khớp = 10 thay vì 2)."""
    import sqlite3

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript(
        "CREATE TABLE posts (id TEXT, relevance_label TEXT, relevance_labeled_at TEXT, scraped_at TEXT);"
        "CREATE TABLE comments (id TEXT, post_id TEXT);"
        "INSERT INTO posts VALUES ('p1','not_related','2020-01-01','2020-01-01'),('p2','related',NULL,'2020-01-01');"
        "INSERT INTO comments VALUES ('c1','p1'),('c2','p1'),('c3','p2'),('c4','p2'),('c5','p2');"
    )

    async def run(sql, params):
        return [dict(row) for row in db.execute(sql, params).fetchall()]

    monkeypatch.setattr(cleanup, "_run", run)
    result = asyncio.run(cleanup.purge_irrelevant_posts(dry_run=True, grace_hours=1))
    assert (result["posts"], result["comments"]) == (1, 2)
