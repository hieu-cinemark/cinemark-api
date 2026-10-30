import asyncio

from app.services.relevance import cleanup


def test_purge_backs_up_before_delete_and_keeps_posts_naming_cast(monkeypatch):
    calls: list[str] = []
    uploads: list[str] = []

    async def fake_run(sql, params):
        calls.append(sql)
        if sql.startswith("SELECT p.id"):
            return [
                {"id": "a", "content": "bảng cửu chương cộng trừ nhân chia", "title": "Cộng Trừ Nhân Chia", "director": "", "cast": ""},
                {"id": "b", "content": "Anh Thư đóng vai này hay quá", "title": "Cộng Trừ Nhân Chia", "director": "", "cast": '["Anh Thư"]'},
            ]
        if sql.startswith("SELECT * FROM posts"):
            return [{"id": "a", "platform": "tiktok", "first_seen_at": "2026-10-20T01:00:00", "keyword_id": "k1"}]
        if sql.startswith("SELECT * FROM comments"):
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
    last_backup = max(i for i, c in enumerate(calls) if c.startswith("SELECT * FROM"))
    assert last_backup < first_delete  # sao lưu xong mọi bảng của lô rồi mới xoá
    assert subtracted == [(1, 1)]
    assert totals["backup"].startswith("backups/purge/")


def test_purge_refuses_without_backup_storage(monkeypatch):
    async def fake_run(sql, params):
        if sql.startswith("SELECT p.id"):
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
