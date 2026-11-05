from app.ai.tasks.sentiment_rules import with_parent_context
from scripts.jobs.embed_comments import texts_to_embed


def row(cid, message, parent=None):
    return {"id": cid, "message": message, "parent_message": parent, "movie_id": "m"}


def test_reply_text_starts_with_its_parent():
    assert with_parent_context("Coi bình thường nha", "Chưa coi phần trước xem hiểu không") == (
        "Chưa coi phần trước xem hiểu không\nCoi bình thường nha"
    )
    assert with_parent_context("Phim hay", None) == "Phim hay"
    assert len(with_parent_context("ok", "x" * 500, parent_chars=200)) == 203


def test_embed_skips_rows_without_content():
    assert texts_to_embed([row("a", "❤️❤️"), row("b", "@Nguyễn Văn A")]) == []


def test_embed_redoes_only_replies_that_lacked_context():
    rows = [
        row("plain", "Phim hay quá trời"),
        row("old-reply", "Coi bình thường nha", "Xem hiểu không"),
        row("new-reply", "Đúng rồi đó bạn ơi", "Phim hay"),
        row("new", "Diễn viên đẹp quá trời"),
    ]
    done = {"plain": 0, "old-reply": 0, "new-reply": 1}
    assert [item[0]["id"] for item in texts_to_embed(rows, done)] == ["old-reply", "new"]
