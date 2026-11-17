"""movie_comment_hourly (Attack Detection, dấu hiệu A): chạy câu SQLAlchemy thật trên SQLite trong bộ nhớ."""

from datetime import UTC, datetime

from sqlalchemy import create_engine, insert, select

from app.repositories.d1 import comment_hourly as ch
from app.repositories.d1.tables import comments, posts

START = datetime(2026, 11, 17, 5, tzinfo=UTC)  # 12:00 giờ VN
END = datetime(2026, 11, 17, 8, tzinfo=UTC)  # 15:00 giờ VN


def _db():
    engine = create_engine("sqlite://")
    for table in (posts, comments, ch.movie_comment_hourly):
        table.create(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(posts),
            [
                {"id": "p1", "movie_id": "m1", "relevance_label": "related", "keyword_match": 1},
                {"id": "p2", "movie_id": "m1", "relevance_label": "not_related", "keyword_match": 1},
            ],
        )
        # Mọi dòng phải có cùng bộ cột: chèn nhiều dòng một lúc thì SQLAlchemy lấy cột theo dòng đầu.
        rows = [
            # 12h VN: 3 bình luận, 2 chê có chữ + 1 chê chỉ là icon ('rule' - không tính là chê)
            {"id": "c1", "post_id": "p1", "posted_at": "2026-11-17T05:10:00+00:00", "sentiment": "negative"},
            {"id": "c2", "post_id": "p1", "posted_at": "2026-11-17T05:40:00+00:00", "sentiment": "negative"},
            {
                "id": "c3",
                "post_id": "p1",
                "posted_at": "2026-11-17T05:50:00+00:00",
                "sentiment": "negative",
                "sentiment_source": "rule",
            },
            # 13h VN: 1 khen
            {"id": "c4", "post_id": "p1", "posted_at": "2026-11-17T06:05:00+00:00", "sentiment": "positive"},
            # bài không liên quan: không đếm
            {"id": "c5", "post_id": "p2", "posted_at": "2026-11-17T06:30:00+00:00", "sentiment": "negative"},
            # ngoài khoảng tính
            {"id": "c6", "post_id": "p1", "posted_at": "2026-11-17T09:00:00+00:00", "sentiment": "negative"},
        ]
        conn.execute(insert(comments), [{"sentiment_source": None, **row} for row in rows])
    return engine


def _rows(engine):
    h = ch.movie_comment_hourly
    with engine.connect() as conn:
        return {r.hour: (r.total, r.negative) for r in conn.execute(select(h).order_by(h.c.hour))}


def test_counts_by_vietnam_hour_and_skips_unrelated_posts():
    engine = _db()
    with engine.begin() as conn:
        conn.execute(ch.upsert_statement(START, END, "r1"))
    assert _rows(engine) == {"2026-11-17T12": (3, 2), "2026-11-17T13": (1, 0)}


def test_rebuild_replaces_counts_and_drops_emptied_hours():
    engine = _db()
    with engine.begin() as conn:
        conn.execute(ch.upsert_statement(START, END, "r1"))
        conn.execute(comments.delete().where(comments.c.id == "c4"))  # giờ 13h không còn bình luận
        conn.execute(ch.upsert_statement(START, END, "r2"))
        conn.execute(ch.cleanup_statement(START, END, "r2"))
    assert _rows(engine) == {"2026-11-17T12": (3, 2)}


def test_vn_hour():
    assert ch.vn_hour(START) == "2026-11-17T12"
