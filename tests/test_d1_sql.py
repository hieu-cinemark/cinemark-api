from sqlalchemy import select

from app.repositories.d1.press_reviews import press
from app.repositories.d1.sql import compile_sql
from app.repositories.d1.topics import snapshots


def test_compile_sql_uses_positional_params_in_order():
    sql, params = compile_sql(select(press.c.movie_id).where(press.c.movie_id == "m1", press.c.snapshot_id == "s1"))
    assert sql.count("?") == 2
    assert params == ["m1", "s1"]


def test_snapshot_batch_fits_d1_param_limit():
    from app.repositories.d1.topics import _SNAPSHOT_BATCH

    assert _SNAPSHOT_BATCH * len(snapshots.columns) <= 100


def test_compile_sql_handles_ddl():
    from sqlalchemy.schema import CreateTable

    sql, params = compile_sql(CreateTable(press, if_not_exists=True))
    assert sql.strip().startswith("CREATE TABLE IF NOT EXISTS movie_press_reviews")
    assert params == []


def test_relevant_post_and_engagement_score_match_raw_sql():
    """Các câu SQL thuần còn lại vẫn dùng RELEVANT_POST_SQL / ENGAGEMENT_SCORE_SQL: hai bản phải cho cùng kết quả."""
    import itertools
    import sqlite3

    from sqlalchemy.dialects import sqlite

    from app.repositories.d1.posts import ENGAGEMENT_SCORE_SQL, RELEVANT_POST_SQL, engagement_score, relevant_post
    from app.repositories.d1.tables import posts

    p = posts.alias("p")
    literal = {"literal_binds": True}
    relevant = relevant_post(p).compile(dialect=sqlite.dialect(), compile_kwargs=literal)
    score = engagement_score(p).compile(dialect=sqlite.dialect(), compile_kwargs=literal)
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE posts (id, relevance_label, keyword_match, like_count, reply_count, repost_count, "
        "quote_count, reshare_count)"
    )
    labels, matches = ("related", "not_related", "uncertain", None), (0, 1, None)
    for i, (label, match) in enumerate(itertools.product(labels, matches)):
        db.execute("INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (i, label, match, i, i % 3, 1, 0, 2))
    query = "SELECT id, {r}, {s} FROM posts p ORDER BY id"
    assert (
        db.execute(query.format(r=relevant, s=score)).fetchall()
        == db.execute(query.format(r=RELEVANT_POST_SQL, s=ENGAGEMENT_SCORE_SQL)).fetchall()
    )


def test_compile_sql_expands_in_lists():
    from app.repositories.d1.tables import comments

    sql, params = compile_sql(select(comments.c.id).where(comments.c.id.in_(["a", "b"]), comments.c.platform == "x"))
    assert "POSTCOMPILE" not in sql and sql.count("?") == 3
    assert params == ["a", "b", "x"]
